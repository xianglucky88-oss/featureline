"""时间点正确、可增量扩展的离线特征回填。

为什么需要这个模块
------------------
旧路径（offline.compute_offline + 逐日重算）有两个结构性问题：

1. **时间穿越（label leakage）**：历史样本的特征用"全量数据的最大 event_time"
   作为窗口右端，样本能看到标签时点之后才发生的行为，训练指标被系统性高估。
2. **不可扩展**：逐日全量重算，N 天回填 = N 次全表扫描，长期补数无法承受。

设计
----
分层存储（lake 目录）::

    lake/
      events/dt=YYYY-MM-DD.parquet   原始事件，按事件日分区，event_id 去重（ingest 幂等）
      events/_manifest.json          watermark + 每个分区的内容 hash
      state/<tile>/dt=....parquet    预聚合 tiles：user_daily / user_cat_daily / item_daily
      state/_manifest.json           每个 tile 分区的 input_hash 与版本（内容寻址）
      backfill/dt=<标签日>/          输出，按标签日分区，原子写
        features.parquet
        _meta.json                   spec 版本、watermark、输入状态 hash、content hash、run_id
      backfill/_manifest.json        每个标签日的版本历史（可追溯、可复现）

不变式
------
- 窗口语义与线上 Go 服务一致：[t-W, t)，左闭右开，t = 样本自身的 label_time。
- features(entity, t) 是 {event : event_time < t} 的纯函数 ——
  t 之后到达的数据**不可能**改变历史样本的结果（稳定性）。
- 迟到事件（event_time 落在已处理的分区）只触发对应日分区重建；
  重跑回填会确定性地把它纳入，并产生新的输出版本（旧版本保留在 manifest）。
- 每个样本的计算成本 = O(窗口天数 + 至多 2 个边界日的事件数)，与历史总量无关。

精确性
------
窗口 = 内部完整日的 tiles 之和 + 至多 2 个边界日的原始事件精修。
tiles 按自然日预聚合；窗口左右端点所在的日回扫当日原始事件，
因此对任意 as_of 精确到事件时间本身，不是"按天取近似"。
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl

# 特征口径版本。窗口语义、聚合方式、缺失值处理任何变更都必须 bump，
# 否则旧分区会被误判为"未变化"。
SPEC_VERSION = "pit-v1"

WINDOW_DAYS = (7, 30)

FEATURE_COLS = [
    "user_7d_order_cnt",
    "user_7d_gmv_cents",
    "user_30d_order_cnt",
    "user_category_affinity",
    "item_7d_ctr",
    "item_7d_exposure",
]

# ---------------------------------------------------------------- 工具


def _read_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8"
    )


def _atomic_write_parquet(df: pl.DataFrame, path: Path) -> None:
    """先写临时文件再 rename：读者永远看到完整文件，重复写是幂等的。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    df.write_parquet(tmp)
    tmp.replace(path)


def _content_hash(df: pl.DataFrame) -> str:
    """内容寻址 hash：与行序、写出次数无关，只取决于数据本身。"""
    cols = sorted(df.columns)
    canonical = df.select(cols).sort(cols)
    return hashlib.sha256(canonical.write_csv().encode()).hexdigest()


def _parse_iso(raw: str | None) -> datetime | None:
    if not raw:
        return None
    return datetime.fromisoformat(raw).astimezone(timezone.utc)


def _day_of(ts_expr: pl.Expr) -> pl.Expr:
    return ts_expr.dt.truncate("1d")


# ---------------------------------------------------------------- ingest


def ingest_events(events: pl.DataFrame, lake: Path) -> dict:
    """把事件按事件日写入 events/dt=*.parquet。

    - 按 event_id 去重：同一批事件重复 ingest 是 no-op（幂等）。
    - 迟到事件：event_time 早于当前 watermark 的**新**事件会被计入 late_rows，
      它们落在哪个日分区，哪个分区的内容 hash 就会变化，
      后续 build_state 只重建这些分区。
    """
    events_dir = lake / "events"
    events_dir.mkdir(parents=True, exist_ok=True)
    manifest = _read_json(events_dir / "_manifest.json") or {"watermark": None, "days": {}}
    prev_wm = _parse_iso(manifest.get("watermark"))
    if events.height == 0:
        return {
            "new_rows": 0,
            "late_rows": 0,
            "days_written": [],
            "watermark": manifest.get("watermark"),
        }

    ev = events.with_columns(pl.col("event_time").dt.date().alias("_day"))
    new_rows = late_rows = 0
    days_written: list[str] = []

    for (day,), part in ev.partition_by("_day", as_dict=True).items():
        part = part.drop("_day").sort(["event_time", "event_id"])
        path = events_dir / f"dt={day}.parquet"
        old_ids: set[str] = set()
        if path.exists():
            old = pl.read_parquet(path)
            old_ids = set(old["event_id"].to_list())
            combined = pl.concat([old, part]).unique(subset=["event_id"], keep="first")
        else:
            combined = part
        genuinely_new = combined.height - len(old_ids)
        if genuinely_new == 0:
            continue  # 重复 ingest：无新内容
        combined = combined.sort(["event_time", "event_id"])
        _atomic_write_parquet(combined, path)
        if prev_wm is not None:
            is_new = (
                ~pl.col("event_id").is_in(list(old_ids)) if old_ids else pl.lit(True)
            )
            late_rows += part.filter(is_new & (pl.col("event_time") < prev_wm)).height
        new_rows += genuinely_new
        day_str = str(day)
        days_written.append(day_str)
        manifest["days"][day_str] = {
            "rows": combined.height,
            "hash": _content_hash(combined),
        }

    batch_max = events["event_time"].max()
    if batch_max.tzinfo is None:
        batch_max = batch_max.replace(tzinfo=timezone.utc)
    wm = batch_max if prev_wm is None else max(prev_wm, batch_max)
    manifest["watermark"] = wm.isoformat()
    _write_json(events_dir / "_manifest.json", manifest)

    return {
        "new_rows": new_rows,
        "late_rows": late_rows,
        "days_written": sorted(days_written),
        "watermark": wm.isoformat(),
    }


# ---------------------------------------------------------------- state tiles


def _tile_user_daily(ev: pl.DataFrame) -> pl.DataFrame:
    return (
        ev.filter(pl.col("event_type") == "order")
        .group_by("user_id")
        .agg(
            pl.len().alias("order_cnt"),
            pl.col("amount_cents").sum().alias("gmv_cents"),
        )
    )


def _tile_user_cat_daily(ev: pl.DataFrame) -> pl.DataFrame:
    return (
        ev.filter(pl.col("event_type") == "click")
        .group_by("user_id", "category_id")
        .agg(pl.len().alias("click_cnt"))
    )


def _tile_item_daily(ev: pl.DataFrame) -> pl.DataFrame:
    return ev.group_by("item_id").agg(
        (pl.col("event_type") == "exposure").sum().alias("exposure_cnt"),
        (pl.col("event_type") == "click").sum().alias("click_cnt"),
    )


TILE_BUILDERS = {
    "user_daily": _tile_user_daily,
    "user_cat_daily": _tile_user_cat_daily,
    "item_daily": _tile_item_daily,
}

_EVENTS_SCHEMA = {
    "event_id": pl.String,
    "user_id": pl.String,
    "item_id": pl.String,
    "category_id": pl.String,
    "event_type": pl.String,
    "amount_cents": pl.Int64,
    "event_time": pl.Datetime("us", "UTC"),
}

_TILE_SCHEMAS = {
    "user_daily": {"user_id": pl.String, "order_cnt": pl.UInt32, "gmv_cents": pl.Int64, "day": pl.Date},
    "user_cat_daily": {"user_id": pl.String, "category_id": pl.String, "click_cnt": pl.UInt32, "day": pl.Date},
    "item_daily": {"item_id": pl.String, "exposure_cnt": pl.UInt32, "click_cnt": pl.UInt32, "day": pl.Date},
}


def build_state(lake: Path) -> dict:
    """增量重建预聚合 tiles。

    每个 tile 分区是当日原始事件的纯函数。只有输入 hash 变化（新分区、
    迟到事件改写了旧分区）时才重建对应日分区，其余分区原样保留 ——
    重复运行是 no-op。
    """
    events_dir, state_dir = lake / "events", lake / "state"
    emanifest = _read_json(events_dir / "_manifest.json") or {}
    manifest = _read_json(state_dir / "_manifest.json") or {"tiles": {}}

    rebuilt: list[str] = []
    skipped = 0
    for path in sorted(events_dir.glob("dt=*.parquet")):
        day = path.stem[len("dt="):]
        ih = (emanifest.get("days", {}).get(day) or {}).get("hash")
        if ih is None:
            ih = _content_hash(pl.read_parquet(path))
        need = [
            name
            for name in TILE_BUILDERS
            if not (
                (rec := manifest["tiles"].get(name, {}).get(day))
                and rec["input_hash"] == ih
                and (state_dir / name / f"dt={day}.parquet").exists()
            )
        ]
        if not need:
            skipped += len(TILE_BUILDERS)
            continue
        ev = pl.read_parquet(path)
        day_lit = pl.lit(datetime.strptime(day, "%Y-%m-%d").date())
        for name in need:
            tile = TILE_BUILDERS[name](ev).with_columns(day_lit.alias("day"))
            tile = tile.sort(tile.columns)
            _atomic_write_parquet(tile, state_dir / name / f"dt={day}.parquet")
            manifest["tiles"].setdefault(name, {})[day] = {
                "input_hash": ih,
                "version": ih[:12],
                "rows": tile.height,
            }
            rebuilt.append(f"{name}/dt={day}")

    manifest["watermark"] = emanifest.get("watermark")
    _write_json(state_dir / "_manifest.json", manifest)
    return {
        "rebuilt": rebuilt,
        "skipped": skipped,
        "watermark": manifest["watermark"],
    }


# ---------------------------------------------------------------- 查询引擎


def _normalize_labels(labels: pl.DataFrame) -> pl.DataFrame:
    """保证有 label_id / label_date 列；label_time 统一为 UTC。"""
    out = labels
    if "label_id" not in out.columns:
        out = out.with_row_index("label_id")
    if out["label_time"].dtype.time_zone is None:
        out = out.with_columns(pl.col("label_time").dt.replace_time_zone("UTC"))
    if "label_date" not in out.columns:
        out = out.with_columns(
            pl.col("label_time").dt.date().cast(pl.String).alias("label_date")
        )
    return out


def _enrich(samples: pl.DataFrame) -> pl.DataFrame:
    """为每个样本预计算窗口边界。

    对每个窗口 W（列后缀）：
        _sW  窗口左端 = as_of - W
        _ffW 第一个完整日的起始 = ceil_day(_sW)
    以及：
        _de  as_of 所在日的起始 = floor_day(as_of)

    完整日 tiles 覆盖 [_ffW, _de)；左边界 [_sW, _ffW) 与右边界 [_de, as_of)
    由原始事件精修。W >= 1 天时 _ffW <= _de，两段边界区间不相交。
    """
    out = samples
    for w in WINDOW_DAYS:
        s = pl.col("as_of") - pl.duration(days=w)
        out = out.with_columns(
            s.alias(f"_s{w}"),
            pl.when(s == s.dt.truncate("1d"))
            .then(s.dt.truncate("1d"))
            .otherwise(s.dt.truncate("1d") + pl.duration(days=1))
            .alias(f"_ff{w}"),
        )
    return out.with_columns(_day_of(pl.col("as_of")).alias("_de"))


def _load_partitions(dir_: Path, days: set[str], schema: dict) -> pl.DataFrame:
    """只读取需要的日分区（分区裁剪 = 扫描成本控制）。"""
    paths = [p for d in sorted(days) if (p := dir_ / f"dt={d}.parquet").exists()]
    if not paths:
        return pl.DataFrame(schema=schema)
    return pl.concat([pl.read_parquet(p) for p in paths])


def _needed_days(labels: pl.DataFrame) -> tuple[set[str], set[str]]:
    """返回 (需要的 tile 日分区, 需要回扫原始事件的边界日)。"""
    lo = (labels["label_time"].min() - timedelta(days=max(WINDOW_DAYS))).date()
    hi = labels["label_time"].max().date()
    tiles = {str(lo + timedelta(days=i)) for i in range((hi - lo).days + 1)}
    boundary: set[str] = set()
    for ts in labels["label_time"].to_list():
        boundary.add(str(ts.date()))
        for w in WINDOW_DAYS:
            boundary.add(str((ts - timedelta(days=w)).date()))
    return tiles, boundary


def _sum_interior(smp: pl.DataFrame, tiles: pl.DataFrame, entity: str, metrics: dict[str, str], w: int) -> pl.DataFrame:
    """完整日 tiles 求和。metrics: {tile 列 -> 输出列}。"""
    j = smp.join(tiles, on=entity, how="inner")
    interior = j.filter(
        (pl.col("day") >= pl.col(f"_ff{w}").dt.date())
        & (pl.col("day") < pl.col("_de").dt.date())
    )
    return interior.group_by("label_id").agg(
        [pl.col(src).sum().alias(dst) for src, dst in metrics.items()]
    )


def _sum_boundary(smp: pl.DataFrame, raw: pl.DataFrame, entity: str, w: int, aggs: list[pl.Expr]) -> pl.DataFrame:
    """边界日原始事件精修：[_sW, _ffW) ∪ [_de, as_of)。"""
    j = raw.join(smp, on=entity, how="inner")
    mask = (
        (pl.col("event_time") >= pl.col(f"_s{w}")) & (pl.col("event_time") < pl.col(f"_ff{w}"))
    ) | (
        (pl.col("event_time") >= pl.col("_de")) & (pl.col("event_time") < pl.col("as_of"))
    )
    return j.filter(mask).group_by("label_id").agg(aggs)


def _combine(label_ids: pl.DataFrame, parts: list[pl.DataFrame], cols: list[str]) -> pl.DataFrame:
    """把同一指标的多个部分和（内部 tiles + 边界精修）按 label_id 相加。"""
    out = label_ids
    for i, p in enumerate(parts):
        out = out.join(p, on="label_id", how="left", suffix=f"__{i}")
    drop = []
    for c in cols:
        variants = [c] + [f"{c}__{i}" for i in range(1, len(parts))]
        drop += variants[1:]
        out = out.with_columns(
            pl.sum_horizontal([pl.col(v).fill_null(0) for v in variants]).alias(c)
        )
    return out.drop(drop)


def _compute(labels: pl.DataFrame, lake: Path) -> pl.DataFrame:
    """对一批样本做点时间正确的特征计算。

    labels 至少包含: user_id, item_id, label_time（可选 label_id）。
    返回 labels + FEATURE_COLS。
    """
    labels = _normalize_labels(labels)
    tile_days, boundary_days = _needed_days(labels)
    state_dir = lake / "state"

    user_tiles = _load_partitions(state_dir / "user_daily", tile_days, _TILE_SCHEMAS["user_daily"])
    cat_tiles = _load_partitions(state_dir / "user_cat_daily", tile_days, _TILE_SCHEMAS["user_cat_daily"])
    item_tiles = _load_partitions(state_dir / "item_daily", tile_days, _TILE_SCHEMAS["item_daily"])
    raw = _load_partitions(lake / "events", boundary_days, _EVENTS_SCHEMA)

    usmp = _enrich(
        labels.select("label_id", "user_id", pl.col("label_time").alias("as_of"))
    )
    ismp = _enrich(
        labels.select("label_id", "item_id", pl.col("label_time").alias("as_of"))
    )

    # ---- 用户粒度：7d / 30d 订单 ----
    orders_raw = raw.filter(pl.col("event_type") == "order")
    u7 = _combine(
        usmp.select("label_id"),
        [
            _sum_interior(usmp, user_tiles, "user_id", {"order_cnt": "user_7d_order_cnt", "gmv_cents": "user_7d_gmv_cents"}, 7),
            _sum_boundary(usmp, orders_raw, "user_id", 7, [
                pl.len().alias("user_7d_order_cnt"),
                pl.col("amount_cents").sum().alias("user_7d_gmv_cents"),
            ]),
        ],
        ["user_7d_order_cnt", "user_7d_gmv_cents"],
    )
    u30 = _combine(
        usmp.select("label_id"),
        [
            _sum_interior(usmp, user_tiles, "user_id", {"order_cnt": "user_30d_order_cnt"}, 30),
            _sum_boundary(usmp, orders_raw, "user_id", 30, [pl.len().alias("user_30d_order_cnt")]),
        ],
        ["user_30d_order_cnt"],
    )

    # ---- 用户×品类：30d 偏好品类（点击最多，并列取 category_id 小者）----
    clicks_raw = raw.filter(pl.col("event_type") == "click")
    cat_interior = (
        usmp.join(cat_tiles, on="user_id", how="inner")
        .filter(
            (pl.col("day") >= pl.col("_ff30").dt.date())
            & (pl.col("day") < pl.col("_de").dt.date())
        )
        .group_by("label_id", "category_id")
        .agg(pl.col("click_cnt").sum().cast(pl.Int64))
    )
    # 边界点击需要保留 category_id 维度，不能复用 _sum_boundary（它只按 label_id 聚合）。
    cat_boundary = (
        clicks_raw.join(usmp, on="user_id", how="inner")
        .filter(
            (
                (pl.col("event_time") >= pl.col("_s30"))
                & (pl.col("event_time") < pl.col("_ff30"))
            )
            | (
                (pl.col("event_time") >= pl.col("_de"))
                & (pl.col("event_time") < pl.col("as_of"))
            )
        )
        .group_by("label_id", "category_id")
        .agg(pl.len().cast(pl.Int64).alias("click_cnt"))
    )
    cat_total = pl.concat([cat_interior, cat_boundary]).group_by("label_id", "category_id").agg(
        pl.col("click_cnt").sum()
    )
    affinity = (
        cat_total.sort("label_id", "click_cnt", "category_id", descending=[False, True, False])
        .group_by("label_id", maintain_order=True)
        .agg(pl.col("category_id").first().alias("user_category_affinity"))
        .with_columns(
            pl.col("user_category_affinity").str.replace("C", "").cast(pl.Float64)
        )
    )

    user_feats = (
        u7.join(u30, on="label_id", how="left")
        .join(affinity, on="label_id", how="left")
    )

    # ---- 商品粒度：7d 曝光 / CTR ----
    i7 = _combine(
        ismp.select("label_id"),
        [
            _sum_interior(ismp, item_tiles, "item_id", {"exposure_cnt": "_exp", "click_cnt": "_clk"}, 7),
            _sum_boundary(ismp, raw, "item_id", 7, [
                (pl.col("event_type") == "exposure").sum().alias("_exp"),
                (pl.col("event_type") == "click").sum().alias("_clk"),
            ]),
        ],
        ["_exp", "_clk"],
    )
    item_feats = i7.with_columns(
        pl.when(pl.col("_exp") > 0)
        .then(pl.col("_clk") / pl.col("_exp"))
        .otherwise(None)
        .alias("item_7d_ctr"),
        pl.col("_exp").alias("item_7d_exposure"),
    ).select("label_id", "item_7d_ctr", "item_7d_exposure")

    out = labels.join(user_feats, on="label_id", how="left").join(
        item_feats, on="label_id", how="left"
    )
    return out.with_columns(
        [pl.col(c).fill_null(0).cast(pl.Float64) for c in FEATURE_COLS]
    )


# ---------------------------------------------------------------- 回填驱动


def backfill_labels(labels: pl.DataFrame, lake: Path, run_id: str | None = None) -> dict:
    """按标签日分区回填，幂等，版本可追溯。

    - 输出 lake/backfill/dt=<标签日>/features.parquet + _meta.json（原子写）。
    - 内容未变化的重跑是 no-op（summary 里状态为 "unchanged"）。
    - 输入变化（迟到数据、口径升级）会产生新版本，旧版本进入 history。
    """
    out_dir = lake / "backfill"
    labels = _normalize_labels(labels)
    if labels.height == 0:
        return {}
    feats = _compute(labels, lake)

    state_manifest_path = lake / "state" / "_manifest.json"
    state_manifest = _read_json(state_manifest_path) or {}
    state_hash = ""
    if state_manifest_path.exists():
        state_hash = hashlib.sha256(state_manifest_path.read_bytes()).hexdigest()[:16]

    manifest = _read_json(out_dir / "_manifest.json") or {
        "spec_version": SPEC_VERSION,
        "partitions": {},
    }
    run_id = run_id or datetime.now(timezone.utc).strftime("run-%Y%m%dT%H%M%SZ")
    summary: dict[str, dict] = {}

    for (date,), part in feats.partition_by("label_date", as_dict=True).items():
        part = part.drop("label_date").sort("label_id")
        ch = _content_hash(part)
        rec = manifest["partitions"].get(date)
        cur = rec["current"] if rec else None
        if cur and cur["content_hash"] == ch and cur["spec_version"] == SPEC_VERSION:
            summary[date] = {"status": "unchanged", "rows": part.height, "content_hash": ch}
            continue

        meta = {
            "label_date": date,
            "spec_version": SPEC_VERSION,
            "run_id": run_id,
            "computed_at": datetime.now(timezone.utc).isoformat(),
            "watermark": state_manifest.get("watermark"),
            "state_manifest_hash": state_hash,
            "rows": part.height,
            "content_hash": ch,
            "window_semantics": "[t-W, t) left-closed right-open, t = label_time",
        }
        pdir = out_dir / f"dt={date}"
        _atomic_write_parquet(part, pdir / "features.parquet")
        _write_json(pdir / "_meta.json", meta)
        if rec:
            rec.setdefault("history", []).append(rec["current"])
            rec["current"] = meta
        else:
            manifest["partitions"][date] = {"current": meta, "history": []}
        summary[date] = {
            "status": "written",
            "rows": part.height,
            "content_hash": ch,
            "version": len(manifest["partitions"][date]["history"]) + 1,
        }

    _write_json(out_dir / "_manifest.json", manifest)
    return summary


# ---------------------------------------------------------------- 校验用暴力参考


def brute_force_features(events: pl.DataFrame, labels: pl.DataFrame) -> pl.DataFrame:
    """每个样本都扫全量事件的参考实现（O(样本数 × 事件数)）。

    只用于校验增量引擎的正确性（测试与 CLI 自检），生产路径禁止调用。
    与 _compute 不共享任何聚合代码，保证校验的独立性。
    """
    labels = _normalize_labels(labels)
    usmp = labels.select("label_id", "user_id", pl.col("label_time").alias("as_of"))
    ismp = labels.select("label_id", "item_id", pl.col("label_time").alias("as_of"))

    j = events.join(usmp, on="user_id", how="inner")
    o7 = (
        j.filter(
            (pl.col("event_time") >= pl.col("as_of") - pl.duration(days=7))
            & (pl.col("event_time") < pl.col("as_of"))
            & (pl.col("event_type") == "order")
        )
        .group_by("label_id")
        .agg(
            pl.len().alias("user_7d_order_cnt"),
            pl.col("amount_cents").sum().alias("user_7d_gmv_cents"),
        )
    )
    o30 = (
        j.filter(
            (pl.col("event_time") >= pl.col("as_of") - pl.duration(days=30))
            & (pl.col("event_time") < pl.col("as_of"))
            & (pl.col("event_type") == "order")
        )
        .group_by("label_id")
        .agg(pl.len().alias("user_30d_order_cnt"))
    )
    aff = (
        j.filter(
            (pl.col("event_time") >= pl.col("as_of") - pl.duration(days=30))
            & (pl.col("event_time") < pl.col("as_of"))
            & (pl.col("event_type") == "click")
        )
        .group_by("label_id", "category_id")
        .agg(pl.len().alias("n"))
        .sort("label_id", "n", "category_id", descending=[False, True, False])
        .group_by("label_id", maintain_order=True)
        .agg(pl.col("category_id").first().alias("user_category_affinity"))
        .with_columns(
            pl.col("user_category_affinity").str.replace("C", "").cast(pl.Float64)
        )
    )

    ij = events.join(ismp, on="item_id", how="inner").filter(
        (pl.col("event_time") >= pl.col("as_of") - pl.duration(days=7))
        & (pl.col("event_time") < pl.col("as_of"))
    )
    it = (
        ij.group_by("label_id")
        .agg(
            (pl.col("event_type") == "exposure").sum().alias("_exp"),
            (pl.col("event_type") == "click").sum().alias("_clk"),
        )
        .with_columns(
            pl.when(pl.col("_exp") > 0)
            .then(pl.col("_clk") / pl.col("_exp"))
            .otherwise(None)
            .alias("item_7d_ctr"),
            pl.col("_exp").alias("item_7d_exposure"),
        )
        .select("label_id", "item_7d_ctr", "item_7d_exposure")
    )

    out = (
        labels.join(o7, on="label_id", how="left")
        .join(o30, on="label_id", how="left")
        .join(aff, on="label_id", how="left")
        .join(it, on="label_id", how="left")
    )
    return out.with_columns(
        [pl.col(c).fill_null(0).cast(pl.Float64) for c in FEATURE_COLS]
    )
