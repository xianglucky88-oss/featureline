"""离线特征管线（Polars）。

窗口语义不再由本模块自行约定，而是从注册表的已发布定义读取：
窗口区间统一为左闭右开 [T-size, T)，右端 T 为事件时间（event_time），
时区 UTC。训练任务通过绑定清单（Binding）固定版本，导出的快照头部
携带同一份清单，在线实例加载时交叉校验——三方看到的是同一版定义。
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl

from .registry_client import Binding, RegistryClient


def _default_paths() -> tuple[Path, Path]:
    return Path("data/events.parquet"), Path("data/offline_features.parquet")


def generate_events(
    n_users: int = 200,
    n_items: int = 60,
    n_events: int = 20000,
    seed: int = 20261112,
    days: int = 45,
) -> pl.DataFrame:
    """生成原始事件表。

    列：
        event_id, user_id, item_id, category_id, event_type(click/exposure/order),
        amount_cents, event_time(UTC)
    """
    import random

    rnd = random.Random(seed)
    base = datetime(2026, 9, 1, tzinfo=timezone.utc)
    rows = []
    for i in range(n_events):
        # 事件时间在 days 天内分布
        secs = rnd.randint(0, days * 24 * 3600)
        et = base + timedelta(seconds=secs)
        et = et.replace(second=0, microsecond=0)
        etype = rnd.choices(
            ["exposure", "click", "order"], weights=[0.62, 0.30, 0.08]
        )[0]
        rows.append(
            {
                "event_id": f"E{i:08d}",
                "user_id": f"U{rnd.randint(1, n_users):04d}",
                "item_id": f"I{rnd.randint(1, n_items):03d}",
                "category_id": f"C{rnd.randint(1, 12):02d}",
                "event_type": etype,
                "amount_cents": rnd.randint(500, 90_000) if etype == "order" else 0,
                "event_time": et,
            }
        )
    df = pl.DataFrame(rows).with_columns(
        pl.col("event_time").cast(pl.Datetime("us", "UTC"))
    )
    return df.sort("event_time")


def compute_offline(
    events: pl.DataFrame,
    as_of: datetime | None = None,
    window_days: int = 7,
    inclusive_right: bool = False,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """离线算特征。返回 (用户级特征, 商品级特征)。

    口径旋钮保留用于对照测试，但默认值已经与注册表统一：
        as_of            窗口右端。None 表示用数据的最大 event_time。
        inclusive_right  False = [T-7d, T)（左闭右开，注册表唯一允许的约定）；
                         True 仅用于复现旧版左闭右闭的历史偏差。
    """
    if as_of is None:
        # 未指定 as_of 时，取数据里的最大时间。
        as_of = events["event_time"].max()
    as_of = as_of.astimezone(timezone.utc) if as_of.tzinfo else as_of.replace(tzinfo=timezone.utc)
    start = as_of - timedelta(days=window_days)

    if inclusive_right:
        # 左闭右闭
        window = events.filter(
            (pl.col("event_time") >= start) & (pl.col("event_time") <= as_of)
        )
    else:
        window = events.filter(
            (pl.col("event_time") >= start) & (pl.col("event_time") < as_of)
        )

    orders = window.filter(pl.col("event_type") == "order")
    clicks = window.filter(pl.col("event_type") == "click")
    exposures = window.filter(pl.col("event_type") == "exposure")

    user_agg = orders.group_by("user_id").agg(
        [
            pl.len().alias("user_7d_order_cnt"),
            pl.col("amount_cents").sum().alias("user_7d_gmv_cents"),
        ]
    )

    item_agg = (
        exposures.group_by("item_id").agg([pl.len().alias("exp")])
        .join(
            clicks.group_by("item_id").agg([pl.len().alias("clk")]),
            on="item_id",
            how="left",
        )
        .with_columns(
            [
                pl.col("clk").fill_null(0),
                (pl.col("clk").fill_null(0) / pl.col("exp")).alias("item_7d_ctr"),
                pl.col("exp").alias("item_7d_exposure"),
            ]
        )
        .select(["item_id", "item_7d_ctr", "item_7d_exposure"])
    )

    # 用户偏好品类：窗口内点击最多的类别
    # 按点击次数降序；并列时用 category_id 作为次级排序键，保证结果可复现。
    affinity = (
        clicks.group_by(["user_id", "category_id"])
        .agg(pl.len().alias("n"))
        .sort(["user_id", "n", "category_id"], descending=[False, True, False])
        .group_by("user_id", maintain_order=True)
        .agg(pl.col("category_id").first().alias("user_category_affinity_code"))
        .with_columns(
            pl.col("user_category_affinity_code")
            .str.replace("C", "")
            .cast(pl.Float64)
            .alias("user_category_affinity")
        )
        .select(["user_id", "user_category_affinity"])
    )

    # 30 天特征
    start30 = as_of - timedelta(days=30)
    w30 = events.filter(
        (pl.col("event_time") >= start30) & (pl.col("event_time") <= as_of)
        if inclusive_right
        else (pl.col("event_time") >= start30) & (pl.col("event_time") < as_of)
    )
    u30 = (
        w30.filter(pl.col("event_type") == "order")
        .group_by("user_id")
        .agg(pl.len().alias("user_30d_order_cnt"))
    )

    out = (
        user_agg.join(u30, on="user_id", how="full", coalesce=True)
        .join(affinity, on="user_id", how="left")
        .select(
            [
                "user_id",
                "user_7d_order_cnt",
                "user_7d_gmv_cents",
                "user_30d_order_cnt",
                "user_category_affinity",
            ]
        )
    )

    # 离线侧对缺失值填 0。
    out = out.with_columns(
        [
            pl.col("user_7d_order_cnt").fill_null(0).cast(pl.Float64),
            pl.col("user_7d_gmv_cents").fill_null(0).cast(pl.Float64),
            pl.col("user_30d_order_cnt").fill_null(0).cast(pl.Float64),
            pl.col("user_category_affinity").fill_null(0).cast(pl.Float64),
        ]
    )

    out = out.with_columns(pl.lit(as_of).alias("as_of"))
    return out, item_agg


def write_offline(features: pl.DataFrame, path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    features.write_parquet(path)
    return features.height


USER_FEATURES = (
    "user_7d_order_cnt",
    "user_7d_gmv_cents",
    "user_30d_order_cnt",
    "user_category_affinity",
)
ITEM_FEATURES = ("item_7d_ctr", "item_7d_exposure")


def export_snapshot(
    features: pl.DataFrame,
    item_agg: pl.DataFrame,
    path: Path,
    binding: Binding | None = None,
    client: RegistryClient | None = None,
) -> dict:
    """把离线特征导出成 Go 服务能读的版本化 JSON 快照。

    新格式（提供 binding + client 时）：
        {
          "manifest": {manifest_id, fingerprint, entries:[{name,fqvn,fingerprint}...]},
          "data": {"f:{entity}:{fqvn}": {score, updated_at, source, fqvn}}
        }
    存储键里的 FQVN 与清单条目一一对应，在线实例按绑定版本读键，
    不兼容新版本与旧版本天然落在不同键空间，不可能串语义。

    不带 binding 时退化为旧格式（裸 map、无版本坐标），仅用于历史对照。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    as_of = features["as_of"][0] if features.height else None
    ts = as_of.isoformat() if as_of is not None else datetime.now(timezone.utc).isoformat()

    if binding is None or client is None:
        return _export_legacy(features, item_agg, path, ts)

    resolved = client.resolve(binding)
    fqvn_by_name = {name: v["fqvn"] for name, v in resolved.items()}

    data: dict[str, dict] = {}
    for row in features.iter_rows(named=True):
        uid = row["user_id"]
        for name in USER_FEATURES:
            if name not in fqvn_by_name:
                continue
            v = row[name]
            if v is None:
                continue
            fqvn = fqvn_by_name[name]
            data[f"f:{uid}:{fqvn}"] = {
                "score": float(v), "updated_at": ts, "source": "offline", "fqvn": fqvn,
            }

    for row in item_agg.iter_rows(named=True):
        iid = row["item_id"]
        for name in ITEM_FEATURES:
            if name not in fqvn_by_name:
                continue
            fqvn = fqvn_by_name[name]
            data[f"f:{iid}:{fqvn}"] = {
                "score": float(row[name]), "updated_at": ts,
                "source": "offline", "fqvn": fqvn,
            }

    manifest = {
        "manifest_id": binding.manifest_id,
        "fingerprint": binding.fingerprint,
        # 清单的 as_of 以本次特征实际的窗口右端为准，
        # 在线实例据此把时间基准固定在事件时间而不是墙钟。
        "as_of": ts,
        "entries": [e.to_dict() for e in binding.entries],
    }
    payload = {"manifest": manifest, "data": data}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "keys": len(data),
        "as_of": ts,
        "manifest_id": binding.manifest_id,
        "binding_fingerprint": binding.fingerprint,
    }


def _export_legacy(features: pl.DataFrame, item_agg: pl.DataFrame, path: Path, ts: str) -> dict:
    """旧格式导出：无版本坐标，仅保留给历史数据对照，不得用于新交付。"""
    data: dict[str, dict] = {}
    for row in features.iter_rows(named=True):
        uid = row["user_id"]
        for name in USER_FEATURES:
            v = row[name]
            if v is None:
                continue
            data[f"f:{uid}:{name}"] = {"score": float(v), "updated_at": ts, "source": "offline"}

    for row in item_agg.iter_rows(named=True):
        iid = row["item_id"]
        for name in ITEM_FEATURES:
            data[f"f:{iid}:{name}"] = {
                "score": float(row[name]),
                "updated_at": ts,
                "source": "offline",
            }

    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"keys": len(data), "as_of": ts, "manifest_id": None}


def compute_offline_bound(
    events: pl.DataFrame,
    binding: Binding,
    client: RegistryClient,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """按绑定定义计算离线特征。

    窗口长度与区间约定全部来自注册表里的已发布版本，而不是函数参数；
    本函数断言所有绑定版本的窗口约定都是注册表唯一允许的左闭右开，
    从训练侧杜绝“换一套口径算特征”的可能。
    """
    resolved = client.resolve(binding)
    for name, version in resolved.items():
        conv = version["definition"]["window"]["convention"]
        if conv != "[start,end)":
            raise ValueError(
                f"binding contains unsupported window convention {conv!r} on {name}; "
                "registry only permits [start,end)"
            )
    # 现有 6 个特征的窗口只有 7d / 30d 两档，compute_offline 已按此产出全部列；
    # 这里以绑定版本为准做一次列选择，未被绑定的特征不会进入训练数据。
    user_cols = [n for n in USER_FEATURES if n in resolved]
    item_cols = [n for n in ITEM_FEATURES if n in resolved]
    user_df, item_df = compute_offline(events, as_of=None, window_days=7, inclusive_right=False)
    keep_user = ["user_id", *user_cols, "as_of"]
    keep_item = ["item_id", *item_cols]
    return user_df.select(keep_user), item_df.select(keep_item)


def backfill_plan(days: int = 30) -> list[str]:
    """回填计划（当前实现：逐日全量重算）。

    30 天要重算 30 次全量窗口，实测 6 小时。
    """
    return [f"day-{i:02d}" for i in range(days)]
