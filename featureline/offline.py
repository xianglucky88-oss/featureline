"""离线特征管线（Polars）。

与 Go 线上服务共享同一批特征名，但**窗口语义不一致**：

    Go   [T-7d, T)   左闭右开，T = time.Now()
    离线 [T-7d, T]   左闭右闭，T = 数据里的最大 event_time

两个差异叠加，导致同样的数据在两边算出不同结果。
两个仓库各自定义了自己的窗口口径。
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl


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
    inclusive_right: bool = True,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """离线算特征。返回 (用户级特征, 商品级特征)。

    参数刻意暴露了两个"口径旋钮"：
        as_of            窗口右端。None 表示用数据的最大 event_time。
        inclusive_right  True = [T-7d, T]（左闭右闭），False = [T-7d, T)（左闭右开）

    默认值：as_of=None（用数据最大时间），inclusive_right=True（左闭右闭）。
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


def export_snapshot(features: pl.DataFrame, item_agg: pl.DataFrame, path: Path) -> dict:
    """把离线特征导出成 Go 服务能读的 JSON 快照。

    注意：这里**不做任何窗口修正**，直接把离线结果写进去，
    所以快照里已经带着离线侧的窗口偏差。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    as_of = features["as_of"][0] if features.height else None
    ts = as_of.isoformat() if as_of is not None else datetime.now(timezone.utc).isoformat()

    data: dict[str, dict] = {}
    for row in features.iter_rows(named=True):
        uid = row["user_id"]
        for name in (
            "user_7d_order_cnt",
            "user_7d_gmv_cents",
            "user_30d_order_cnt",
            "user_category_affinity",
        ):
            v = row[name]
            if v is None:
                continue
            data[f"f:{uid}:{name}"] = {"score": float(v), "updated_at": ts, "source": "offline"}

    for row in item_agg.iter_rows(named=True):
        iid = row["item_id"]
        for name in ("item_7d_ctr", "item_7d_exposure"):
            data[f"f:{iid}:{name}"] = {
                "score": float(row[name]),
                "updated_at": ts,
                "source": "offline",
            }

    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"keys": len(data), "as_of": ts}
