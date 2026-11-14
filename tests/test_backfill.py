"""增量回填引擎的测试。

每个测试对应设计文档里的一条不变式：

- 时间点正确性：与"每样本扫全量"的暴力参考实现逐值一致
- 稳定性：标签时点之后到达的数据不能改变历史结果
- 迟到事件：只重建受影响的日分区，重跑后确定性纳入并产生新版本
- 幂等：ingest / build_state / backfill 重复运行都是 no-op
- 窗口边界：[t-W, t) 左闭右开，精确到事件时间（不是按天近似）
- 实体粒度：user / item / user×category
- 版本追溯：manifest 记录 watermark、输入状态 hash、content hash 与历史
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from featureline.backfill import (
    FEATURE_COLS,
    SPEC_VERSION,
    _compute,
    backfill_labels,
    brute_force_features,
    build_state,
    ingest_events,
)
from featureline.offline import generate_events

BASE = datetime(2026, 9, 1, tzinfo=timezone.utc)


def ev(event_id, user, item, cat, etype, amount, ts):
    return {
        "event_id": event_id,
        "user_id": user,
        "item_id": item,
        "category_id": cat,
        "event_type": etype,
        "amount_cents": amount,
        "event_time": ts,
    }


def make_events(rows) -> pl.DataFrame:
    return pl.DataFrame(rows).with_columns(
        pl.col("event_time").cast(pl.Datetime("us", "UTC"))
    ).sort("event_time")


def make_labels(pairs) -> pl.DataFrame:
    """pairs: [(label_id, user_id, item_id, label_time)]"""
    rows = [
        {"label_id": lid, "user_id": u, "item_id": i, "label_time": t}
        for lid, u, i, t in pairs
    ]
    return pl.DataFrame(rows).with_columns(
        pl.col("label_time").cast(pl.Datetime("us", "UTC"))
    )


def feature_row(df: pl.DataFrame, label_id: str) -> dict:
    return df.filter(pl.col("label_id") == label_id).row(0, named=True)


# ---------------------------------------------------------------- 正确性


@pytest.fixture()
def gen_lake(tmp_path):
    lake = tmp_path / "lake"
    events = generate_events(n_users=20, n_items=8, n_events=2000, seed=11)
    ingest_events(events, lake)
    build_state(lake)
    return lake, events


def test_matches_brute_force(gen_lake):
    """增量引擎（tiles + 边界精修）必须与逐样本全量扫描完全一致。"""
    lake, events = gen_lake
    max_ts = events["event_time"].max()
    users = sorted(events["user_id"].unique().to_list())
    items = sorted(events["item_id"].unique().to_list())
    pairs = []
    for d in range(10):
        day = (max_ts - timedelta(days=d)).date()
        lt = datetime(day.year, day.month, day.day, 23, 47, tzinfo=timezone.utc)
        for u in users:
            pairs.append((f"{day}:{u}", u, items[hash((d, u)) % len(items)], lt))
    labels = make_labels(pairs)

    fast = _compute(labels, lake).sort("label_id")
    ref = brute_force_features(events, labels).sort("label_id")
    assert_frame_equal(
        fast.select(["label_id", *FEATURE_COLS]),
        ref.select(["label_id", *FEATURE_COLS]),
    )


def test_missing_entity_defaults(tmp_path):
    """没有任何事件的实体：所有特征为默认值 0，而不是报错或 NaN。"""
    lake = tmp_path / "lake"
    events = make_events([ev("E1", "U1", "I1", "C01", "order", 100, BASE)])
    ingest_events(events, lake)
    build_state(lake)
    labels = make_labels([("L1", "U-ghost", "I-ghost", BASE + timedelta(days=2))])
    row = feature_row(_compute(labels, lake), "L1")
    for c in FEATURE_COLS:
        assert row[c] == 0.0, c


# ---------------------------------------------------------------- 稳定性


def test_future_data_cannot_change_history(gen_lake):
    """标签时点之后到达的数据，不可能改变已回填的历史分区。"""
    lake, events = gen_lake
    max_ts = events["event_time"].max()
    day = (max_ts - timedelta(days=5)).date()
    lt = datetime(day.year, day.month, day.day, 23, 47, tzinfo=timezone.utc)
    labels = make_labels([("L1", "U0001", "I001", lt)])

    before = backfill_labels(labels, lake)
    hash_before = before[str(day)]["content_hash"]

    future = make_events([
        ev("F1", "U0001", "I001", "C01", "order", 999999, max_ts + timedelta(hours=2)),
        ev("F2", "U0001", "I001", "C01", "order", 888888, max_ts + timedelta(days=3)),
    ])
    ing = ingest_events(future, lake)
    assert ing["late_rows"] == 0  # 是"未来"数据，不是迟到数据
    build_state(lake)

    after = backfill_labels(labels, lake)
    assert after[str(day)]["status"] == "unchanged"
    assert after[str(day)]["content_hash"] == hash_before


# ---------------------------------------------------------------- 迟到事件


def test_late_event_rebuilds_only_its_partition(tmp_path):
    lake = tmp_path / "lake"
    events = make_events([
        ev("E1", "U1", "I1", "C01", "order", 100, datetime(2026, 9, 5, 10, tzinfo=timezone.utc)),
        ev("E2", "U1", "I1", "C01", "order", 200, datetime(2026, 9, 12, 10, tzinfo=timezone.utc)),
        ev("E3", "U2", "I1", "C01", "order", 300, datetime(2026, 9, 14, 10, tzinfo=timezone.utc)),
    ])
    ingest_events(events, lake)
    build_state(lake)

    lt = datetime(2026, 9, 15, 23, 47, tzinfo=timezone.utc)
    far = datetime(2026, 10, 15, 23, 47, tzinfo=timezone.utc)
    labels = make_labels([("L1", "U1", "I1", lt), ("L2", "U1", "I1", far)])
    backfill_labels(labels, lake)

    def read(lid):
        df = pl.read_parquet(lake / "backfill" / "dt=*" / "features.parquet")
        return feature_row(df, lid)

    before = read("L1")
    assert before["user_7d_order_cnt"] == 1.0  # 只有 E2 在 7d 窗口内
    assert before["user_30d_order_cnt"] == 2.0

    # 迟到事件：event_time 落在已处理的分区（09-12），晚于 watermark 到达
    late = make_events([
        ev("LATE1", "U1", "I1", "C01", "order", 777, datetime(2026, 9, 12, 18, tzinfo=timezone.utc))
    ])
    ing = ingest_events(late, lake)
    assert ing["late_rows"] == 1

    st = build_state(lake)
    # 只重建 09-12 这一天的 tiles（3 种粒度各一个分区），其余分区不动
    assert sorted(st["rebuilt"]) == [
        "item_daily/dt=2026-09-12",
        "user_cat_daily/dt=2026-09-12",
        "user_daily/dt=2026-09-12",
    ]

    summary = backfill_labels(labels, lake)
    # 窗口覆盖 09-12 的分区得到新版本；30 天窗口都不覆盖的分区保持原样
    assert summary["2026-09-15"]["status"] == "written"
    assert summary["2026-09-15"]["version"] == 2
    assert summary["2026-10-15"]["status"] == "unchanged"

    after = read("L1")
    assert after["user_7d_order_cnt"] == 2.0
    assert after["user_7d_gmv_cents"] == 977.0
    assert after["user_30d_order_cnt"] == 3.0

    # 版本可追溯：旧版本保留在 history，content hash 不同
    manifest = json.loads((lake / "backfill" / "_manifest.json").read_text())
    rec = manifest["partitions"]["2026-09-15"]
    assert len(rec["history"]) == 1
    assert rec["history"][0]["content_hash"] != rec["current"]["content_hash"]
    assert rec["current"]["watermark"] is not None
    assert rec["current"]["spec_version"] == SPEC_VERSION


# ---------------------------------------------------------------- 幂等


def test_idempotent_rerun(tmp_path):
    lake = tmp_path / "lake"
    events = generate_events(n_users=10, n_items=5, n_events=500, seed=3)
    ing1 = ingest_events(events, lake)
    ing2 = ingest_events(events, lake)
    assert ing1["new_rows"] == 500
    assert ing2["new_rows"] == 0  # 重复 ingest 是 no-op

    st1 = build_state(lake)
    st2 = build_state(lake)
    assert len(st1["rebuilt"]) > 0
    assert st2["rebuilt"] == []  # 输入未变，零重建

    max_ts = events["event_time"].max()
    labels = make_labels([("L1", "U0001", "I001", max_ts)])
    s1 = backfill_labels(labels, lake)
    s2 = backfill_labels(labels, lake)
    date = str(max_ts.date())
    assert s1[date]["status"] == "written"
    assert s2[date]["status"] == "unchanged"

    manifest = json.loads((lake / "backfill" / "_manifest.json").read_text())
    assert manifest["partitions"][date]["history"] == []  # 没有产生多余版本


# ---------------------------------------------------------------- 窗口边界


def test_window_boundary_exact(tmp_path):
    """[t-W, t)：左端恰好闭、右端恰好开，精确到微秒而不是按天近似。"""
    lake = tmp_path / "lake"
    as_of = datetime(2026, 10, 15, 23, 47, tzinfo=timezone.utc)
    us = timedelta(microseconds=1)
    events = make_events([
        ev("E1", "U1", "I1", "C01", "order", 10, as_of),                      # 右端：排除
        ev("E2", "U1", "I1", "C01", "order", 10, as_of - us),                # 右端内：计入
        ev("E3", "U1", "I1", "C01", "order", 10, as_of - timedelta(days=7)),  # 左端：计入
        ev("E4", "U1", "I1", "C01", "order", 10, as_of - timedelta(days=7) - us),  # 左端外：排除
        ev("E5", "U1", "I1", "C01", "order", 10, as_of - timedelta(days=7) + us),  # 左端内：计入
        ev("E6", "U1", "I1", "C01", "order", 10, as_of - timedelta(days=30)),  # 30d 左端：计入
        ev("E7", "U1", "I1", "C01", "order", 10, as_of - timedelta(days=30) - us),  # 30d 左端外：排除
    ])
    ingest_events(events, lake)
    build_state(lake)
    labels = make_labels([("L1", "U1", "I1", as_of)])
    row = feature_row(_compute(labels, lake), "L1")
    assert row["user_7d_order_cnt"] == 3.0   # E2, E3, E5
    assert row["user_30d_order_cnt"] == 5.0  # E2,E3,E5 + E4（7d 外但 30d 内）+ E6


def test_midnight_as_of_uses_pure_tiles(tmp_path):
    """as_of 恰好是整日边界时，窗口完全由 tiles 覆盖（无边界日），结果仍精确。"""
    lake = tmp_path / "lake"
    as_of = datetime(2026, 10, 16, 0, 0, tzinfo=timezone.utc)
    events = make_events([
        ev("E1", "U1", "I1", "C01", "order", 10, datetime(2026, 10, 15, 12, tzinfo=timezone.utc)),
        ev("E2", "U1", "I1", "C01", "order", 10, datetime(2026, 10, 9, 0, 0, tzinfo=timezone.utc)),
        ev("E3", "U1", "I1", "C01", "order", 10, datetime(2026, 10, 8, 23, 59, tzinfo=timezone.utc)),
    ])
    ingest_events(events, lake)
    build_state(lake)
    labels = make_labels([("L1", "U1", "I1", as_of)])
    row = feature_row(_compute(labels, lake), "L1")
    assert row["user_7d_order_cnt"] == 2.0  # E1 + E2（左端 10-09 00:00 恰好闭）


# ---------------------------------------------------------------- 实体粒度


def test_item_granularity(tmp_path):
    lake = tmp_path / "lake"
    as_of = datetime(2026, 10, 15, 12, 0, tzinfo=timezone.utc)
    rows = []
    for i in range(10):  # 窗口内 10 次曝光 4 次点击
        rows.append(ev(f"X{i}", "U1", "I1", "C01", "exposure", 0, as_of - timedelta(days=1, minutes=i)))
        if i < 4:
            rows.append(ev(f"C{i}", "U1", "I1", "C01", "click", 0, as_of - timedelta(days=1, minutes=i)))
    rows.append(ev("Y1", "U1", "I1", "C01", "exposure", 0, as_of - timedelta(days=30)))  # 窗口外
    events = make_events(rows)
    ingest_events(events, lake)
    build_state(lake)
    labels = make_labels([("L1", "U1", "I1", as_of), ("L2", "U1", "I2", as_of)])
    out = _compute(labels, lake)
    r1 = feature_row(out, "L1")
    assert r1["item_7d_exposure"] == 10.0
    assert r1["item_7d_ctr"] == pytest.approx(0.4)
    r2 = feature_row(out, "L2")  # 窗口内无曝光：ctr 落默认值 0，不是 NaN
    assert r2["item_7d_exposure"] == 0.0
    assert r2["item_7d_ctr"] == 0.0


def test_category_affinity_tiebreak_and_boundary(tmp_path):
    """并列时取 category_id 小者；边界日（as_of 当天）的点击必须计入。"""
    lake = tmp_path / "lake"
    as_of = datetime(2026, 10, 15, 12, 0, tzinfo=timezone.utc)
    events = make_events([
        ev("E1", "U1", "I1", "C02", "click", 0, as_of - timedelta(days=3)),
        ev("E2", "U1", "I1", "C02", "click", 0, as_of - timedelta(days=2)),
        ev("E3", "U1", "I1", "C01", "click", 0, as_of - timedelta(days=2)),
        ev("E4", "U1", "I1", "C01", "click", 0, as_of - timedelta(hours=1)),  # 边界日
    ])
    ingest_events(events, lake)
    build_state(lake)
    labels = make_labels([("L1", "U1", "I1", as_of)])
    row = feature_row(_compute(labels, lake), "L1")
    assert row["user_category_affinity"] == 1.0  # C01 与 C02 各 2 次，取小者


# ---------------------------------------------------------------- 版本追溯


def test_partition_metadata_traceable(tmp_path):
    lake = tmp_path / "lake"
    events = generate_events(n_users=5, n_items=3, n_events=200, seed=5)
    ingest_events(events, lake)
    build_state(lake)
    max_ts = events["event_time"].max()
    labels = make_labels([("L1", "U0001", "I001", max_ts)])
    backfill_labels(labels, lake, run_id="run-test")

    date = str(max_ts.date())
    meta = json.loads((lake / "backfill" / f"dt={date}" / "_meta.json").read_text())
    assert meta["run_id"] == "run-test"
    assert meta["spec_version"] == SPEC_VERSION
    assert meta["watermark"] is not None
    assert len(meta["state_manifest_hash"]) == 16
    assert len(meta["content_hash"]) == 64
    assert meta["rows"] == 1

    manifest = json.loads((lake / "backfill" / "_manifest.json").read_text())
    assert manifest["spec_version"] == SPEC_VERSION
    assert date in manifest["partitions"]
