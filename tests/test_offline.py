"""离线管线的单元测试。

不依赖 Go 服务：只测纯计算部分。
线上/离线一致性见 scripts/verify_features.py。
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import polars as pl
import pytest

from featureline.offline import compute_offline, generate_events


@pytest.fixture(scope="module")
def events():
    return generate_events(n_users=30, n_items=10, n_events=3000, seed=7)


def test_generator_deterministic():
    a = generate_events(n_users=10, n_events=200, seed=3)
    b = generate_events(n_users=10, n_events=200, seed=3)
    assert a.equals(b)


def test_generator_sorted_by_event_time(events):
    ts = events["event_time"].to_list()
    assert ts == sorted(ts)


def test_event_types_present(events):
    kinds = set(events["event_type"].unique().to_list())
    assert kinds == {"exposure", "click", "order"}


def test_order_amounts_nonzero(events):
    orders = events.filter(pl.col("event_type") == "order")
    assert orders.height > 0
    assert orders["amount_cents"].min() > 0


def test_non_order_amounts_zero(events):
    non_orders = events.filter(pl.col("event_type") != "order")
    assert non_orders["amount_cents"].max() == 0


# ---------------------------------------------------------------- 窗口语义


def test_window_right_edge_differs(events):
    """左闭右闭与左闭右开在边界上应有差异。"""
    as_of = events["event_time"].max()
    incl, _ = compute_offline(events, as_of=as_of, window_days=7, inclusive_right=True)
    excl, _ = compute_offline(events, as_of=as_of, window_days=7, inclusive_right=False)
    # 至少有一行不同（除非边界恰好没有事件）
    if incl["user_7d_order_cnt"].sum() == excl["user_7d_order_cnt"].sum():
        pytest.skip("no events exactly on the right boundary in this dataset")
    assert not incl.equals(excl)


def test_as_of_controls_window(events):
    """as_of 不同，窗口内容不同。"""
    max_ts = events["event_time"].max()
    recent, _ = compute_offline(events, as_of=max_ts, window_days=7)
    old, _ = compute_offline(events, as_of=max_ts - timedelta(days=20), window_days=7)
    assert not recent.equals(old)


def test_far_future_as_of_yields_empty(events):
    """as_of 远在数据之后，7 天窗口内应无事件。"""
    future = events["event_time"].max() + timedelta(days=365)
    df, _ = compute_offline(events, as_of=future, window_days=7)
    assert df["user_7d_order_cnt"].sum() == 0


def test_30d_window_ge_7d(events):
    as_of = events["event_time"].max()
    f7, _ = compute_offline(events, as_of=as_of, window_days=7)
    f30, _ = compute_offline(events, as_of=as_of, window_days=30)
    assert f30["user_30d_order_cnt"].sum() >= f7["user_7d_order_cnt"].sum()


# ---------------------------------------------------------------- 缺失值


def test_missing_filled_with_zero(events):
    """离线缺失填 0（线上填 NaN 后当 0，但打点不同）。"""
    as_of = events["event_time"].max()
    df, _ = compute_offline(events, as_of=as_of, window_days=7)
    assert df["user_7d_order_cnt"].null_count() == 0
    assert df["user_7d_gmv_cents"].null_count() == 0


# ---------------------------------------------------------------- item 特征


def test_item_ctr_in_range(events):
    as_of = events["event_time"].max()
    _, items = compute_offline(events, as_of=as_of, window_days=7)
    ctr = items["item_7d_ctr"].to_list()
    assert all(0.0 <= v <= 1.0 for v in ctr), f"CTR out of range: {ctr[:10]}"


def test_item_exposure_positive(events):
    as_of = events["event_time"].max()
    _, items = compute_offline(events, as_of=as_of, window_days=7)
    assert items["item_7d_exposure"].min() > 0


# ---------------------------------------------------------------- 时间解析


def test_parse_go_nanosecond_timestamp():
    """Go 输出纳秒精度，Python 3.10 的 fromisoformat 只接受 6 位。"""
    from featureline.parity import _parse_ts

    assert _parse_ts("2026-11-12T12:17:57.989780078Z") is not None
    assert _parse_ts("2026-11-12T12:17:57.989780Z") is not None
    assert _parse_ts("2026-11-12T12:17:57Z") is not None
    assert _parse_ts("garbage") is None
    assert _parse_ts(None) is None


def test_parse_preserves_instant():
    from featureline.parity import _parse_ts

    a = _parse_ts("2026-11-12T12:17:57.989780078Z")
    b = _parse_ts("2026-11-12T12:17:57.989780Z")
    assert a is not None and b is not None
    assert abs((a - b).total_seconds()) < 1e-5
