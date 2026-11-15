"""一致性门禁的单元测试。

不依赖 Go 服务：在线侧全部用注入的 fake fetcher。
每个测试对应门禁必须区分的一种情况——关键断言不是"有差异"，
而是"差异被归到了正确的原因，且不可结论的情况没有被当成相等"。
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import polars as pl
import pytest

from featureline.gate import (
    CONTRACT,
    ITEM_FEATURES,
    USER_FEATURES,
    Cause,
    build_strata,
    parse_snapshot,
    run_gate,
)
from featureline.offline import compute_offline

AS_OF = datetime(2026, 11, 8, 0, 0, tzinfo=timezone.utc)
START7 = AS_OF - timedelta(days=7)


def ev(eid, uid, etype, ts, item="I001", cat="C01", amt=0):
    return {
        "event_id": eid,
        "user_id": uid,
        "item_id": item,
        "category_id": cat,
        "event_type": etype,
        "amount_cents": amt,
        "event_time": ts,
    }


def make_events(rows):
    df = pl.DataFrame(rows)
    return df.with_columns(pl.col("event_time").cast(pl.Datetime("us", "UTC"))).sort(
        "event_time"
    )


@pytest.fixture()
def events():
    """基准数据集：覆盖边界、活跃、陈旧、沉默四类实体。"""
    return make_events(
        [
            # U001：窗口内有订单（活跃）
            ev("E1", "U001", "order", datetime(2026, 11, 5, tzinfo=timezone.utc), amt=1000),
            # U002：订单恰好落在 as_of 上（右边界，开闭口径的分水岭）
            ev("E2", "U002", "order", AS_OF, amt=500),
            # U003：订单恰好落在窗口左端（两种口径都包含）
            ev("E3", "U003", "order", START7, amt=300),
            # U004：只有 7~30 天前的订单（陈旧）
            ev("E4", "U004", "order", datetime(2026, 10, 30, tzinfo=timezone.utc), amt=700),
            # U005：只有点击，没有订单（沉默路径）
            ev("E5", "U005", "click", datetime(2026, 11, 6, tzinfo=timezone.utc)),
            # 商品曝光/点击
            ev("E6", "U005", "exposure", datetime(2026, 11, 5, tzinfo=timezone.utc), item="I001"),
            ev("E7", "U005", "exposure", datetime(2026, 11, 5, tzinfo=timezone.utc), item="I002"),
        ]
    )


# ---------------------------------------------------------------- 构造辅助


def snap_from_recompute(events, as_of, closed_right, ts):
    """按给定口径复算并构造快照 dict（模拟一次发布）。"""
    rec_u, rec_i = compute_offline(events, as_of=as_of, window_days=7,
                                   inclusive_right=closed_right)
    data = {}
    for row in rec_u.iter_rows(named=True):
        for f in USER_FEATURES:
            data[f"f:{row['user_id']}:{f}"] = {
                "score": float(row[f]), "updated_at": ts.isoformat(), "source": "offline",
            }
    for row in rec_i.iter_rows(named=True):
        for f in ITEM_FEATURES:
            data[f"f:{row['item_id']}:{f}"] = {
                "score": float(row[f]), "updated_at": ts.isoformat(), "source": "offline",
            }
    return data


def online_resp(values, as_of, updated_at):
    """构造一次在线响应。values 里没有的特征按"存储缺失"处理。"""
    feats, dbg = {}, {}
    for f in USER_FEATURES + ITEM_FEATURES:
        if f in values:
            feats[f] = values[f]
            dbg[f] = {"found": True, "filled": values[f], "updated_at": updated_at.isoformat()}
        else:
            feats[f] = 0.0
            dbg[f] = {"found": False, "filled": 0.0, "note": "missing -> NaN -> 0 (serving)"}
    return {
        "user_id": "x",
        "features": feats,
        "debug": dbg,
        "as_of": as_of.isoformat(),
    }


def snap_values_for(snapshot, entity):
    out = {}
    for f in USER_FEATURES + ITEM_FEATURES:
        key = f"f:{entity}:{f}"
        if key in snapshot:
            out[f] = snapshot[key]["score"]
    return out


class FakeOnline:
    """按实体返回预置响应；支持一个实体排队多个响应（模拟并发写）。"""

    def __init__(self, responses):
        self.responses = responses  # entity -> list[resp] 或 resp 或 None
        self.calls = []

    def __call__(self, base, entity):
        self.calls.append(entity)
        r = self.responses.get(entity)
        if isinstance(r, list):
            return r.pop(0) if r else None
        return r


def registry_ok():
    return {
        "features": {
            name: {
                "name": name,
                "kind": spec["kind"],
                "window_ns": spec["window_days"] * 86400 * 10**9,
                "default": spec["default"],
                "event_time_column": "event_time",
            }
            for name, spec in CONTRACT.items()
        }
    }


def run(events, snapshot, online, registry=None, **kw):
    kw.setdefault("as_of", AS_OF)
    kw.setdefault("fetch", online)
    kw.setdefault("fetch_reg", lambda base: registry if registry is not None else registry_ok())
    return run_gate(events, snapshot=snapshot, **kw)


def causes(report):
    return {f["cause"] for f in report["evidence"]}


# ---------------------------------------------------------------- 放行路径


def test_pass_when_all_sources_aligned(events):
    """快照与在线都等于基准复算 → 放行。"""
    snap = snap_from_recompute(events, AS_OF, closed_right=False, ts=AS_OF)

    def fetch(base, entity):
        return online_resp(snap_values_for(snap, entity), AS_OF, AS_OF)

    rep = run(events, snap, fetch)
    assert rep["decision"] == "pass", rep["evidence"][:3]
    assert rep["summary"]["findings"] == 0
    # 每个来源都真的被比对过，而不是"没查就算等"
    for src in ("snapshot", "online"):
        assert rep["summary"]["checks"][src]["checked"] > 0


# ---------------------------------------------------------------- 窗口边界


def test_window_boundary_isolated(events):
    """快照用左闭右闭发布，门禁用左闭右开复核 → 差异只应归为窗口边界。"""
    snap = snap_from_recompute(events, AS_OF, closed_right=True, ts=AS_OF)

    def fetch(base, entity):
        return online_resp(snap_values_for(snap, entity), AS_OF, AS_OF)

    rep = run(events, snap, fetch, closed_right=False)
    assert rep["decision"] == "fail"
    # U002 的订单恰好在 as_of 上：差异必须归为窗口边界，而不是值漂移
    assert causes(rep) == {Cause.WINDOW_BOUNDARY.value}
    boundary_entities = {e["entity"] for e in rep["evidence"]}
    assert "U002" in boundary_entities
    assert "U001" not in boundary_entities  # 窗口内实体不受开闭影响


# ---------------------------------------------------------------- 快照落后


def test_snapshot_stale(events):
    """快照版本时间早于基准 → snapshot_stale，而不是 value_mismatch。"""
    old_ts = AS_OF - timedelta(days=14)
    snap = snap_from_recompute(events, AS_OF, closed_right=False, ts=AS_OF)
    # 篡改：版本时间改旧，并改一个值模拟旧数据
    for k in snap:
        snap[k]["updated_at"] = old_ts.isoformat()
    snap["f:U001:user_7d_order_cnt"]["score"] = 99.0

    def fetch(base, entity):
        return online_resp(snap_values_for(snap, entity), AS_OF, old_ts)

    rep = run(events, snap, fetch)
    assert rep["decision"] == "fail"
    assert Cause.SNAPSHOT_STALE.value in causes(rep)
    stale = [e for e in rep["evidence"] if e["cause"] == "snapshot_stale"]
    assert any(e["entity"] == "U001" for e in stale)


# ---------------------------------------------------------------- 空值传播


def test_null_propagation_when_online_missing(events):
    """在线侧存储缺失（found=false）而复算有值 → null_propagation。"""
    snap = snap_from_recompute(events, AS_OF, closed_right=False, ts=AS_OF)

    def fetch(base, entity):
        values = snap_values_for(snap, entity)
        if entity == "U001":
            values.pop("user_7d_order_cnt", None)  # 模拟存储丢失
        return online_resp(values, AS_OF, AS_OF)

    rep = run(events, snap, fetch)
    assert rep["decision"] == "fail"
    nulls = [e for e in rep["evidence"] if e["cause"] == "null_propagation"]
    assert any(e["entity"] == "U001" and e["feature"] == "user_7d_order_cnt" for e in nulls)


def test_nan_in_snapshot_is_null_evidence(events):
    snap = snap_from_recompute(events, AS_OF, closed_right=False, ts=AS_OF)
    snap["f:U001:user_7d_order_cnt"]["score"] = float("nan")
    parsed = parse_snapshot(snap)
    assert ("U001", "user_7d_order_cnt") in parsed.nullish


# ---------------------------------------------------------------- 在线读取失败


def test_online_unreachable_is_inconclusive_not_equal(events):
    """服务不可用绝不能计为特征相等。"""
    snap = snap_from_recompute(events, AS_OF, closed_right=False, ts=AS_OF)
    rep = run(events, snap, lambda base, entity: None)
    assert rep["decision"] == "inconclusive"
    assert Cause.ONLINE_READ_FAILURE.value in causes(rep)
    assert rep["summary"]["checks"]["online"]["diffs"] > 0


# ---------------------------------------------------------------- 版本混用


def test_mixed_snapshot_versions_refused(events):
    """快照里有两个版本 → 拒绝比较，inconclusive。"""
    snap = snap_from_recompute(events, AS_OF, closed_right=False, ts=AS_OF)
    snap["f:U001:user_7d_order_cnt"]["updated_at"] = (
        AS_OF - timedelta(days=3)
    ).isoformat()
    rep = run(events, snap, lambda b, e: None)
    assert rep["decision"] == "inconclusive"
    assert Cause.VERSION_MIX.value in causes(rep)
    # 拒绝后不应再产生任何值级结论
    assert not any(p["name"] == "data_arrived" for p in rep["preflight"])


def test_online_version_mix_detected(events):
    """在线值的 updated_at 与快照版本不一致 → version_mix。"""
    snap = snap_from_recompute(events, AS_OF, closed_right=False, ts=AS_OF)
    newer = AS_OF + timedelta(hours=2)

    def fetch(base, entity):
        values = snap_values_for(snap, entity)
        if entity == "U001":
            values["user_7d_order_cnt"] = 42.0  # 与快照不同的新写值
            resp = online_resp(values, AS_OF, AS_OF)
            resp["debug"]["user_7d_order_cnt"]["updated_at"] = newer.isoformat()
            return resp
        return online_resp(values, AS_OF, AS_OF)

    rep = run(events, snap, fetch)
    assert rep["decision"] == "inconclusive"
    assert Cause.VERSION_MIX.value in causes(rep)


# ---------------------------------------------------------------- 并发写入


def test_concurrent_write_is_unstable_not_drift(events):
    """两次读取间 updated_at 变化 → unstable_read，不能记成值漂移。"""
    snap = snap_from_recompute(events, AS_OF, closed_right=False, ts=AS_OF)
    t1 = AS_OF + timedelta(minutes=5)

    # U001 第一次读到一个"差异值"，确认重读时值和版本都变了（并发写入）
    responses = {}
    for ent in ("U001", "U002", "U003", "U004", "U005", "I001", "I002"):
        vals = snap_values_for(snap, ent)
        if ent == "U001":
            v1 = dict(vals, user_7d_order_cnt=50.0)
            v2 = dict(vals, user_7d_order_cnt=51.0)
            responses[ent] = [
                online_resp(v1, AS_OF, t1),
                online_resp(v2, AS_OF, t1 + timedelta(seconds=30)),
            ]
        else:
            responses[ent] = online_resp(vals, AS_OF, AS_OF)
    online = FakeOnline(responses)

    rep = run(events, snap, online)
    assert Cause.UNSTABLE_READ.value in causes(rep)
    assert Cause.VALUE_MISMATCH.value not in causes(rep)
    assert rep["decision"] == "inconclusive"


# ---------------------------------------------------------------- 时间基准


def test_online_time_base_mismatch(events):
    """在线 as_of 偏离基准（墙钟 vs 业务时点）→ time_base_mismatch。"""
    snap = snap_from_recompute(events, AS_OF, closed_right=False, ts=AS_OF)
    wall_clock = AS_OF + timedelta(days=21)

    def fetch(base, entity):
        return online_resp(snap_values_for(snap, entity), wall_clock, AS_OF)

    rep = run(events, snap, fetch)
    assert rep["decision"] == "fail"
    assert Cause.TIME_BASE_MISMATCH.value in causes(rep)


def test_data_not_arrived(events):
    """事件最大时间落后于基准 → 数据未到达，inconclusive。"""
    snap = snap_from_recompute(events, AS_OF, closed_right=False, ts=AS_OF)
    future = AS_OF + timedelta(days=3)
    rep = run(events, snap, lambda b, e: None, as_of=future)
    assert rep["decision"] == "inconclusive"
    assert Cause.DATA_NOT_ARRIVED.value in causes(rep)


# ---------------------------------------------------------------- 定义变更


def test_definition_change_detected(events):
    snap = snap_from_recompute(events, AS_OF, closed_right=False, ts=AS_OF)
    reg = registry_ok()
    del reg["features"]["item_7d_ctr"]  # 线上少了一个特征
    reg["features"]["user_7d_order_cnt"]["window_ns"] = 14 * 86400 * 10**9  # 窗口被改

    def fetch(base, entity):
        return online_resp(snap_values_for(snap, entity), AS_OF, AS_OF)

    rep = run(events, snap, fetch, registry=reg)
    assert rep["decision"] == "fail"
    defs = [e for e in rep["evidence"] if e["cause"] == "definition_change"]
    assert any("item_7d_ctr" in e["detail"] for e in defs)
    assert any("user_7d_order_cnt" in e["detail"] for e in defs)


# ---------------------------------------------------------------- 精度策略


def test_tolerance_cannot_be_widened(events):
    snap = snap_from_recompute(events, AS_OF, closed_right=False, ts=AS_OF)
    with pytest.raises(ValueError, match="上限"):
        run(events, snap, lambda b, e: None, tolerance=1.0)


def test_count_features_always_exact(events):
    """即使调用方给了容差，计数类特征仍必须精确相等。"""
    snap = snap_from_recompute(events, AS_OF, closed_right=False, ts=AS_OF)
    snap["f:U001:user_7d_order_cnt"]["score"] += 1e-8  # 小于任何容差的"漂移"

    def fetch(base, entity):
        return online_resp(snap_values_for(snap, entity), AS_OF, AS_OF)

    rep = run(events, snap, fetch, tolerance=1e-6)
    assert rep["decision"] == "fail"
    assert rep["tolerances"]["user_7d_order_cnt"] == 0.0


# ---------------------------------------------------------------- 抽样与升级


def test_sampling_deterministic_and_stratified(events):
    a = build_strata(events, AS_OF, closed_right=False, seed=7, max_entities=100)
    b = build_strata(events, AS_OF, closed_right=False, seed=7, max_entities=100)
    assert [(s.name, s.sample) for s in a] == [(s.name, s.sample) for s in b]
    names = {s.name for s in a}
    assert {"boundary", "active", "stale_only", "silent"} <= names
    boundary = next(s for s in a if s.name == "boundary")
    # U002 在 as_of 上有事件、U003 在窗口左端有事件 → 都应在边界层
    assert {"U002", "U003"} <= set(boundary.sample)
    # 每个层都写了存在理由
    assert all(s.reason for s in a)


def test_escalation_to_full_population(events):
    """异常率超阈值的层应升级到全量，并在报告里留痕。"""
    snap = snap_from_recompute(events, AS_OF, closed_right=False, ts=AS_OF)
    # 让所有在线值都错 → 所有层都超阈值 → 触发升级
    def fetch(base, entity):
        values = {f: 999.0 for f in USER_FEATURES + ITEM_FEATURES}
        return online_resp(values, AS_OF, AS_OF)

    rep = run(events, snap, fetch, max_entities=2, escalate_threshold=0.0)
    escalated = [s for s in rep["sampling"]["strata"] if s["escalated"]]
    assert escalated, "异常率 100% 时必须触发升级"
    for s in escalated:
        assert s["sampled"] == s["population"] or s["sampled"] > 2


# ---------------------------------------------------------------- 缺失实体不算相等


def test_missing_entity_in_snapshot_not_equal(events):
    """快照缺了整个实体，而复算有值 → null_propagation，不是相等。"""
    snap = snap_from_recompute(events, AS_OF, closed_right=False, ts=AS_OF)
    for k in [k for k in snap if k.startswith("f:U001:")]:
        del snap[k]

    def fetch(base, entity):
        return online_resp(snap_values_for(snap, entity), AS_OF, AS_OF)

    rep = run(events, snap, fetch)
    assert rep["decision"] == "fail"
    nulls = [e for e in rep["evidence"] if e["cause"] == "null_propagation"]
    assert any(e["entity"] == "U001" for e in nulls)
