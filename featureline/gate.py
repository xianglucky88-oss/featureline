"""发布前训练/服务一致性门禁。

定位：**发布流程中的阻断式检查**，与 parity / verify_features 两个"漂移演示工具"
不同，本模块给出的是放行/不放行的结论，并且每个结论都带可定位来源的证据。

基准：同一批事件、同一批实体、同一个业务时点（as_of）。在此基准上独立复算特征，
然后三方对比：

    复算结果（基准） vs 训练产物（离线 parquet）
                     vs 已发布快照（redis_snapshot.json）
                     vs 在线响应（/v1/features/:entity）

设计原则：

    * 每个差异都归类到确定的原因（见 Cause），并保留时间、版本、特征级证据；
    * 服务不可用、数据未到达、版本混用、读取不稳定一律判"无法结论"（exit 2），
      绝不把未验证当作相等；
    * 抽样分层、确定性、可升级：先低成本抽查，异常层自动升级到全量，
      抽样口径写进报告，可解释、可复算；
    * 比较精度由策略固定（计数类精确比较），命令行只能收紧、不能放宽，
      防止通过降低精度掩盖真实漂移；
    * 比较对象钉版本：快照必须来自同一次发布（单一 updated_at），
      在线值的 updated_at 必须与快照版本一致，否则拒绝下结论；
    * 在线值出现差异时做确认性重读，updated_at 变化说明有并发写入，
      改判 UNSTABLE_READ 而不是把瞬时假差异记成漂移。

退出码：0 = 放行，1 = 发现漂移（阻断），2 = 无法结论（阻断，需先解决前置问题）。
"""

from __future__ import annotations

import json
import math
import random
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable

import polars as pl

from .offline import compute_offline
from .parity import _parse_ts, fetch_online

# ---------------------------------------------------------------- 契约

USER_FEATURES = [
    "user_7d_order_cnt",
    "user_7d_gmv_cents",
    "user_30d_order_cnt",
    "user_category_affinity",
]
ITEM_FEATURES = ["item_7d_ctr", "item_7d_exposure"]

# 门禁认定的特征契约。与 Go 注册表（internal/feature/spec.go）一一对应；
# 任一侧变更（增删特征、改窗口、改默认值）都会被 definition_change 捕获。
CONTRACT: dict[str, dict[str, Any]] = {
    "user_7d_order_cnt": {"kind": "count", "window_days": 7, "default": 0.0},
    "user_7d_gmv_cents": {"kind": "numeric", "window_days": 7, "default": 0.0},
    "user_30d_order_cnt": {"kind": "count", "window_days": 30, "default": 0.0},
    "user_category_affinity": {"kind": "categorical", "window_days": 30, "default": 0.0},
    "item_7d_ctr": {"kind": "ratio", "window_days": 7, "default": 0.0},
    "item_7d_exposure": {"kind": "count", "window_days": 7, "default": 0.0},
}

# 各类型允许的最大比较误差。计数/类别必须精确相等；比率只允许浮点噪声级误差。
KIND_TOLERANCE = {"count": 0.0, "numeric": 0.0, "categorical": 0.0, "ratio": 1e-9}

# 命令行允许传入的容差上限。超过即拒绝运行——放宽精度不能由调用方决定。
MAX_USER_TOLERANCE = 1e-6

# 差异确认时重读在线值的次数。两次读取间 updated_at 变化即判并发写。
CONFIRM_READS = 1


class Cause(str, Enum):
    DATA_NOT_ARRIVED = "data_not_arrived"  # 数据尚未到达：事件最大时间落后于基准时点
    SNAPSHOT_STALE = "snapshot_stale"  # 快照落后：快照版本时间早于基准时点
    DEFINITION_CHANGE = "definition_change"  # 定义变更：注册表/产物与契约不符
    WINDOW_BOUNDARY = "window_boundary"  # 窗口边界：差异仅由窗口开闭口径引起
    NULL_PROPAGATION = "null_propagation"  # 空值传播：一侧缺失被默认值掩盖
    ONLINE_READ_FAILURE = "online_read_failure"  # 在线读取失败
    VERSION_MIX = "version_mix"  # 比较对象跨版本混用
    UNSTABLE_READ = "unstable_read"  # 读取期间发生并发写入，无法定论
    TIME_BASE_MISMATCH = "time_base_mismatch"  # 时间基准不一致
    VALUE_MISMATCH = "value_mismatch"  # 排除上述原因后的真实值差异


# 这些原因意味着"门禁无法验证"，而不是"特征相等"。
INCONCLUSIVE_CAUSES = {
    Cause.DATA_NOT_ARRIVED,
    Cause.ONLINE_READ_FAILURE,
    Cause.VERSION_MIX,
    Cause.UNSTABLE_READ,
}

DRIFT_CAUSES = set(Cause) - INCONCLUSIVE_CAUSES

# 证据排序优先级：越能直接定位问题的差异越靠前，避免被大面积同类证据淹没。
_CAUSE_PRIORITY = [
    Cause.VALUE_MISMATCH,
    Cause.WINDOW_BOUNDARY,
    Cause.NULL_PROPAGATION,
    Cause.DEFINITION_CHANGE,
    Cause.SNAPSHOT_STALE,
    Cause.VERSION_MIX,
    Cause.TIME_BASE_MISMATCH,
    Cause.UNSTABLE_READ,
    Cause.ONLINE_READ_FAILURE,
    Cause.DATA_NOT_ARRIVED,
]


# ---------------------------------------------------------------- 证据


@dataclass
class Finding:
    entity: str
    feature: str
    source: str  # snapshot / offline_parquet / online / registry / pipeline
    cause: Cause
    detail: str
    recompute: float | None = None
    artifact: float | None = None
    window_start: datetime | None = None
    window_end: datetime | None = None
    snapshot_version: datetime | None = None
    artifact_updated_at: datetime | None = None
    online_as_of: datetime | None = None
    stratum: str = ""

    def to_dict(self) -> dict[str, Any]:
        def ts(v: datetime | None) -> str | None:
            return v.isoformat() if v else None

        return {
            "entity": self.entity,
            "feature": self.feature,
            "source": self.source,
            "cause": self.cause.value,
            "detail": self.detail,
            "recompute": self.recompute,
            "artifact": self.artifact,
            "window": {"start": ts(self.window_start), "end": ts(self.window_end)},
            "snapshot_version": ts(self.snapshot_version),
            "artifact_updated_at": ts(self.artifact_updated_at),
            "online_as_of": ts(self.online_as_of),
            "stratum": self.stratum,
        }


@dataclass
class Stratum:
    """一个抽样层。reason 写进报告，说明这一层为什么存在。"""

    name: str
    reason: str
    kind: str  # "user" | "item"
    population: list[str]
    sample: list[str]
    escalated: bool = False
    mismatched_entities: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "reason": self.reason,
            "population": len(self.population),
            "sampled": len(self.sample),
            "escalated": self.escalated,
            "mismatched_entities": self.mismatched_entities,
        }


# ---------------------------------------------------------------- 快照


@dataclass
class Snapshot:
    values: dict[tuple[str, str], float]
    updated: dict[tuple[str, str], datetime]
    nullish: set[tuple[str, str]]  # 值为 NaN 的键：按缺失处理并留证
    versions: set[datetime]

    @property
    def version(self) -> datetime | None:
        return next(iter(self.versions)) if len(self.versions) == 1 else None


def parse_snapshot(raw: dict[str, Any]) -> Snapshot:
    """解析发布的快照 JSON（key 形如 f:{entity}:{feature}）。"""
    values: dict[tuple[str, str], float] = {}
    updated: dict[tuple[str, str], datetime] = {}
    nullish: set[tuple[str, str]] = set()
    versions: set[datetime] = set()
    for key, entry in raw.items():
        parts = key.split(":", 2)
        if len(parts) != 3 or parts[0] != "f":
            continue
        ent, feat = parts[1], parts[2]
        try:
            score = float(entry.get("score"))
        except (TypeError, ValueError):
            nullish.add((ent, feat))
            continue
        if math.isnan(score) or math.isinf(score):
            # 非有限值写进快照本身就是空值传播的信号
            nullish.add((ent, feat))
            continue
        values[(ent, feat)] = score
        ts = _parse_ts(entry.get("updated_at"))
        if ts:
            updated[(ent, feat)] = ts
            versions.add(ts)
    return Snapshot(values, updated, nullish, versions)


# ---------------------------------------------------------------- 在线侧


def fetch_registry(base: str) -> dict[str, Any] | None:
    try:
        with urllib.request.urlopen(f"{base}/v1/registry", timeout=10) as r:
            return json.loads(r.read().decode())
    except Exception:  # noqa: BLE001
        return None


def _online_view(resp: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """把在线响应整理成 {feature: {present, value, updated_at, note}}。"""
    out: dict[str, dict[str, Any]] = {}
    feats = resp.get("features") or {}
    debug = resp.get("debug") or {}
    for name, dbg in debug.items():
        out[name] = {
            "present": bool(dbg.get("found")),
            "value": feats.get(name),
            "updated_at": _parse_ts(dbg.get("updated_at")),
            "note": dbg.get("note"),
        }
    for name, v in feats.items():
        out.setdefault(name, {"present": True, "value": v, "updated_at": None, "note": None})
    return out


# ---------------------------------------------------------------- 抽样


def build_strata(
    events: pl.DataFrame,
    baseline: datetime,
    *,
    closed_right: bool,
    seed: int,
    max_entities: int,
) -> list[Stratum]:
    """分层抽样。

    层的划分直接对应风险来源：
        boundary    窗口边缘上有事件的实体 —— 窗口开闭口径差异只在这里显形
        active      窗口内有活动的实体 —— 值漂移的主要承载者
        stale_only  只有旧窗口活动的实体 —— 快照过期/时间基准问题在这里显形
        silent      长期无事件的实体 —— 空值/默认值传播路径
    """
    start7 = baseline - timedelta(days=7)
    start30 = baseline - timedelta(days=30)
    t = pl.col("event_time")

    def users_in(mask) -> set[str]:
        return set(events.filter(mask)["user_id"].unique().to_list())

    def items_in(mask) -> set[str]:
        return set(events.filter(mask)["item_id"].unique().to_list())

    in7 = (t >= start7) & ((t <= baseline) if closed_right else (t < baseline))
    in30 = (t >= start30) & ((t <= baseline) if closed_right else (t < baseline))
    on_edge = (t == baseline) | (t == start7) | (t == start30)

    all_users = set(events["user_id"].unique().to_list())
    all_items = set(events["item_id"].unique().to_list())
    active_u, active_i = users_in(in7), items_in(in7)
    in30_u = users_in(in30)

    pops: list[tuple[str, str, str, list[str], int]] = [
        (
            "boundary",
            "窗口边缘（as_of 或窗口起点）上有事件的实体，窗口开闭口径差异只在这里显形",
            "user",
            sorted(users_in(on_edge)),
            10**9,  # 边界实体通常很少，全取
        ),
        (
            "active",
            "近 7 天窗口内有活动的实体，值漂移的主要承载者",
            "user",
            sorted(active_u),
            max(30, math.ceil(0.05 * len(active_u))),
        ),
        (
            "stale_only",
            "只有 7~30 天前活动的实体，快照过期与时间基准问题在这里显形",
            "user",
            sorted(in30_u - active_u),
            max(20, math.ceil(0.05 * len(in30_u - active_u))),
        ),
        (
            "silent",
            "近 30 天无事件的实体，走空值/默认值路径",
            "user",
            sorted(all_users - in30_u),
            20,
        ),
        (
            "items_active",
            "窗口内有曝光/点击的商品",
            "item",
            sorted(active_i),
            max(10, math.ceil(0.10 * len(active_i))),
        ),
        (
            "items_other",
            "窗口内无活动的商品，走默认值路径",
            "item",
            sorted(all_items - active_i),
            10,
        ),
    ]

    rnd = random.Random(seed)
    strata: list[Stratum] = []
    budget = max_entities
    for name, reason, kind, pop, want in pops:
        # 边界层不参与预算压缩：它是成本最低、信号最强的层
        take = len(pop) if name == "boundary" else min(want, len(pop), max(budget, 0))
        sample = rnd.sample(pop, take) if take < len(pop) else list(pop)
        if name != "boundary":
            budget -= len(sample)
        strata.append(Stratum(name, reason, kind, pop, sample))
    return strata


# ---------------------------------------------------------------- 主流程


def run_gate(
    events: pl.DataFrame,
    *,
    snapshot: dict[str, Any] | None,
    offline_df: pl.DataFrame | None = None,
    base: str = "http://localhost:9500",
    as_of: datetime | None = None,
    closed_right: bool = False,
    seed: int = 20261115,
    max_entities: int = 500,
    escalate_threshold: float = 0.02,
    tolerance: float | None = None,
    lateness: timedelta = timedelta(0),
    time_base_allowance: timedelta = timedelta(hours=1),
    fetch: Callable[[str, str], dict[str, Any] | None] = fetch_online,
    fetch_reg: Callable[[str], dict[str, Any] | None] = fetch_registry,
    evidence_limit: int = 50,
) -> dict[str, Any]:
    """执行门禁。返回可 JSON 序列化的报告；decision ∈ pass/fail/inconclusive。"""
    if tolerance is not None and tolerance > MAX_USER_TOLERANCE:
        raise ValueError(
            f"tolerance {tolerance} 超过上限 {MAX_USER_TOLERANCE}："
            "门禁不允许通过放宽精度放行"
        )
    tolerances = {
        name: (
            min(KIND_TOLERANCE[spec["kind"]], tolerance)
            if tolerance is not None
            else KIND_TOLERANCE[spec["kind"]]
        )
        for name, spec in CONTRACT.items()
    }

    preflight: list[dict[str, Any]] = []
    findings: list[Finding] = []

    def pre(name: str, ok: bool, detail: str, severity: str = "block") -> bool:
        preflight.append({"name": name, "ok": ok, "severity": severity, "detail": detail})
        return ok

    def report(decision: str, strata: list[Stratum], summary: dict[str, Any], extra=None) -> dict[str, Any]:
        rep = {
            "decision": decision,
            "baseline": {
                "as_of": baseline.isoformat() if baseline else None,
                "window_days": 7,
                "closed_right": closed_right,
            },
            "versions": versions_info,
            "data": data_info,
            "tolerances": tolerances,
            "preflight": preflight,
            "sampling": {
                "seed": seed,
                "max_entities": max_entities,
                "escalate_threshold": escalate_threshold,
                "strata": [s.to_dict() for s in strata],
            },
            "summary": summary,
            "evidence": [
                f.to_dict()
                for f in sorted(
                    findings,
                    key=lambda x: (_CAUSE_PRIORITY.index(x.cause), x.entity, x.feature),
                )[:evidence_limit]
            ],
        }
        if extra:
            rep.update(extra)
        return rep

    versions_info: dict[str, Any] = {}
    data_info: dict[str, Any] = {}
    baseline: datetime | None = as_of

    # ---------- 前置 1：数据到达 ----------
    if events.height == 0:
        pre("events_loaded", False, "事件表为空")
        data_info = {"rows": 0}
        return report("inconclusive", [], {"findings": 0})
    data_max = events["event_time"].max()
    data_info = {"rows": events.height, "max_event_time": data_max.isoformat()}
    pre("events_loaded", True, f"{events.height} 行事件")

    # ---------- 前置 2：快照版本钉扎 ----------
    if snapshot is None:
        pre("snapshot_present", False, "未提供已发布快照，无法钉住比较对象的版本")
        return report("inconclusive", [], {"findings": 0})
    snap = parse_snapshot(snapshot)
    versions_info["snapshot_versions_seen"] = sorted(v.isoformat() for v in snap.versions)
    single_version = len(snap.versions) == 1
    pre(
        "snapshot_single_version",
        single_version,
        f"快照来自单次发布（版本 {snap.version.isoformat()}）" if single_version
        else f"快照包含 {len(snap.versions)} 个不同的 updated_at："
        "比较对象跨版本混用，任何结论都不可信，拒绝比较",
    )
    if not single_version:
        findings.append(
            Finding("*", "*", "snapshot", Cause.VERSION_MIX,
                    f"快照内出现多个版本：{sorted(v.isoformat() for v in snap.versions)}")
        )
        return report("inconclusive", [], {"findings": len(findings)})
    snap_version = snap.version
    versions_info["snapshot"] = snap_version.isoformat() if snap_version else None

    # ---------- 基准时点 ----------
    if baseline is None:
        # 未显式指定时，以快照发布时间为准：发布物自称的业务时点就是基准。
        baseline = snap_version
    if baseline is None:
        pre("baseline", False, "无法确定基准时点：--as-of 未给且快照没有版本时间")
        return report("inconclusive", [], {"findings": 0})

    # ---------- 前置 3：数据到达（相对基准） ----------
    arrived = data_max >= baseline - lateness
    pre(
        "data_arrived",
        arrived,
        f"数据已到达基准时点（max(event_time)={data_max.isoformat()}）" if arrived
        else f"事件最大时间 {data_max.isoformat()} 落后于基准 {baseline.isoformat()}："
        "数据尚未到达，窗口内容不完整，比较无意义",
    )
    if not arrived:
        findings.append(
            Finding("*", "*", "pipeline", Cause.DATA_NOT_ARRIVED,
                    f"max(event_time)={data_max.isoformat()} < as_of={baseline.isoformat()}")
        )
        return report("inconclusive", [], {"findings": len(findings)})

    # ---------- 前置 4：快照是否落后于基准 ----------
    snapshot_stale = bool(snap_version and snap_version != baseline)
    if snapshot_stale:
        pre(
            "snapshot_fresh",
            False,
            f"快照版本 {snap_version.isoformat()} 与基准 {baseline.isoformat()} 不一致：快照落后",
            severity="drift",
        )
        findings.append(
            Finding("*", "*", "snapshot", Cause.SNAPSHOT_STALE,
                    f"快照版本 {snap_version.isoformat()} != 基准 {baseline.isoformat()}",
                    snapshot_version=snap_version)
        )
    else:
        pre("snapshot_fresh", True, "快照版本与基准一致")

    # ---------- 前置 5：在线可达 + 定义未变更 ----------
    registry = fetch_reg(base)
    if registry is None:
        pre("online_reachable", False, f"无法获取 {base}/v1/registry")
        findings.append(
            Finding("*", "*", "online", Cause.ONLINE_READ_FAILURE, "registry 不可达")
        )
        return report("inconclusive", [], {"findings": len(findings)})
    pre("online_reachable", True, "registry 可达")

    reg_feats = registry.get("features") or {}
    def_problems: list[str] = []
    for name, spec in CONTRACT.items():
        got = reg_feats.get(name)
        if got is None:
            def_problems.append(f"注册表缺少特征 {name}")
            continue
        want_ns = spec["window_days"] * 86400 * 10**9
        if got.get("window_ns") != want_ns:
            def_problems.append(
                f"{name} 窗口定义变更：线上 {got.get('window_ns')}ns != 契约 {want_ns}ns"
            )
        if got.get("kind") != spec["kind"]:
            def_problems.append(f"{name} 类型变更：{got.get('kind')} != {spec['kind']}")
        if float(got.get("default", 0.0)) != spec["default"]:
            def_problems.append(f"{name} 默认值变更：{got.get('default')} != {spec['default']}")
    for name in sorted(set(reg_feats) - set(CONTRACT)):
        def_problems.append(f"注册表出现契约外特征 {name}")
    if offline_df is not None:
        missing_cols = [f for f in USER_FEATURES if f not in offline_df.columns]
        def_problems += [f"训练产物缺少列 {c}" for c in missing_cols]
    pre("definition_stable", not def_problems, "；".join(def_problems) or "定义与契约一致",
        severity="drift" if def_problems else "block")
    for p in def_problems:
        findings.append(Finding("*", "*", "registry", Cause.DEFINITION_CHANGE, p))

    # ---------- 训练产物的时间基准 ----------
    parquet_skew = False
    parquet_as_of: datetime | None = None
    parquet_rows: dict[str, dict[str, Any]] = {}
    if offline_df is not None:
        if "as_of" in offline_df.columns and offline_df.height:
            parquet_as_of = offline_df["as_of"][0]
            if parquet_as_of.tzinfo is None:
                parquet_as_of = parquet_as_of.replace(tzinfo=timezone.utc)
            parquet_skew = abs((parquet_as_of - baseline).total_seconds()) > 1
        for row in offline_df.iter_rows(named=True):
            parquet_rows[row["user_id"]] = row
        pre(
            "offline_time_base",
            not parquet_skew,
            f"训练产物 as_of={parquet_as_of} 基准={baseline.isoformat()}",
            severity="drift" if parquet_skew else "block",
        )
    else:
        pre("offline_time_base", True, "未提供训练产物，跳过该来源", severity="warn")
    versions_info["offline_parquet_as_of"] = (
        parquet_as_of.isoformat() if parquet_as_of else None
    )

    # ---------- 独立复算（基准） ----------
    rec_user, rec_item = compute_offline(events, as_of=baseline, window_days=7,
                                         inclusive_right=closed_right)
    # 反口径复算：用于把"窗口边界"从其他原因中分离出来
    alt_user, alt_item = compute_offline(events, as_of=baseline, window_days=7,
                                         inclusive_right=not closed_right)
    rec_u = {r["user_id"]: r for r in rec_user.iter_rows(named=True)}
    alt_u = {r["user_id"]: r for r in alt_user.iter_rows(named=True)}
    rec_i = {r["item_id"]: r for r in rec_item.iter_rows(named=True)}
    alt_i = {r["item_id"]: r for r in alt_item.iter_rows(named=True)}

    # ---------- 抽样 ----------
    strata = build_strata(events, baseline, closed_right=closed_right,
                          seed=seed, max_entities=max_entities)

    start7 = baseline - timedelta(days=7)
    checks = {"snapshot": [0, 0], "offline_parquet": [0, 0], "online": [0, 0]}  # [checked, diffs]
    online_as_ofs: list[datetime] = []

    def close(a: float | None, b: float | None, tol: float) -> bool:
        if a is None or b is None:
            return a is None and b is None
        if math.isnan(a) or math.isnan(b):
            return False
        return abs(a - b) <= tol

    def boundary_explains(entity: str, feature: str, value: float | None, kind: str) -> bool:
        """该差异是否仅由窗口开闭口径引起：反口径复算值与产物值相等。"""
        alt_row = (alt_u if kind == "user" else alt_i).get(entity)
        if alt_row is None:
            return value is None
        alt_v = alt_row.get(feature)
        if alt_v is None:
            return value is None
        return close(float(alt_v), value, tolerances[feature])

    def compare_source(entity: str, feature: str, kind: str, source: str,
                       art_present: bool, art_value: float | None,
                       art_updated: datetime | None, stratum: str,
                       online_meta: dict[str, Any] | None = None) -> None:
        rec_row = (rec_u if kind == "user" else rec_i).get(entity)
        rec_v = rec_row.get(feature) if rec_row else None
        if rec_v is not None:
            rec_v = float(rec_v)
        tol = tolerances[feature]
        checks[source][0] += 1

        def find(cause: Cause, detail: str) -> None:
            checks[source][1] += 1
            findings.append(Finding(
                entity, feature, source, cause, detail,
                recompute=rec_v, artifact=art_value,
                window_start=start7, window_end=baseline,
                snapshot_version=snap_version, artifact_updated_at=art_updated,
                online_as_of=(online_meta or {}).get("as_of"), stratum=stratum,
            ))

        # 1) 存在性：一侧有一侧没有 → 空值传播（除非快照本身落后）
        if not art_present and rec_v is None:
            return  # 两侧都按契约缺省，视为相等
        if not art_present:
            find(Cause.NULL_PROPAGATION,
                 f"{source} 缺少 {entity}/{feature}，复算值为 {rec_v}：缺失被默认值掩盖")
            return
        if rec_v is None:
            if art_value == 0.0 and not snapshot_stale:
                return  # 显式 0 与"无行"在契约下等价
            if source == "snapshot" and snapshot_stale:
                find(Cause.SNAPSHOT_STALE, "复算无此行，快照有过期值")
                return
            if boundary_explains(entity, feature, art_value, kind):
                find(Cause.WINDOW_BOUNDARY, "差异仅由窗口开闭口径引起")
                return
            find(Cause.VALUE_MISMATCH, "复算无此行，产物却有非零值")
            return

        # 2) 值比较
        if close(rec_v, art_value, tol):
            return
        if source == "snapshot" and snapshot_stale:
            find(Cause.SNAPSHOT_STALE,
                 f"快照版本 {snap_version} 落后于基准 {baseline.isoformat()}")
            return
        if source == "online" and snapshot_stale:
            # 在线值与过期快照一致 → 漂移来源是快照落后，不是在线计算
            snap_v = snap.values.get((entity, feature))
            if snap_v is not None and close(art_value, snap_v, tol):
                find(Cause.SNAPSHOT_STALE, "在线值与过期快照一致：漂移来自快照落后")
                return
        if source == "offline_parquet" and parquet_skew:
            find(Cause.TIME_BASE_MISMATCH,
                 f"训练产物 as_of={parquet_as_of} 与基准不一致")
            return
        if source == "online" and online_meta:
            if online_meta.get("version_mix"):
                find(Cause.VERSION_MIX,
                     "在线值 updated_at 与快照版本不一致：比较对象跨版本混用")
                return
            if online_meta.get("time_skew"):
                find(Cause.TIME_BASE_MISMATCH,
                     f"在线 as_of={online_meta.get('as_of')} 偏离基准 "
                     f"{baseline.isoformat()} 超过 {time_base_allowance}")
                return
        if boundary_explains(entity, feature, art_value, kind):
            find(Cause.WINDOW_BOUNDARY, "差异仅由窗口开闭口径引起")
            return
        find(Cause.VALUE_MISMATCH, "排除窗口/空值/版本/时间基准后仍不一致")

    def eval_entity(entity: str, kind: str, stratum: str) -> None:
        features = USER_FEATURES if kind == "user" else ITEM_FEATURES

        # --- 来源 1：训练产物 ---
        if offline_df is not None:
            prow = parquet_rows.get(entity)
            for f in features:
                if f not in (offline_df.columns if kind == "user" else []):
                    continue
                compare_source(entity, f, kind, "offline_parquet",
                               prow is not None and prow.get(f) is not None,
                               float(prow[f]) if prow and prow.get(f) is not None else None,
                               parquet_as_of, stratum)

        # --- 来源 2：已发布快照 ---
        for f in features:
            key = (entity, f)
            if key in snap.nullish:
                checks["snapshot"][0] += 1
                checks["snapshot"][1] += 1
                findings.append(Finding(
                    entity, f, "snapshot", Cause.NULL_PROPAGATION,
                    "快照值非有限（NaN/Inf）", stratum=stratum,
                    snapshot_version=snap_version,
                ))
                continue
            compare_source(entity, f, kind, "snapshot",
                           key in snap.values, snap.values.get(key),
                           snap.updated.get(key), stratum)

        # --- 来源 3：在线响应 ---
        resp = fetch(base, entity)
        if resp is None:
            checks["online"][0] += len(features)
            checks["online"][1] += len(features)
            findings.append(Finding(
                entity, "*", "online", Cause.ONLINE_READ_FAILURE,
                "在线读取失败：不计为相等，门禁无法结论", stratum=stratum,
            ))
            return
        o_as_of = _parse_ts(resp.get("as_of"))
        if o_as_of:
            online_as_ofs.append(o_as_of)
        time_skew = bool(o_as_of) and abs((o_as_of - baseline)) > time_base_allowance
        view = _online_view(resp)

        # 并发写确认：先算一遍，若出现差异则重读确认
        def online_meta_for(f: str) -> dict[str, Any]:
            v = view.get(f, {})
            return {
                "as_of": o_as_of,
                "time_skew": time_skew,
                "version_mix": bool(
                    v.get("updated_at") and snap_version and v["updated_at"] != snap_version
                ),
            }

        pending: list[tuple[str, bool, float | None]] = []
        for f in features:
            v = view.get(f, {"present": False, "value": None})
            pending.append((f, bool(v.get("present")), v.get("value")))

        diffs_exist = False
        rec_map = rec_u if kind == "user" else rec_i
        for f, present, value in pending:
            rec_row = rec_map.get(entity)
            rec_v = float(rec_row[f]) if rec_row and rec_row.get(f) is not None else None
            if (present and rec_v is None and value not in (None, 0.0)) or (
                present and rec_v is not None and not close(rec_v, float(value), tolerances[f])
            ) or (not present and rec_v is not None):
                diffs_exist = True
                break

        unstable = False
        if diffs_exist:
            # 确认性重读：updated_at 变了说明有并发写入，不能把瞬时状态记成漂移
            for _ in range(CONFIRM_READS):
                again = fetch(base, entity)
                if again is None:
                    continue
                aview = _online_view(again)
                for f, _, _ in pending:
                    old = view.get(f, {})
                    new = aview.get(f, {})
                    if old.get("updated_at") != new.get("updated_at") or (
                        old.get("present") and new.get("present")
                        and old.get("value") != new.get("value")
                    ):
                        unstable = True
                view.update(aview)
                if unstable:
                    break

        if unstable:
            # 读取期间有并发写入：任何瞬时差异都不能记成漂移，
            # 该实体的在线侧本轮无法结论。
            checks["online"][0] += len(features)
            checks["online"][1] += len(features)
            findings.append(Finding(
                entity, "*", "online", Cause.UNSTABLE_READ,
                "两次读取间 updated_at/值发生变化：存在并发写入，"
                "瞬时差异不计为漂移，本轮无法结论",
                snapshot_version=snap_version, online_as_of=o_as_of, stratum=stratum,
            ))
            return

        for f, present, value in pending:
            v = view.get(f, {})
            compare_source(entity, f, kind, "online",
                           bool(v.get("present", present)), v.get("value", value),
                           v.get("updated_at"), stratum,
                           online_meta=online_meta_for(f))

        # 时间基准本身就是证据：即使值碰巧相等，在线窗口右端偏离基准也要留证
        if time_skew:
            checks["online"][0] += 1
            checks["online"][1] += 1
            findings.append(Finding(
                entity, "*", "online", Cause.TIME_BASE_MISMATCH,
                f"在线 as_of={o_as_of.isoformat()} 偏离基准 {baseline.isoformat()} "
                f"超过 {time_base_allowance}：在线窗口右端取的是墙钟而非业务时点",
                snapshot_version=snap_version, online_as_of=o_as_of, stratum=stratum,
            ))

    # ---------- 执行 + 升级 ----------
    evaluated: set[tuple[str, str]] = set()

    def run_stratum(s: Stratum, entities: list[str]) -> None:
        before = len(findings)
        for ent in entities:
            if (s.kind, ent) in evaluated:
                continue
            evaluated.add((s.kind, ent))
            eval_entity(ent, s.kind, s.name)
        s.mismatched_entities += len({f.entity for f in findings[before:] if f.entity != "*"})

    for s in strata:
        run_stratum(s, s.sample)

    # 升级策略：某层实体异常率超过阈值 → 该层升级到全量（受升级预算约束）。
    # 这让"小样本没抽到"不能成为放行理由，同时把成本花在出问题的层上。
    escalation_budget = max_entities * 2
    for s in strata:
        if not s.sample:
            continue
        rate = s.mismatched_entities / len(s.sample)
        remaining = [e for e in s.population if e not in set(s.sample)]
        if rate > escalate_threshold and remaining and escalation_budget > 0:
            take = remaining[:escalation_budget]
            escalation_budget -= len(take)
            s.escalated = True
            s.sample = s.sample + take
            run_stratum(s, take)

    # ---------- 汇总 ----------
    by_cause: dict[str, int] = {}
    for f in findings:
        by_cause[f.cause.value] = by_cause.get(f.cause.value, 0) + 1
    if online_as_ofs:
        versions_info["online_as_of_range"] = [
            min(online_as_ofs).isoformat(), max(online_as_ofs).isoformat()
        ]
    summary = {
        "entities_evaluated": len(evaluated),
        "checks": {k: {"checked": v[0], "diffs": v[1]} for k, v in checks.items()},
        "findings": len(findings),
        "by_cause": by_cause,
    }

    causes = {f.cause for f in findings}
    if causes & DRIFT_CAUSES:
        decision = "fail"
    elif causes & INCONCLUSIVE_CAUSES:
        decision = "inconclusive"
    else:
        decision = "pass"
    return report(decision, strata, summary)


# ---------------------------------------------------------------- CLI


def main(argv: list[str] | None = None) -> int:
    import argparse
    import sys
    from pathlib import Path

    ap = argparse.ArgumentParser(prog="featureline gate")
    ap.add_argument("--base", default="http://localhost:9500")
    ap.add_argument("--events", default="data/events.parquet")
    ap.add_argument("--offline", default="data/offline_features.parquet")
    ap.add_argument("--snapshot", default="data/redis_snapshot.json")
    ap.add_argument("--as-of", default=None, help="基准业务时点（ISO 8601）。默认取快照版本时间")
    ap.add_argument("--closed-right", action="store_true",
                    help="规范窗口用左闭右闭（默认左闭右开，与线上契约一致）")
    ap.add_argument("--seed", type=int, default=20261115)
    ap.add_argument("--max-entities", type=int, default=500)
    ap.add_argument("--escalate-threshold", type=float, default=0.02)
    ap.add_argument("--tolerance", type=float, default=None,
                    help=f"收紧比较容差（上限 {MAX_USER_TOLERANCE}，超过即拒绝运行）")
    ap.add_argument("--lateness-minutes", type=int, default=0,
                    help="允许的数据迟到时长（分钟）")
    ap.add_argument("--time-base-allowance-minutes", type=float, default=60,
                    help="在线 as_of 与基准的允许偏差（分钟）")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    events_path = Path(args.events)
    if not events_path.exists():
        print(f"events not found: {events_path}; run `python -m featureline build`",
              file=sys.stderr)
        return 2
    events = pl.read_parquet(events_path)

    snap_path = Path(args.snapshot)
    snapshot = None
    if snap_path.exists():
        snapshot = json.loads(snap_path.read_text(encoding="utf-8"))
    else:
        print(f"warning: snapshot not found: {snap_path}", file=sys.stderr)

    offline_df = None
    off_path = Path(args.offline)
    if off_path.exists():
        offline_df = pl.read_parquet(off_path)

    try:
        result = run_gate(
            events,
            snapshot=snapshot,
            offline_df=offline_df,
            base=args.base,
            as_of=_parse_ts(args.as_of),
            closed_right=args.closed_right,
            seed=args.seed,
            max_entities=args.max_entities,
            escalate_threshold=args.escalate_threshold,
            tolerance=args.tolerance,
            lateness=timedelta(minutes=args.lateness_minutes),
            time_base_allowance=timedelta(minutes=args.time_base_allowance_minutes),
        )
    except ValueError as e:
        print(f"gate refused to run: {e}", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    else:
        decision_label = {"pass": "放行", "fail": "阻断（发现漂移）", "inconclusive": "阻断（无法结论）"}
        print(f"== 训练/服务一致性门禁：{decision_label[result['decision']]} ==")
        print(f"  基准时点：{result['baseline']['as_of']}  "
              f"窗口 [{'左闭右闭' if result['baseline']['closed_right'] else '左闭右开'}]")
        for p in result["preflight"]:
            mark = "OK " if p["ok"] else ("!! " if p["severity"] != "warn" else "~  ")
            print(f"  [{mark}] {p['name']}: {p['detail']}")
        s = result["summary"]
        print(f"  评估实体 {s['entities_evaluated']} 个，发现 {s['findings']} 个问题")
        for src, c in s["checks"].items():
            print(f"    {src:16} 比对 {c['checked']:5} 项，差异 {c['diffs']}")
        if s["by_cause"]:
            print("  按原因分布：")
            for c, n in sorted(s["by_cause"].items(), key=lambda x: -x[1]):
                print(f"    {c:22} {n}")
        samp = result["sampling"]
        print(f"  抽样（seed={samp['seed']}）：")
        for st in samp["strata"]:
            esc = "，已升级全量" if st["escalated"] else ""
            print(f"    {st['name']:14} 总体 {st['population']:5} 抽 {st['sampled']:5}"
                  f" 异常实体 {st['mismatched_entities']}{esc}")
        if result["evidence"]:
            print("  证据（前几条）：")
            for e in result["evidence"][:8]:
                print(f"    [{e['cause']}] {e['entity']}/{e['feature']} "
                      f"来源={e['source']} 复算={e['recompute']} 产物={e['artifact']}")
                print(f"        {e['detail']}")

    return {"pass": 0, "fail": 1, "inconclusive": 2}[result["decision"]]


if __name__ == "__main__":
    import sys

    sys.exit(main())
