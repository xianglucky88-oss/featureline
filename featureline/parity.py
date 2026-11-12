"""parity check：离线与线上特征的一致性对比。

这是本仓库最重要的工具：它把"训练-服务漂移"量化成一个百分比。

对比口径的设计（这是关键）：

    线上侧：服务读 Redis 快照，窗口右端 = **请求时刻**（time.Now）
    离线侧：按**训练样本的标签时间**重算窗口

这两者的右端天然不同——线上的 as_of 是"今天"，离线的 as_of 是"样本那天"。
如果训练数据是 3 周前产出的，而线上服务今天还在用同一份快照，
那么任何随时间变化的特征都会漂移。

要复现这个问题，`--replay-as-of` 模拟"离线在历史时点算特征、线上在当下读取"：

    python -m featureline parity --base http://localhost:9500 \
        --replay-as-of 2026-10-15T23:47:00Z

此时离线的窗口右端是 10-15，线上的窗口右端是今天（11-12），
两者相差近一个月，窗口内容完全不同。

`--aligned` 表示两边用同一个 as_of（理论上应该 0% 差异），
用于验证修复后窗口语义是否真的统一了。
"""

from __future__ import annotations

import json
import math
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import polars as pl

NUMERIC_FEATURES = [
    "user_7d_order_cnt",
    "user_7d_gmv_cents",
    "user_30d_order_cnt",
    "user_category_affinity",
]
ITEM_FEATURES = ["item_7d_ctr", "item_7d_exposure"]


@dataclass
class Diff:
    entity: str
    feature: str
    offline: float | None
    online: float | None
    reason: str

    @property
    def abs_diff(self) -> float:
        if self.offline is None or self.online is None:
            return math.inf
        return abs(self.offline - self.online)


def fetch_online(base: str, entity_id: str) -> dict[str, Any] | None:
    url = f"{base}/v1/features/{entity_id}"
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            return json.loads(r.read().decode())
    except Exception:  # noqa: BLE001
        return None


def _parse_ts(raw: str | None) -> datetime | None:
    """解析 ISO 8601 时间戳。

    坑：Go 的 time.Time 序列化后是纳秒精度（9 位小数），
    而 Python 3.10 的 datetime.fromisoformat 只接受 3 或 6 位小数。
    直接解析会抛 ValueError，早期版本没捕获，导致所有时间相关判断
    静默返回 None（parity 的 stale 检测因此完全失效）。
    """
    if not raw:
        return None
    s = raw.replace("Z", "+00:00")
    # 把小数秒截断到 6 位
    if "." in s:
        head, rest = s.split(".", 1)
        frac, sep, tz = rest.partition("+")
        if not sep:
            frac, sep, tz = rest.partition("-")
        frac = (frac + "000000")[:6]
        s = f"{head}.{frac}{sep}{tz}"
    try:
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except ValueError:
        return None


def _as_of_of(online: dict[str, Any]) -> datetime | None:
    return _parse_ts(online.get("as_of"))


def compare(
    events: pl.DataFrame,
    base: str = "http://localhost:9500",
    tolerance: float = 1e-6,
    sample: int = 50,
    replay_as_of: datetime | None = None,
    aligned: bool = False,
    inclusive_right: bool = True,
) -> dict[str, Any]:
    """逐实体对比。

    replay_as_of: 离线侧的窗口右端。None 表示用数据的最大时间。
    aligned:      若为 True，离线也按线上返回的 as_of 计算（用于验证修复）。
    inclusive_right: 离线窗口是否左闭右闭。
    """
    from .offline import compute_offline

    users = sorted({u for u in events["user_id"].unique().to_list()})[:sample]
    rows: list[Diff] = []
    checked = 0
    as_of_used: datetime | None = None

    for uid in users:
        online = fetch_online(base, uid)
        if online is None:
            # 每个实体按 NUMERIC_FEATURES 的数量计入分母，避免比例失真
            checked += len(NUMERIC_FEATURES)
            rows.append(Diff(uid, "*", None, None, "online unreachable"))
            continue

        if aligned:
            as_of = _as_of_of(online)
        else:
            as_of = replay_as_of
        as_of_used = as_of or as_of_used

        offline_df, _ = compute_offline(
            events,
            as_of=as_of,
            window_days=7,
            inclusive_right=inclusive_right,
        )
        mine = offline_df.filter(pl.col("user_id") == uid)
        if mine.height == 0:
            checked += len(NUMERIC_FEATURES)
            rows.append(Diff(uid, "*", None, None, "offline has no row"))
            continue
        row = mine.row(0, named=True)

        for f in NUMERIC_FEATURES:
            checked += 1
            off = row.get(f)
            on = online["features"].get(f)
            if off is None or on is None:
                rows.append(Diff(uid, f, off, on, "missing on one side"))
                continue
            if abs(float(off) - float(on)) > tolerance:
                rows.append(Diff(uid, f, float(off), float(on), "value mismatch"))

    # item 级
    item_ids = sorted(events["item_id"].unique().to_list())[:10]
    _, item_agg = compute_offline(
        events, as_of=replay_as_of, window_days=7, inclusive_right=inclusive_right
    )
    for iid in item_ids:
        online = fetch_online(base, iid)
        if online is None:
            checked += len(ITEM_FEATURES)
            rows.append(Diff(iid, "*", None, None, "online unreachable"))
            continue
        mine = item_agg.filter(pl.col("item_id") == iid)
        if mine.height == 0:
            continue
        row = mine.row(0, named=True)
        for f in ITEM_FEATURES:
            checked += 1
            off = row.get(f)
            on = online["features"].get(f)
            if off is None or on is None:
                rows.append(Diff(iid, f, off, on, "missing on one side"))
                continue
            if abs(float(off) - float(on)) > tolerance:
                rows.append(Diff(iid, f, float(off), float(on), "value mismatch"))

    # ---------- 关键检测：窗口与快照时间的矛盾 ----------
    # 线上服务用 time.Now() 作为窗口右端，但读的是离线快照（可能几周前的值）。
    # 如果窗口内根本没有事件，却返回了非零特征值，就说明"窗口"是装饰，
    # 真正被读的是过期快照。
    stale: list[dict] = []
    for uid in users[: min(10, len(users))]:
        online = fetch_online(base, uid)
        if online is None:
            continue
        as_of = _as_of_of(online)
        if as_of is None:
            continue
        wstart = as_of - timedelta(days=7)
        window_events = events.filter(
            (pl.col("event_time") >= wstart) & (pl.col("event_time") <= as_of)
        ).height
        nonzero = {
            k: v
            for k, v in online["features"].items()
            if k.startswith("user_") and abs(v) > 0
        }
        if window_events == 0 and nonzero:
            stale.append(
                {
                    "entity": uid,
                    "window_events": 0,
                    "as_of": as_of.isoformat(),
                    "features_returned": nonzero,
                    "snapshot_updated_at": online["debug"]
                    .get("user_7d_order_cnt", {})
                    .get("updated_at"),
                }
            )

    percent = min(len(rows) / checked, 1.0) if checked else 0.0

    by_feature: dict[str, int] = {}
    for d in rows:
        by_feature[d.feature] = by_feature.get(d.feature, 0) + 1

    return {
        "checked": checked,
        "mismatched": len(rows),
        "percent": round(percent * 100, 2),
        "by_feature": by_feature,
        "offline_as_of": as_of_used.isoformat() if as_of_used else None,
        "stale_snapshot": {
            "count": len(stale),
            "samples": stale[:5],
        },
        "samples": [
            {
                "entity": d.entity,
                "feature": d.feature,
                "offline": d.offline,
                "online": d.online,
                "reason": d.reason,
                "abs_diff": None if math.isinf(d.abs_diff) else d.abs_diff,
            }
            for d in sorted(rows, key=lambda x: -x.abs_diff)[:10]
        ],
    }


def main(argv: list[str] | None = None) -> int:
    import argparse
    import sys

    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://localhost:9500")
    ap.add_argument("--events", default="data/events.parquet")
    ap.add_argument("--sample", type=int, default=50)
    ap.add_argument("--replay-as-of", default=None, help="离线侧窗口右端（ISO 8601）")
    ap.add_argument("--aligned", action="store_true", help="两边用同一个 as_of")
    ap.add_argument("--closed-right", action="store_true", help="离线用左闭右闭")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    path = Path(args.events)
    if not path.exists():
        print(f"events not found: {path}", file=sys.stderr)
        print("run: python -m featureline build", file=sys.stderr)
        return 1

    events = pl.read_parquet(path)
    result = compare(
        events,
        base=args.base,
        sample=args.sample,
        replay_as_of=_parse_ts(args.replay_as_of),
        aligned=args.aligned,
        inclusive_right=args.closed_right,
    )

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        mode = "两边对齐 as_of" if args.aligned else "离线历史时点 vs 线上当前"
        print(f"== 训练/服务一致性（{mode}）==")
        if result.get("offline_as_of"):
            print(f"  离线窗口右端：{result['offline_as_of']}")
        print(
            f"  抽查 {result['checked']} 个特征值，不一致 {result['mismatched']} 个"
            f"（{result['percent']}%）"
        )
        if result["by_feature"]:
            print("  按特征分布：")
            for f, n in sorted(result["by_feature"].items(), key=lambda x: -x[1]):
                print(f"    {f:26} {n}")
        stale = result.get("stale_snapshot") or {}
        if stale.get("count"):
            print(f"  ⚠ 快照过期：{stale['count']} 个实体的窗口内无事件，却返回了非零特征值")
            for s_ in stale["samples"][:3]:
                print(
                    f"    {s_['entity']}: 窗口内 {s_['window_events']} 条事件，"
                    f"却返回 {s_['features_returned']}，"
                    f"快照时间 {s_['snapshot_updated_at']}"
                )

        if result["samples"]:
            print("  差异最大的样本：")
            for s in result["samples"]:
                print(
                    f"    {s['entity']}/{s['feature']}: "
                    f"离线 {s['offline']} vs 线上 {s['online']} ({s['reason']})"
                )
    return 0 if result["mismatched"] == 0 else 1


if __name__ == "__main__":
    import sys

    sys.exit(main())
