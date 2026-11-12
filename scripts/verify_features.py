#!/usr/bin/env python3
"""verify_features.py —— 训练/服务一致性断言。

核心命题：**同一个实体，离线算出的特征必须等于线上返回的特征**。

本脚本不只看"百分比"，而是分三类给出证据：

    V1 快照过期：线上窗口内无数据却返回非零值
    V2 窗口语义：离线左闭右闭 vs 线上左闭右开
    V3 缺失值语义：离线填 0 vs 线上填 NaN 后当 0

用法:
    python3 scripts/verify_features.py --base http://localhost:9500
    python3 scripts/verify_features.py --base http://localhost:9500 --json
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl  # noqa: E402

from featureline.offline import compute_offline  # noqa: E402
from featureline.parity import _parse_ts, compare  # noqa: E402


def fetch(base: str, entity: str) -> dict | None:
    try:
        with urllib.request.urlopen(f"{base}/v1/features/{entity}", timeout=10) as r:
            return json.loads(r.read().decode())
    except Exception:  # noqa: BLE001
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://localhost:9500")
    ap.add_argument("--events", default="data/events.parquet")
    ap.add_argument("--sample", type=int, default=40)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    path = Path(args.events)
    if not path.exists():
        print(f"events not found: {path}; run `python -m featureline build`", file=sys.stderr)
        return 1

    events = pl.read_parquet(path)
    checks: list[dict] = []

    def add(name: str, ok: bool, detail: str) -> None:
        checks.append({"name": name, "ok": ok, "detail": detail})

    # ---------- V1 快照过期 ----------
    result = compare(events, base=args.base, sample=args.sample)
    stale = result.get("stale_snapshot", {})
    add(
        "V1/snapshot-fresh",
        stale.get("count", 0) == 0,
        f"{stale.get('count', 0)} 个实体的窗口内无事件却返回非零特征值"
        + (
            f"（例：{stale['samples'][0]['entity']} 窗口 {stale['samples'][0]['window_events']} 条事件，"
            f"返回 {stale['samples'][0]['features_returned']}，"
            f"快照时间 {stale['samples'][0]['snapshot_updated_at']}）"
            if stale.get("samples")
            else ""
        ),
    )

    # ---------- V2 窗口语义 ----------
    aligned = compare(events, base=args.base, sample=args.sample, aligned=True, inclusive_right=True)
    add(
        "V2/window-convention",
        aligned["mismatched"] == 0,
        f"对齐 as_of 后仍不一致 {aligned['mismatched']}/{aligned['checked']}"
        f"（{aligned['percent']}%）—— 差异来自窗口开闭或缺失值语义",
    )

    # ---------- V3 缺失值语义 ----------
    # 取一个确定不存在的实体，看线上返回什么
    ghost = fetch(args.base, "U9999")
    if ghost is None:
        add("V3/missing-semantics", False, "无法获取不存在实体的响应")
    else:
        feats = ghost["features"]
        notes = {k: v.get("note") for k, v in ghost.get("debug", {}).items() if v.get("note")}
        add(
            "V3/missing-semantics",
            not notes,
            f"不存在实体返回 {feats}；debug note: {list(notes.values())[:2]}",
        )

    # ---------- V4 时间基准 ----------
    # 线上 as_of 应是"数据时间"而非"墙钟时间"
    one = fetch(args.base, "U0001")
    if one is None:
        add("V4/time-base", False, "无法获取 U0001")
    else:
        as_of = _parse_ts(one.get("as_of"))
        updated = _parse_ts(one.get("debug", {}).get("user_7d_order_cnt", {}).get("updated_at"))
        if as_of is None or updated is None:
            add("V4/time-base", False, "时间字段解析失败")
        else:
            gap_hours = abs((as_of - updated).total_seconds()) / 3600
            add(
                "V4/time-base",
                gap_hours < 24,
                f"服务 as_of {as_of:%Y-%m-%d %H:%M} 与特征快照时间 {updated:%Y-%m-%d %H:%M} "
                f"相差 {gap_hours:.0f} 小时（应 < 24）",
            )

    if args.json:
        print(json.dumps({"checks": checks, "parity": result}, ensure_ascii=False, indent=2, default=str))
    else:
        print("== 训练/服务一致性 ==")
        for c in checks:
            print(f"  [{'PASS' if c['ok'] else 'FAIL'}] {c['name']}: {c['detail']}")
        failed = sum(1 for c in checks if not c["ok"])
        print(f"\n结果: {len(checks) - failed}/{len(checks)} 通过")

    return 0 if all(c["ok"] for c in checks) else 1


if __name__ == "__main__":
    sys.exit(main())
