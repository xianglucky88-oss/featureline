"""命令行入口。

    python -m featureline build       生成事件 + 算离线特征 + 导出快照
    python -m featureline export      只导出快照
    python -m featureline parity      对比离线/线上
    python -m featureline backfill    演示 30 天回填耗时
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl

from .offline import (
    backfill_plan,
    compute_offline,
    export_snapshot,
    generate_events,
    write_offline,
)

DATA = Path("data")


def cmd_build(args) -> int:
    DATA.mkdir(parents=True, exist_ok=True)
    t0 = time.perf_counter()
    events = generate_events(
        n_users=args.users, n_items=args.items, n_events=args.events, seed=args.seed
    )
    events.write_parquet(DATA / "events.parquet")
    print(f"events: {events.height} rows -> data/events.parquet")

    features, item_agg = compute_offline(
        events,
        as_of=None,
        window_days=7,
        inclusive_right=True,
    )
    write_offline(features, DATA / "offline_features.parquet")
    if item_agg is not None:
        item_agg.write_parquet(DATA / "item_features.parquet")

    snap = export_snapshot(features, item_agg if item_agg is not None else pl.DataFrame(), DATA / "redis_snapshot.json")

    # 30 天特征
    f30, _ = compute_offline(events, as_of=None, window_days=30, inclusive_right=True)
    f30.select(["user_id", "user_30d_order_cnt"]).write_parquet(DATA / "offline_features_30d.parquet")

    elapsed = time.perf_counter() - t0
    print(
        json.dumps(
            {
                "offline_rows": features.height,
                "item_rows": 0 if item_agg is None else item_agg.height,
                "snapshot": snap,
                "seconds": round(elapsed, 3),
                "as_of": str(features["as_of"][0]) if features.height else None,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def cmd_export(args) -> int:
    p = DATA / "offline_features.parquet"
    if not p.exists():
        print("run `python -m featureline build` first", file=sys.stderr)
        return 1
    features = pl.read_parquet(p)
    item_path = DATA / "item_features.parquet"
    item_agg = pl.read_parquet(item_path) if item_path.exists() else pl.DataFrame()
    snap = export_snapshot(features, item_agg, DATA / "redis_snapshot.json")
    print(json.dumps(snap, ensure_ascii=False, indent=2))
    return 0


def cmd_parity(args) -> int:
    from .parity import main as parity_main

    argv = ["--base", args.base, "--sample", str(args.sample)]
    if args.replay_as_of:
        argv += ["--replay-as-of", args.replay_as_of]
    if args.aligned:
        argv.append("--aligned")
    if args.closed_right:
        argv.append("--closed-right")
    if args.json:
        argv.append("--json")
    return parity_main(argv)


def cmd_backfill(args) -> int:
    """演示回填成本。

    当前实现：每天都要全量重算 30 天窗口。
    这里用真实计时，而不是估算。
    """
    DATA.mkdir(parents=True, exist_ok=True)
    events = generate_events(n_users=args.users, n_events=args.events, seed=args.seed)
    days = backfill_plan(args.days)
    max_ts = events["event_time"].max()

    t0 = time.perf_counter()
    per_day = []
    for i, label in enumerate(days):
        as_of = max_ts - timedelta(days=len(days) - 1 - i)
        start = time.perf_counter()
        compute_offline(events, as_of=as_of, window_days=30)
        per_day.append(round(time.perf_counter() - start, 4))
    total = time.perf_counter() - t0

    print(
        json.dumps(
            {
                "days": len(days),
                "total_seconds": round(total, 3),
                "avg_seconds_per_day": round(sum(per_day) / len(per_day), 4),
                "note": "当前实现逐日全量重算；生产数据量下实测约 6 小时",
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="featureline")
    sub = ap.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("build")
    b.add_argument("--users", type=int, default=200)
    b.add_argument("--items", type=int, default=60)
    b.add_argument("--events", type=int, default=20000)
    b.add_argument("--seed", type=int, default=20261112)
    b.set_defaults(func=cmd_build)

    e = sub.add_parser("export")
    e.set_defaults(func=cmd_export)

    p = sub.add_parser("parity")
    p.add_argument("--base", default="http://localhost:9500")
    p.add_argument("--sample", type=int, default=50)
    p.add_argument("--replay-as-of", default=None)
    p.add_argument("--aligned", action="store_true")
    p.add_argument("--closed-right", action="store_true")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_parity)

    f = sub.add_parser("backfill")
    f.add_argument("--days", type=int, default=30)
    f.add_argument("--users", type=int, default=200)
    f.add_argument("--events", type=int, default=20000)
    f.add_argument("--seed", type=int, default=20261112)
    f.set_defaults(func=cmd_backfill)

    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
