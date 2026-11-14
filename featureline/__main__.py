"""命令行入口。

    python -m featureline build       生成事件 + 算离线特征 + 导出快照
    python -m featureline export      只导出快照
    python -m featureline parity      对比离线/线上
    python -m featureline backfill    时间点正确的增量回填（见 featureline/backfill.py）
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl

from .offline import (
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


def _make_labels(events: pl.DataFrame, days: int, seed: int) -> pl.DataFrame:
    """构造待回填的样本：最近 days 天，每天每个用户一条，label_time=当天 23:47。

    label_time 故意不取整日，用来验证边界日精修（窗口右端落在一天中间）。
    """
    import random

    rnd = random.Random(seed)
    max_ts = events["event_time"].max()
    users = sorted(events["user_id"].unique().to_list())
    items = sorted(events["item_id"].unique().to_list())
    rows = []
    for i in range(days):
        day = (max_ts - timedelta(days=i)).date()
        lt = datetime(day.year, day.month, day.day, 23, 47, tzinfo=timezone.utc)
        for u in users:
            rows.append(
                {
                    "label_id": f"{day}:{u}",
                    "user_id": u,
                    "item_id": rnd.choice(items),
                    "label_time": lt,
                }
            )
    return pl.DataFrame(rows).with_columns(
        pl.col("label_time").cast(pl.Datetime("us", "UTC"))
    )


def cmd_backfill(args) -> int:
    """时间点正确的增量回填。

    流程：ingest（幂等分区）→ build_state（增量 tiles）→ backfill_labels
    （按标签日分区输出）。随后做两件验证：

      1. 立刻重跑一次 —— 所有分区应报 unchanged（幂等）；
      2. 抽样与"每样本扫全量"的暴力参考实现逐值对比（正确性）。
    """
    from .backfill import (
        _compute,
        backfill_labels,
        brute_force_features,
        build_state,
        ingest_events,
    )

    lake = DATA / "lake"
    if lake.exists() and not args.keep:
        shutil.rmtree(lake)
    lake.mkdir(parents=True, exist_ok=True)

    t0 = time.perf_counter()
    events = generate_events(n_users=args.users, n_events=args.events, seed=args.seed)
    ing = ingest_events(events, lake)
    st = build_state(lake)
    t_state = time.perf_counter() - t0

    labels = _make_labels(events, args.days, seed=args.seed)
    labels.write_parquet(lake / "labels.parquet")

    t1 = time.perf_counter()
    summary = backfill_labels(labels, lake)
    t_backfill = time.perf_counter() - t1

    # 幂等性：重跑应全部 unchanged
    rerun = backfill_labels(labels, lake)
    idempotent = all(s["status"] == "unchanged" for s in rerun.values())

    # 正确性自检：抽样 vs 暴力全量扫描
    sample = labels.sample(min(300, labels.height), seed=1)
    fast = _compute(sample, lake).sort("label_id")
    ref = brute_force_features(events, sample).sort("label_id")
    mismatched = 0
    max_diff = 0.0
    for c in ("user_7d_order_cnt", "user_7d_gmv_cents", "user_30d_order_cnt",
              "user_category_affinity", "item_7d_ctr", "item_7d_exposure"):
        diff = (fast[c] - ref[c]).abs().max()
        max_diff = max(max_diff, float(diff) if diff is not None else 0.0)
        mismatched += int((fast[c] != ref[c]).sum())

    print(
        json.dumps(
            {
                "design": "point-in-time backfill: 每日预聚合 tiles + 边界日原始事件精修",
                "ingest": ing,
                "state": {
                    "rebuilt_partitions": len(st["rebuilt"]),
                    "watermark": st["watermark"],
                },
                "labels": labels.height,
                "label_dates": len(summary),
                "seconds": {
                    "ingest_and_state": round(t_state, 3),
                    "backfill": round(t_backfill, 3),
                    "per_label_date": round(t_backfill / max(len(summary), 1), 4),
                },
                "idempotent_rerun": idempotent,
                "verification": {
                    "sampled_labels": sample.height,
                    "mismatched_values": mismatched,
                    "max_abs_diff": max_diff,
                },
                "manifest": str(lake / "backfill" / "_manifest.json"),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if idempotent and mismatched == 0 else 1


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
    f.add_argument("--keep", action="store_true", help="复用已有 lake（不清空重建）")
    f.set_defaults(func=cmd_backfill)

    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
