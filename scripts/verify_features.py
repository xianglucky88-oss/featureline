#!/usr/bin/env python3
"""verify_features.py —— 训练/服务一致性断言。

核心命题：**同一个实体、同一份已发布定义，离线算出的特征必须等于线上返回的特征**。

断言分五类给出证据：

    V0 定义绑定：在线响应带绑定坐标，且同一实例内绑定指纹一致
    V1 快照新鲜：线上窗口内无数据却返回非零值
    V2 窗口语义：统一为注册表规定的左闭右开，对齐 as_of 后 0 差异
    V3 缺失值语义：缺失实体按定义里的默认值填充，且返回值带 FQVN
    V4 时间基准：服务 as_of 与特征快照时间相差不超过 24 小时

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

    # ---------- V0 定义绑定 ----------
    result = compare(events, base=args.base, sample=args.sample)
    coherence = result.get("binding_coherence", {})
    add(
        "V0/definition-binding",
        bool(coherence.get("ok")),
        f"manifest={coherence.get('manifest_ids')} 指纹={coherence.get('fingerprints')} "
        f"问题={coherence.get('problems')}",
    )

    # ---------- V1 快照新鲜 ----------
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

    # ---------- V2 窗口语义（默认左闭右开，与注册表一致） ----------
    aligned = compare(events, base=args.base, sample=args.sample, aligned=True, inclusive_right=False)
    add(
        "V2/window-convention",
        aligned["mismatched"] == 0,
        f"对齐 as_of 后仍不一致 {aligned['mismatched']}/{aligned['checked']}"
        f"（{aligned['percent']}%）",
    )

    # ---------- V3 缺失值语义 + 版本坐标 ----------
    ghost = fetch(args.base, "U9999")
    if ghost is None:
        add("V3/missing-semantics", False, "无法获取不存在实体的响应")
    else:
        feats = ghost["features"]
        versions = ghost.get("versions", {})
        missing_coord = [n for n in feats if "@" not in versions.get(n, "")]
        all_default = all(v == 0 for v in feats.values())
        add(
            "V3/missing-semantics",
            all_default and not missing_coord,
            f"不存在实体返回 {len(feats)} 个默认值特征；"
            f"缺失 FQVN 坐标的特征={missing_coord}",
        )

    # ---------- V4 时间基准 ----------
    one = fetch(args.base, "U0001")
    if one is None:
        add("V4/time-base", False, "无法获取 U0001")
    else:
        as_of = _parse_ts(one.get("as_of"))
        # 只看实际命中值的 updated_at；缺失特征的零值时间不参与。
        updated = None
        for dbg in one.get("debug", {}).values():
            if dbg.get("found") and dbg.get("updated_at"):
                updated = _parse_ts(dbg["updated_at"])
                if updated is not None:
                    break
        if as_of is None or updated is None:
            add("V4/time-base", False, "时间字段解析失败或无命中值")
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
