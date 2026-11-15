"""featureline.bootstrap —— 训练侧注册表播种与绑定生成。

注册表的写操作在生产环境走 Go 服务的治理面（带文件锁与审计链）。
本地/CI 的训练流程需要在没有运行中服务时也能固定定义，因此这里提供
等价的播种能力：直接按与 Go 相同的磁盘格式写出 state.json，并生成
binding.json。指纹算法与 Go 逐字节一致，跨语言测试已锁定。
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .registry_client import (
    Binding,
    BindingEntry,
    WINDOW_HALF_OPEN_LEFT,
    fingerprint,
    make_fqvn,
)

SCHEMA_VERSION = 1


def seed_definitions() -> list[dict[str, Any]]:
    """与 Go registry.SeedDefinitions 完全同构的初始 6 个定义。"""
    week = 7 * 24 * 3600
    month = 30 * 24 * 3600
    ev = {"time_column": "event_time", "time_right_bound": "event_time", "time_zone": "UTC"}

    def one(name: str, entity: str, kind: str, window: int, desc: str) -> dict[str, Any]:
        return {
            "name": name,
            "entity": entity,
            "value_type": "float64",
            "kind": kind,
            "window": {"size_seconds": window, "convention": WINDOW_HALF_OPEN_LEFT},
            "time_semantics": ev,
            "default": {"type": "float64", "value": "0"},
            "description": desc,
            "owner": "platform",
            "tags": [],
        }

    return [
        one("user_7d_order_cnt", "user", "count", week, "用户近 7 天订单数"),
        one("user_7d_gmv_cents", "user", "numeric", week, "用户近 7 天成交金额（分）"),
        one("user_30d_order_cnt", "user", "count", month, "用户近 30 天订单数"),
        one("item_7d_ctr", "item", "ratio", week, "商品近 7 天点击率"),
        one("item_7d_exposure", "item", "count", week, "商品近 7 天曝光次数"),
        one("user_category_affinity", "user", "categorical", month, "用户偏好品类（编码后的类别 id）"),
    ]


def bootstrap_registry(registry_dir: str | Path, binding_path: str | Path) -> Binding:
    """在空目录播种 1.0 定义并生成绑定；已存在则只重新生成绑定。"""
    reg = Path(registry_dir)
    reg.mkdir(parents=True, exist_ok=True)
    state_path = reg / "state.json"
    now = datetime.now(timezone.utc)

    if state_path.exists():
        state = json.loads(state_path.read_text(encoding="utf-8"))
    else:
        versions: dict[str, Any] = {}
        active: dict[str, str] = {}
        for d in seed_definitions():
            fqvn = make_fqvn(d["name"], 1, 0)
            versions[fqvn] = {
                "fqvn": fqvn,
                "name": d["name"],
                "major": 1,
                "minor": 0,
                "status": "published",
                "definition": d,
                "fingerprint": fingerprint(d),
                "based_on": "",
                "change_note": "bootstrap",
                "revision": -1,
                "created_at": now.isoformat().replace("+00:00", "Z"),
                "published_at": now.isoformat().replace("+00:00", "Z"),
            }
            active[d["name"]] = fqvn
        state = {
            "schema_version": SCHEMA_VERSION,
            "versions": versions,
            "drafts": {},
            "active": active,
            "updated_at": now.isoformat().replace("+00:00", "Z"),
        }
        _atomic_write_json(state_path, state)
        _append_audit(reg, now)

    entries = [
        BindingEntry(name=n, fqvn=state["active"][n],
                     fingerprint=state["versions"][state["active"][n]]["fingerprint"])
        for n in sorted(state["active"])
    ]
    import secrets

    manifest_id = "bm-" + secrets.token_hex(16)
    binding = Binding(manifest_id=manifest_id, entries=entries, created_at=now)
    binding.write(binding_path)
    return binding


def _atomic_write_json(path: Path, payload: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _append_audit(reg: Path, now: datetime) -> None:
    audit = reg / "audit.jsonl"
    with audit.open("a", encoding="utf-8") as f:
        for i, d in enumerate(seed_definitions(), start=1):
            entry = {
                "seq": i,
                "at": now.isoformat().replace("+00:00", "Z"),
                "action": "publish",
                "name": d["name"],
                "fqvn": make_fqvn(d["name"], 1, 0),
                "revision": -1,
                "change_class": "",
                "detail": {"actor": "bootstrap"},
            }
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
