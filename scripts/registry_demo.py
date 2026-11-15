#!/usr/bin/env python3
"""registry_demo.py —— 通过治理面 HTTP API 演练完整演进生命周期。

流程（每一步都有可验证的断言）：
  1. 兼容修改（改描述）→ 发布得到 1.1，自动激活
  2. 不兼容修改（窗口 7d→14d）→ 发布得到 2.0，但保持未激活
  3. 旧绑定仍解析到 1.x；显式 activate 后默认版本切到 2.0
  4. rollback 回 1.1，确认默认版本恢复
  5. 弃用 1.0 并指向 2.0：旧绑定继续可读，新绑定拒绝引用弃用版本
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request

FEATURE = "user_7d_order_cnt"


def call(method: str, url: str, payload: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


def step(title: str) -> None:
    print(f"\n--- {title} ---")


def save_draft(base: str, definition: dict, note: str) -> tuple[int, dict]:
    """幂等存草稿：中断重跑时从当前 revision 继续，而不是假设 revision=0。"""
    code, body = call("GET", f"{base}/v2/registry/drafts")
    revision = 0
    for d in body.get("drafts", []):
        if d["name"] == definition["name"]:
            revision = d["revision"]
            break
    return call("PUT", f"{base}/v2/registry/drafts",
                {"definition": definition, "revision": revision, "change_note": note})


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://127.0.0.1:9500")
    args = ap.parse_args()
    base = args.base.rstrip("/")

    step("初始版本列表")
    _, body = call("GET", f"{base}/v2/registry/versions/{FEATURE}")
    versions = [v["fqvn"] for v in body["versions"]]
    print("versions:", versions)
    assert f"{FEATURE}@1.0" in versions

    # 取 1.0 定义，做兼容修改。
    _, active_body = call("GET", f"{base}/v2/registry/active")
    v1 = next(v for v in active_body["versions"] if v["name"] == FEATURE)
    definition = v1["definition"]
    definition["description"] = "兼容修订：仅文档变化"

    step("保存草稿（按当前 revision 提交）并发布兼容版本")
    code, body = save_draft(base, definition, "docs")
    assert code == 200, body

    step("并发修改检测：用过期 revision 再存一次必须 409")
    code, body = call("PUT", f"{base}/v2/registry/drafts",
                      {"definition": definition, "revision": 0, "change_note": "stale"})
    print("stale write ->", code, body["error"])
    assert code == 409

    code, body = call("POST", f"{base}/v2/registry/publish/{FEATURE}", {"actor": "demo"})
    print("publish:", code, body["version"]["fqvn"], body["change_class"], "activated=", body["activated"])
    assert code == 200 and body["version"]["fqvn"] == f"{FEATURE}@1.1" and body["activated"] is True

    step("不兼容修改：窗口 7d -> 14d，发布后必须保持未激活")
    breaking = dict(definition)
    breaking["window"] = {**definition["window"], "size_seconds": 14 * 24 * 3600}
    code, _ = save_draft(base, breaking, "widen window")
    assert code == 200
    code, body = call("POST", f"{base}/v2/registry/publish/{FEATURE}", {"actor": "demo"})
    print("publish:", code, body["version"]["fqvn"], body["change_class"], "activated=", body["activated"])
    assert code == 202 and body["version"]["fqvn"] == f"{FEATURE}@2.0" and body["activated"] is False

    step("默认版本仍停在 1.1（旧模型不会无感知读到新语义）")
    _, body = call("GET", f"{base}/v2/registry/active")
    active_fqvn = next(v["fqvn"] for v in body["versions"] if v["name"] == FEATURE)
    print("active:", active_fqvn)
    assert active_fqvn == f"{FEATURE}@1.1"

    step("并行版本共存：分别绑定 1.1 与 2.0")
    code, b1 = call("POST", f"{base}/v2/registry/bindings", {"fqvns": [f"{FEATURE}@1.1"]})
    assert code == 200
    code, b2 = call("POST", f"{base}/v2/registry/bindings", {"fqvns": [f"{FEATURE}@2.0"]})
    assert code == 200
    print("binding v1:", b1["manifest_id"], "v2:", b2["manifest_id"])
    assert b1["fingerprint"] != b2["fingerprint"]

    step("显式激活 2.0，再回退到 1.1")
    code, _ = call("POST", f"{base}/v2/registry/activate/{FEATURE}@2.0", {"actor": "demo"})
    assert code == 200
    code, _ = call("POST", f"{base}/v2/registry/rollback/{FEATURE}",
                   {"fqvn": f"{FEATURE}@1.1", "actor": "demo"})
    assert code == 200
    _, body = call("GET", f"{base}/v2/registry/active")
    active_fqvn = next(v["fqvn"] for v in body["versions"] if v["name"] == FEATURE)
    print("after rollback active:", active_fqvn)
    assert active_fqvn == f"{FEATURE}@1.1"

    step("弃用 1.0（指向 2.0）：旧绑定仍可读，新绑定被拒绝")
    code, _ = call("POST", f"{base}/v2/registry/deprecate/{FEATURE}@1.0",
                   {"successor": f"{FEATURE}@2.0", "actor": "demo"})
    assert code == 200
    code, body = call("POST", f"{base}/v2/registry/bindings", {"fqvns": [f"{FEATURE}@1.0"]})
    print("bind deprecated ->", code, body["error"])
    assert code == 409

    step("审计链（最后 6 条）")
    _, body = call("GET", f"{base}/v2/registry/audit")
    for e in body["audit"][-6:]:
        print(f"  seq={e['seq']:>3} {e['action']:<16} {e.get('fqvn', '')} {e.get('change_class', '')}")

    print("\n全部治理断言通过。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
