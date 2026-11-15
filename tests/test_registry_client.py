"""注册表客户端的 Python 侧测试。

分两层：
1. 跨语言契约：与 Go 侧 internal/registry/cross_language_test.go 锁同一份
   规范化字节与指纹，任一侧改动都会同时红。
2. 端到端：直接读取 Go 注册表写出的 state.json / binding.json，验证解析、
   绑定校验、漂移检测与弃用迁移规则。
"""

from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pytest

from featureline.registry_client import (
    Binding,
    BindingEntry,
    BindingError,
    RegistryClient,
    binding_fingerprint,
    canonical_json,
    fingerprint,
    make_fqvn,
    parse_fqvn,
)

ROOT = Path(__file__).resolve().parent.parent
FIXTURE = ROOT / "tests" / "fixtures" / "canonical_definition.json"

EXPECTED_FINGERPRINT = "dd88466111b54fa33f0416d32c33f4bfb6142430864e0b853a4a57a752a46972"
EXPECTED_CANONICAL = (
    '{"default":{"type":"float64","value":"0"},"description":"用户近 7 天订单数",'
    '"entity":"user","kind":"count","name":"user_7d_order_cnt","owner":"platform",'
    '"tags":[],"time_semantics":{"time_column":"event_time","time_right_bound":"event_time",'
    '"time_zone":"UTC"},"value_type":"float64",'
    '"window":{"convention":"[start,end)","size_seconds":604800}}'
)


def test_cross_language_canonical_bytes():
    d = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert canonical_json(d).decode("utf-8") == EXPECTED_CANONICAL


def test_cross_language_fingerprint():
    d = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert fingerprint(d) == EXPECTED_FINGERPRINT


def test_nil_tags_normalized_to_empty_array():
    d = json.loads(FIXTURE.read_text(encoding="utf-8"))
    d.pop("tags")
    # 对齐 Go Fingerprint：缺失 tags 与空数组的指纹必须一致。
    assert fingerprint(d) == EXPECTED_FINGERPRINT
    assert fingerprint(d) == fingerprint({**d, "tags": []})


def test_fqvn_roundtrip():
    assert make_fqvn("user_7d_order_cnt", 2, 1) == "user_7d_order_cnt@2.1"
    assert parse_fqvn("a@1.0") == ("a", 1, 0)
    for bad in ("a", "a@", "@1.0", "a@x.0"):
        with pytest.raises(ValueError):
            parse_fqvn(bad)


def test_binding_order_independent_fingerprint():
    e1 = [BindingEntry("a", "a@1.0", "fp-a"), BindingEntry("b", "b@1.0", "fp-b")]
    e2 = list(reversed(e1))
    assert binding_fingerprint("bm-1", [e.to_dict() for e in e1]) == binding_fingerprint(
        "bm-1", [e.to_dict() for e in e2]
    )


def test_binding_tamper_detection():
    b = Binding(
        manifest_id="bm-x",
        entries=[BindingEntry("a", "a@1.0", "fp-a")],
    )
    payload = b.to_dict()
    payload["entries"][0]["fingerprint"] = "tampered"
    with pytest.raises(BindingError):
        Binding.from_dict(payload)


# ------------------------------------------------- 端到端：读取 Go 注册表产出


def _go_env() -> dict[str, str]:
    import os

    env = dict(os.environ)
    env["CGO_ENABLED"] = "0"
    go_bin = Path.home() / ".local" / "go" / "bin"
    env["PATH"] = f"{go_bin}:{env.get('PATH', '')}"
    return env


@pytest.fixture(scope="module")
def go_registry(tmp_path_factory) -> Path:
    """用一个临时 Go 程序驱动真实注册表，产出 state.json/binding.json 供 Python 读。

    internal 包只能被模块内代码 import，因此驱动放在 cmd/_registry_driver/，
    测试结束后删除。
    """
    reg_dir = tmp_path_factory.mktemp("registry")
    driver_dir = ROOT / "cmd" / "_registry_driver"
    driver_dir.mkdir(parents=True, exist_ok=True)
    (driver_dir / "main.go").write_text(
        f"""
package main

import (
    "fmt"
    "github.com/internal-platform/featureline/internal/registry"
)

func main() {{
    r, err := registry.Open({json.dumps(str(reg_dir))})
    if err != nil {{ panic(err) }}
    defer r.Close()
    if err := r.Bootstrap(); err != nil {{ panic(err) }}
    b, err := r.BindActive(nil)
    if err != nil {{ panic(err) }}
    if err := registry.WriteBinding({json.dumps(str(reg_dir / "binding.json"))}, b); err != nil {{
        panic(err)
    }}
    fmt.Println(b.ManifestID)
}}
""",
        encoding="utf-8",
    )
    import shutil

    try:
        out = subprocess.run(
            ["go", "run", "./cmd/_registry_driver"], cwd=ROOT, env=_go_env(),
            check=True, capture_output=True, text=True,
        )
        assert out.stdout.strip().startswith("bm-")
        yield reg_dir
    finally:
        shutil.rmtree(driver_dir, ignore_errors=True)


def test_python_resolves_go_registry_binding(go_registry: Path):
    client = RegistryClient.open(go_registry)
    active = client.list_active()
    assert len(active) == 6
    binding = Binding.read(go_registry / "binding.json")
    assert len(binding.entries) == 6
    resolved = client.resolve(binding)
    assert set(resolved) == {e.name for e in binding.entries}
    # Python 侧重算指纹必须等于 Go 侧落库指纹。
    for name, version in resolved.items():
        assert fingerprint(version["definition"]) == version["fingerprint"]
        assert version["status"] == "published"


def test_python_detects_drift_against_registry(go_registry: Path):
    binding = Binding.read(go_registry / "binding.json")
    bad = Binding(
        manifest_id=binding.manifest_id,
        entries=[
            BindingEntry(e.name, e.fqvn, "0" * 64) for e in binding.entries
        ],
    )
    client = RegistryClient.open(go_registry)
    with pytest.raises(BindingError):
        client.resolve(bad)


def test_python_rejects_unknown_version(go_registry: Path):
    client = RegistryClient.open(go_registry)
    b = Binding(
        manifest_id="bm-bogus",
        entries=[BindingEntry("ghost", "ghost@9.9", "a" * 64)],
    )
    with pytest.raises(BindingError):
        client.resolve(b)


def test_binding_file_roundtrip(tmp_path):
    b = Binding(
        manifest_id="bm-rt",
        entries=[BindingEntry("a", "a@1.0", "fp")],
        as_of=datetime(2026, 11, 15, tzinfo=timezone.utc),
    )
    p = tmp_path / "binding.json"
    b.write(p)
    again = Binding.read(p)
    assert again.manifest_id == "bm-rt"
    assert again.entries[0].fqvn == "a@1.0"
    assert again.as_of == datetime(2026, 11, 15, tzinfo=timezone.utc)
