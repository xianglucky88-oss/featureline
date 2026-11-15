"""版本化离线交付的端到端测试。

覆盖：
  - 播种注册表 → 绑定 → compute_offline_bound 按绑定定义计算
  - export_snapshot 产出带清单的版本化键，且与绑定条目一一对应
  - 旧格式（无绑定）快照仍可导出，明确标注无 manifest
  - 绑定里不包含的特征不会进入训练数据
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from featureline.bootstrap import bootstrap_registry, seed_definitions
from featureline.offline import (
    ITEM_FEATURES,
    USER_FEATURES,
    compute_offline_bound,
    export_snapshot,
    generate_events,
)
from featureline.registry_client import (
    Binding,
    BindingEntry,
    RegistryClient,
    fingerprint,
    make_fqvn,
)


@pytest.fixture(scope="module")
def events():
    return generate_events(n_users=30, n_items=10, n_events=3000, seed=7)


@pytest.fixture()
def bound_env(tmp_path):
    reg_dir = tmp_path / "registry"
    binding_path = tmp_path / "binding.json"
    binding = bootstrap_registry(reg_dir, binding_path)
    client = RegistryClient.open(reg_dir)
    return client, binding, tmp_path


def test_bootstrap_fingerprints_match_definitions(bound_env):
    client, _, _ = bound_env
    state = client.state()
    for d in seed_definitions():
        fqvn = make_fqvn(d["name"], 1, 0)
        version = state["versions"][fqvn]
        assert version["status"] == "published"
        assert version["fingerprint"] == fingerprint(d)


def test_compute_offline_bound_returns_only_bound_features(events, bound_env):
    client, binding, _ = bound_env
    # 去掉一个用户特征，验证“未绑定不进训练数据”。
    kept = [e for e in binding.entries if e.name != "user_30d_order_cnt"]
    partial = Binding(manifest_id="bm-partial", entries=kept)
    user_df, item_df = compute_offline_bound(events, partial, client)
    assert "user_30d_order_cnt" not in user_df.columns
    assert "user_7d_order_cnt" in user_df.columns
    assert set(item_df.columns) == {"item_id", *ITEM_FEATURES}


def test_compute_offline_bound_rejects_bad_convention(events, bound_env):
    client, binding, _ = bound_env
    # 手改 state 里的窗口约定，解析时必须被发现。
    state_path = Path(client.root) / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    fqvn = make_fqvn("user_7d_order_cnt", 1, 0)
    state["versions"][fqvn]["definition"]["window"]["convention"] = "(start,end]"
    state_path.write_text(json.dumps(state), encoding="utf-8")
    with pytest.raises(ValueError, match="window convention"):
        compute_offline_bound(events, binding, client)


def test_versioned_snapshot_keys_and_manifest(events, bound_env):
    client, binding, tmp_path = bound_env
    user_df, item_df = compute_offline_bound(events, binding, client)
    snap_path = tmp_path / "snap.json"
    info = export_snapshot(user_df, item_df, snap_path, binding=binding, client=client)

    assert info["manifest_id"] == binding.manifest_id
    assert info["binding_fingerprint"] == binding.fingerprint

    payload = json.loads(snap_path.read_text(encoding="utf-8"))
    manifest = payload["manifest"]
    assert manifest["manifest_id"] == binding.manifest_id
    assert {e["name"] for e in manifest["entries"]} == {
        e.name for e in binding.entries
    }
    # 每个键都带 FQVN，且 FQVN 必须在清单里声明。
    declared = {e["fqvn"] for e in manifest["entries"]}
    for key, value in payload["data"].items():
        parts = key.split(":")
        assert len(parts) == 3
        fqvn = parts[2]
        assert fqvn in declared
        assert value["fqvn"] == fqvn
        assert value["source"] == "offline"


def test_legacy_export_without_binding(events, tmp_path):
    from featureline.offline import compute_offline

    user_df, item_df = compute_offline(events, inclusive_right=False)
    snap_path = tmp_path / "legacy.json"
    info = export_snapshot(user_df, item_df, snap_path)
    assert info["manifest_id"] is None
    payload = json.loads(snap_path.read_text(encoding="utf-8"))
    # 旧格式是裸 map。
    sample_key = next(iter(payload))
    assert "fqvn" not in payload[sample_key]


def test_binding_with_unknown_fqvn_fails_resolution(bound_env):
    client, _, _ = bound_env
    bogus = Binding(
        manifest_id="bm-bogus",
        entries=[BindingEntry("ghost", "ghost@9.9", "a" * 64)],
    )
    with pytest.raises(Exception):
        client.resolve(bogus)
