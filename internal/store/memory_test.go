package store

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestVersionedKeyIsolatesParallelMajors(t *testing.T) {
	st := NewMemory("")
	ctx := context.Background()
	v1 := Value{Score: 7, Source: "offline", FQVN: "f_x@1.0"}
	v2 := Value{Score: 14, Source: "offline", FQVN: "f_x@2.0"}
	if err := st.Set(ctx, "U1", "f_x@1.0", v1); err != nil {
		t.Fatal(err)
	}
	if err := st.Set(ctx, "U1", "f_x@2.0", v2); err != nil {
		t.Fatal(err)
	}
	got, ok, err := st.Get(ctx, "U1", "f_x@1.0")
	if err != nil || !ok || got.Score != 7 {
		t.Fatalf("v1 read wrong: %v ok=%v err=%v", got, ok, err)
	}
	got, ok, _ = st.Get(ctx, "U1", "f_x@2.0")
	if !ok || got.Score != 14 {
		t.Fatalf("v2 read wrong: %v ok=%v", got, ok)
	}
	mget, err := st.MGet(ctx, "U1", []string{"f_x@1.0", "f_x@2.0"})
	if err != nil || len(mget) != 2 {
		t.Fatalf("mget wrong: %v err=%v", mget, err)
	}
}

func TestSnapshotRoundTripWithManifest(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")
	asOf := time.Date(2026, 10, 15, 23, 47, 0, 0, time.UTC)
	manifest := &SnapshotManifest{
		ManifestID: "bm-1", Fingerprint: "fp-1", AsOf: &asOf,
		Entries: []ManifestEntry{{Name: "f_x", FQVN: "f_x@1.0", Fingerprint: "def-fp"}},
	}
	st := NewMemory(path)
	st.SetManifest(manifest)
	if err := st.Set(context.Background(), "U1", "f_x@1.0",
		Value{Score: 9, Source: "offline", FQVN: "f_x@1.0"}); err != nil {
		t.Fatal(err)
	}

	// 重新加载：清单与数据都应恢复，as_of 时间基准可用。
	reloaded := NewMemory(path)
	m := reloaded.Manifest()
	if m == nil || m.ManifestID != "bm-1" || m.Fingerprint != "fp-1" {
		t.Fatalf("manifest lost: %+v", m)
	}
	if m.AsOf == nil || !m.AsOf.Equal(asOf) {
		t.Fatalf("as_of lost: %+v", m.AsOf)
	}
	got, ok, _ := reloaded.Get(context.Background(), "U1", "f_x@1.0")
	if !ok || got.Score != 9 {
		t.Fatalf("data lost: %+v ok=%v", got, ok)
	}
}

func TestLegacySnapshotStillLoads(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "legacy.json")
	// 旧格式：裸 map，键为 f:{uid}:{name}，没有 manifest。
	legacy := map[string]Value{
		"f:U1:f_x": {Score: 3, Source: "offline"},
	}
	raw, _ := json.Marshal(legacy)
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	st := NewMemory(path)
	if st.Manifest() != nil {
		t.Fatal("legacy snapshot must not fabricate a manifest")
	}
	keys, err := st.Keys(context.Background())
	if err != nil || len(keys) != 1 {
		t.Fatalf("legacy data not loaded: %v err=%v", keys, err)
	}
}

func TestSetPropagatesFQVN(t *testing.T) {
	st := NewMemory("")
	if err := st.Set(context.Background(), "U9", "f_y@1.0", Value{Score: 1, Source: "online"}); err != nil {
		t.Fatal(err)
	}
	got, ok, _ := st.Get(context.Background(), "U9", "f_y@1.0")
	if !ok || got.FQVN != "f_y@1.0" {
		t.Fatalf("fqvn not stamped: %+v", got)
	}
}
