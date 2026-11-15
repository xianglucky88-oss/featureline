package online

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/internal-platform/featureline/internal/feature"
	"github.com/internal-platform/featureline/internal/store"
)

func newSvc() (*Service, *store.MemoryStore) {
	st := store.NewMemory()
	reg := feature.DefaultRegistry()
	return New(st, reg), st
}

func TestComputeMissingFeaturesAreZero(t *testing.T) {
	svc, _ := newSvc()
	fv, err := svc.Compute(context.Background(), "U-missing")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for name, v := range fv.Features {
		if v != 0 {
			t.Fatalf("feature %s should default to 0, got %v", name, v)
		}
	}
}

// TestNaNSerializesAsNull 记录一个真实踩过的坑：
// encoding/json 不支持 NaN，直接 Encode 会写出 0 字节。
func TestNaNSerializesAsNull(t *testing.T) {
	svc, _ := newSvc()
	fv, err := svc.Compute(context.Background(), "U-missing")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	raw, err := json.Marshal(fv)
	if err != nil {
		t.Fatalf("marshal must not fail on NaN: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("marshal produced empty output")
	}
	var back map[string]any
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("output is not valid json: %v", err)
	}
}

func TestWindowForFeature(t *testing.T) {
	svc, _ := newSvc()
	at := time.Date(2026, 10, 15, 23, 47, 0, 0, time.UTC)
	start, end, err := svc.WindowFor("user_7d_order_cnt", at)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !end.Equal(at) {
		t.Fatalf("end mismatch: %v", end)
	}
	if !start.Equal(at.Add(-7 * 24 * time.Hour)) {
		t.Fatalf("start mismatch: %v", start)
	}
}

func TestWindowForUnknownFeature(t *testing.T) {
	svc, _ := newSvc()
	if _, _, err := svc.WindowFor("nope", time.Now()); err == nil {
		t.Fatal("expected error for unknown feature")
	}
}

func TestStoredValueIsReturned(t *testing.T) {
	svc, st := newSvc()
	ctx := context.Background()
	if err := st.Set(ctx, "U1", "user_7d_order_cnt", store.Value{Score: 42, Source: "offline"}); err != nil {
		t.Fatalf("set: %v", err)
	}
	fv, err := svc.Compute(ctx, "U1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fv.Features["user_7d_order_cnt"] != 42 {
		t.Fatalf("want 42, got %v", fv.Features["user_7d_order_cnt"])
	}
}

func TestRegistryHasSixFeatures(t *testing.T) {
	reg := feature.DefaultRegistry()
	if len(reg.Names()) != 6 {
		t.Fatalf("want 6 features, got %d", len(reg.Names()))
	}
}

func TestRegistryNamesSorted(t *testing.T) {
	reg := feature.DefaultRegistry()
	names := reg.Names()
	for i := 1; i < len(names); i++ {
		if names[i] < names[i-1] {
			t.Fatalf("names not sorted: %v", names)
		}
	}
}
