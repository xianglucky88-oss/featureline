package store

import (
	"context"
	"errors"
	"testing"
)

func TestMemoryStoreRespectsCanceledContext(t *testing.T) {
	s := NewMemory("")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := s.MGet(ctx, "U1", []string{"a"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("MGet err = %v, want context.Canceled", err)
	}
	if err := s.Set(ctx, "U1", "a", Value{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Set err = %v, want context.Canceled", err)
	}
	if _, _, err := s.Get(ctx, "U1", "a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Get err = %v, want context.Canceled", err)
	}
	if _, err := s.Keys(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Keys err = %v, want context.Canceled", err)
	}
}

func TestMemoryStoreHappyPathStillWorks(t *testing.T) {
	s := NewMemory("")
	ctx := context.Background()
	if err := s.Set(ctx, "U1", "f", Value{Score: 3.5, Source: "online"}); err != nil {
		t.Fatalf("set: %v", err)
	}
	v, ok, err := s.Get(ctx, "U1", "f")
	if err != nil || !ok || v.Score != 3.5 {
		t.Fatalf("get = %+v ok=%v err=%v", v, ok, err)
	}
	all, err := s.MGet(ctx, "U1", []string{"f", "missing"})
	if err != nil || len(all) != 1 {
		t.Fatalf("mget = %v err=%v", all, err)
	}
	keys, err := s.Keys(ctx)
	if err != nil || len(keys) != 1 {
		t.Fatalf("keys = %v err=%v", keys, err)
	}
}
