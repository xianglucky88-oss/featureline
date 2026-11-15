// Package store 是特征存储。
//
// 线上：Redis（key 形如 f:{uid}:{name}）
// 本地：内存 map（可选的 JSON 快照落盘）
//
// 线上服务按 7 天滑动窗口计算，窗口右端为当前时刻。
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// Value 是一个特征值。
type Value struct {
	Score     float64   `json:"score"`
	UpdatedAt time.Time `json:"updated_at"`
	// Source 记录是谁写的：offline / online。
	// 线上服务写的是 "online"，离线回填写 "offline"。
	Source string `json:"source"`
}

// Store 是特征存储接口。
type Store interface {
	Get(ctx context.Context, userID, name string) (Value, bool, error)
	Set(ctx context.Context, userID, name string, v Value) error
	MGet(ctx context.Context, userID string, names []string) (map[string]Value, error)
	Keys(ctx context.Context) ([]string, error)
	Close() error
}

// MemoryStore 是内存实现。本地验证与 CI 用它。
type MemoryStore struct {
	mu   sync.RWMutex
	data map[string]Value
	// snapshotPath 非空时，每次写入都会落盘，便于跨进程验证。
	snapshotPath string
}

// NewMemory 构造内存存储。snapshotPath 为空表示不落盘。
func NewMemory(snapshotPath string) *MemoryStore {
	s := &MemoryStore{data: map[string]Value{}, snapshotPath: snapshotPath}
	if snapshotPath != "" {
		s.load()
	}
	return s
}

func key(userID, name string) string { return fmt.Sprintf("f:%s:%s", userID, name) }

func (s *MemoryStore) load() {
	raw, err := os.ReadFile(s.snapshotPath)
	if err != nil {
		return
	}
	var data map[string]Value
	if err := json.Unmarshal(raw, &data); err == nil {
		s.data = data
	}
}

func (s *MemoryStore) persist() {
	if s.snapshotPath == "" {
		return
	}
	raw, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(s.snapshotPath, raw, 0o644)
}

func (s *MemoryStore) Get(ctx context.Context, userID, name string) (Value, bool, error) {
	// 先响应取消/超时，避免在依赖已经放弃后还继续占用资源。
	if err := ctx.Err(); err != nil {
		return Value{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key(userID, name)]
	return v, ok, nil
}

func (s *MemoryStore) Set(ctx context.Context, userID, name string, v Value) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key(userID, name)] = v
	s.persist()
	return nil
}

func (s *MemoryStore) MGet(ctx context.Context, userID string, names []string) (map[string]Value, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]Value, len(names))
	for _, n := range names {
		if v, ok := s.data[key(userID, n)]; ok {
			out[n] = v
		}
	}
	return out, nil
}

func (s *MemoryStore) Keys(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.data))
	for k := range s.data {
		out = append(out, k)
	}
	return out, nil
}

func (s *MemoryStore) Close() error { return nil }

// Stats 返回存储规模，供 /debug 使用。
func (s *MemoryStore) Stats() map[string]int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	online, offline := 0, 0
	for _, v := range s.data {
		if v.Source == "online" {
			online++
		} else {
			offline++
		}
	}
	return map[string]int{"keys": len(s.data), "online": online, "offline": offline}
}
