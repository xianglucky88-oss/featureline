// Package store 是特征存储。
//
// 线上：Redis（key 形如 f:{uid}:{name}）
// 本地：DurableStore（追加日志 + 原子快照），见 durable.go
//
// 线上服务按 7 天滑动窗口计算，窗口右端为当前时刻。
package store

import (
	"context"
	"sync"
)

// MemoryStore 是纯内存实现，仅用于测试与本地验证。
//
// 持久化不再由它承担：历史上的“每次写入整体覆盖 JSON 快照”已移除，
// 需要落盘的调用方应使用 DurableStore。这样只有一种磁盘语义，
// 不会出现两套实现各自定义原子性与恢复行为。
type MemoryStore struct {
	mu   sync.RWMutex
	data map[string]Value
}

// NewMemory 构造一个空的内存存储。
func NewMemory() *MemoryStore {
	return &MemoryStore{data: map[string]Value{}}
}

func (s *MemoryStore) Get(_ context.Context, userID, name string) (Value, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key(userID, name)]
	return v, ok, nil
}

func (s *MemoryStore) Set(_ context.Context, userID, name string, v Value) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key(userID, name)] = v
	return nil
}

// SetBatch 在一次写锁内提交整批，保证读者看不到中间状态。
func (s *MemoryStore) SetBatch(_ context.Context, entries []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range entries {
		s.data[key(e.UserID, e.Name)] = e.Value
	}
	return nil
}

func (s *MemoryStore) MGet(_ context.Context, userID string, names []string) (map[string]Value, error) {
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

func (s *MemoryStore) Keys(_ context.Context) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.data))
	for k := range s.data {
		out = append(out, k)
	}
	return out, nil
}

func (s *MemoryStore) Close() error { return nil }

// Stats 返回存储规模，供测试使用。
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

var _ Store = (*MemoryStore)(nil)
var _ BatchWriter = (*MemoryStore)(nil)
