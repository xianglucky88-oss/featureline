// Package store 是特征存储。
//
// 线上：Redis（versioned key 形如 f:{uid}:{fqvn}）
// 本地：内存 map（可选的 JSON 快照落盘）
//
// 键里带 FQVN（name@major.minor）是版本治理落到存储的关键：
// 同一特征的两个 major 并存为两组键，旧模型按绑定读旧键，新模型读新键，
// 定义发生不兼容变化时不可能在同一个键上读到另一套语义的数值。
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
	Source string `json:"source"`
	// FQVN 是该值对应的定义版本；旧格式快照没有该字段时按 legacy 处理。
	FQVN string `json:"fqvn,omitempty"`
}

// Store 是特征存储接口。
type Store interface {
	Get(ctx context.Context, userID, fqvn string) (Value, bool, error)
	Set(ctx context.Context, userID, fqvn string, v Value) error
	MGet(ctx context.Context, userID string, fqvns []string) (map[string]Value, error)
	Keys(ctx context.Context) ([]string, error)
	Close() error
}

// SnapshotManifest 是快照头部的绑定清单。
// 快照里每一个值都必须能在 manifest.entries 里找到对应 FQVN，
// 加载快照的在线实例据此校验“这份数据是按哪一版定义算出来的”。
type SnapshotManifest struct {
	ManifestID  string          `json:"manifest_id"`
	Fingerprint string          `json:"fingerprint"`
	AsOf        *time.Time      `json:"as_of,omitempty"`
	Entries     []ManifestEntry `json:"entries"`
}

// ManifestEntry 是清单中的一条版本坐标。
type ManifestEntry struct {
	Name        string `json:"name"`
	FQVN        string `json:"fqvn"`
	Fingerprint string `json:"fingerprint"`
}

// snapshotFile 是落盘格式：manifest + data，data 的键为 f:{uid}:{fqvn}。
type snapshotFile struct {
	Manifest *SnapshotManifest `json:"manifest,omitempty"`
	Data     map[string]Value  `json:"data"`
}

// MemoryStore 是内存实现。本地验证与 CI 用它。
type MemoryStore struct {
	mu   sync.RWMutex
	data map[string]Value
	// snapshotPath 非空时，每次写入都会落盘，便于跨进程验证。
	snapshotPath string
	// manifest 是当前快照携带的绑定清单，写入新版快照时覆盖。
	manifest *SnapshotManifest
}

// NewMemory 构造内存存储。snapshotPath 为空表示不落盘。
func NewMemory(snapshotPath string) *MemoryStore {
	s := &MemoryStore{data: map[string]Value{}}
	if snapshotPath != "" {
		s.snapshotPath = snapshotPath
		s.load()
	}
	return s
}

// SetManifest 绑定快照清单（通常在加载离线快照后由 main 注入）。
func (s *MemoryStore) SetManifest(m *SnapshotManifest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.manifest = m
}

// Manifest 返回当前清单。
func (s *MemoryStore) Manifest() *SnapshotManifest {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.manifest
}

// SnapshotAsOf 返回快照清单声明的事件时间基准；
// 没有清单（旧格式或在线直写）时返回 nil，调用方回退到墙钟。
func (s *MemoryStore) SnapshotAsOf() *time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.manifest == nil {
		return nil
	}
	return s.manifest.AsOf
}

// VersionedKey 导出键规则，供离线侧（Python）保持同一拼写。
func VersionedKey(userID, fqvn string) string {
	return fmt.Sprintf("f:%s:%s", userID, fqvn)
}

func (s *MemoryStore) load() {
	raw, err := os.ReadFile(s.snapshotPath)
	if err != nil {
		return
	}
	// 新格式：带 manifest 的包裹结构。
	var wrap snapshotFile
	if err := json.Unmarshal(raw, &wrap); err == nil && wrap.Data != nil {
		s.data = wrap.Data
		s.manifest = wrap.Manifest
		return
	}
	// 兼容旧格式：裸 map[string]Value，键为 f:{uid}:{name}。
	// 不做隐式升级——这些值没有版本坐标，只能由调用方显式标注 legacy。
	var legacy map[string]Value
	if err := json.Unmarshal(raw, &legacy); err == nil {
		s.data = legacy
	}
}

func (s *MemoryStore) persist() {
	if s.snapshotPath == "" {
		return
	}
	wrap := snapshotFile{Manifest: s.manifest, Data: s.data}
	raw, err := json.MarshalIndent(&wrap, "", "  ")
	if err != nil {
		return
	}
	tmp := s.snapshotPath + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return
	}
	_ = os.Rename(tmp, s.snapshotPath)
}

func (s *MemoryStore) Get(_ context.Context, userID, fqvn string) (Value, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[VersionedKey(userID, fqvn)]
	return v, ok, nil
}

func (s *MemoryStore) Set(_ context.Context, userID, fqvn string, v Value) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v.FQVN == "" {
		v.FQVN = fqvn
	}
	s.data[VersionedKey(userID, fqvn)] = v
	s.persist()
	return nil
}

func (s *MemoryStore) MGet(_ context.Context, userID string, fqvns []string) (map[string]Value, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]Value, len(fqvns))
	for _, fqvn := range fqvns {
		if v, ok := s.data[VersionedKey(userID, fqvn)]; ok {
			out[fqvn] = v
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
