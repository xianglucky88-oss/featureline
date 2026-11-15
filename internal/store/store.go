package store

import (
	"context"
	"errors"
	"fmt"
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
//
// 这是调用方唯一可见的边界：服务、回填、测试都不应感知底层是内存、
// 追加日志还是外部介质。新增存储实现时不要扩展这个接口的方法；
// 需要批量写之类的能力请由具体实现额外暴露。
type Store interface {
	Get(ctx context.Context, userID, name string) (Value, bool, error)
	Set(ctx context.Context, userID, name string, v Value) error
	MGet(ctx context.Context, userID string, names []string) (map[string]Value, error)
	Keys(ctx context.Context) ([]string, error)
	Close() error
}

// Entry 是一次批量写入中的一项。DurableStore 用它在单条日志记录里
// 原子地提交多个特征值。
type Entry struct {
	UserID string
	Name   string
	Value  Value
}

// BatchWriter 是 Store 的可选能力：一次提交多条写入，整批要么全可见、
// 要么全不可见。调用方通过类型断言使用它，从而在不扩展 Store 接口、
// 不感知具体介质的前提下利用批量语义。MemoryStore 也实现了它。
type BatchWriter interface {
	SetBatch(ctx context.Context, entries []Entry) error
}

// ErrClosed 表示存储已经关闭。
var ErrClosed = errors.New("store: closed")

func key(userID, name string) string { return fmt.Sprintf("f:%s:%s", userID, name) }
