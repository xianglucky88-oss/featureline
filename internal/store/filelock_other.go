//go:build !unix

package store

import "fmt"

// 非 unix 平台没有 flock 实现；此构建拒绝跨进程独占语义，而不是假装安全。
// CI 与部署目标均为 linux/darwin。
type fileLock struct{}

func acquireLock(path string) (*fileLock, error) {
	return nil, fmt.Errorf("store: durable directory lock is not supported on this platform (%s)", path)
}

func (l *fileLock) release() error { return nil }
