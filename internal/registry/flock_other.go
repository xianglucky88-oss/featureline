//go:build !(linux || darwin)

package registry

import "os"

// 非 Unix 平台退化为无跨进程锁；进程内仍由 RWMutex 保护。
func flockExclusive(_ *os.File) error { return nil }

func unlock(_ *os.File) error { return nil }
