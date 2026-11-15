//go:build linux || darwin

package registry

import (
	"os"
	"syscall"
)

// flockExclusive 获取排他文件锁，保证同一时刻只有一个进程能写注册表。
// 拿不到锁立即失败（LOCK_EX | LOCK_NB），由调用方决定重试还是报错退出，
// 避免两个发布流程互相等待造成发布中断不可见。
func flockExclusive(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

func unlock(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
