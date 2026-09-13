//go:build linux || darwin

package store

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// Lock 的 FD 可交给子进程继承；只通过 Close 释放，不调用 LOCK_UN。
type Lock struct{ File *os.File }

func Acquire(path string) (*Lock, error) {
	if err := EnsureDir(filepath.Dir(path)); err != nil {
		return nil, err
	}
	fd, err := syscall.Open(path, syscall.O_CREAT|syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	if err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("执行锁被占用 %s: %w", path, err)
	}
	return &Lock{File: f}, nil
}

func (l *Lock) Close() error { return l.File.Close() }
