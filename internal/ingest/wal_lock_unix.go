//go:build darwin || linux || freebsd || netbsd || openbsd || dragonfly

package ingest

import (
	"os"
	"syscall"
)

func lockWALWriter(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

func lockWALReader(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_SH|syscall.LOCK_NB)
}
