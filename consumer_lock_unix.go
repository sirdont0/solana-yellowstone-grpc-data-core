//go:build darwin || linux || freebsd || netbsd || openbsd || dragonfly

package core

import (
	"os"
	"syscall"
)

func acquireConsumerCursorFileLock(f *os.File) error {
	for {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX)
		if err == syscall.EINTR {
			continue
		}
		return err
	}
}
