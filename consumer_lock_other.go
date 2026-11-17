//go:build !darwin && !linux && !freebsd && !netbsd && !openbsd && !dragonfly

package core

import (
	"errors"
	"os"
)

func acquireConsumerCursorFileLock(*os.File) error {
	return errors.New("core: durable consumer cursor locking is unsupported on this platform")
}
