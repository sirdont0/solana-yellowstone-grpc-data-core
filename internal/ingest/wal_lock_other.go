//go:build !darwin && !linux && !freebsd && !netbsd && !openbsd && !dragonfly

package ingest

import (
	"errors"
	"os"
)

var errWALFileLockUnsupported = errors.New("WAL locking is unsupported on this platform")

func lockWALWriter(*os.File) error { return errWALFileLockUnsupported }
func lockWALReader(*os.File) error { return errWALFileLockUnsupported }
