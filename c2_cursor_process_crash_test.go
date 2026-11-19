package core

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

const (
	c2CursorCrashHelperEnv = "CORE_C2_CURSOR_PROCESS_CRASH_HELPER"
	c2CursorCrashDirEnv    = "CORE_C2_CURSOR_PROCESS_CRASH_DIR"
	c2CursorCrashStageEnv  = "CORE_C2_CURSOR_PROCESS_CRASH_STAGE"
	c2CursorCrashReady     = "C2_CURSOR_CRASH_READY "
)

func c2CursorCrashFixture() (RawCursor, RawCursor) {
	old := RawCursor{
		Source: "source-a", Identity: "identity-a", JournalID: "journal-a",
		ConsumerEpoch: "epoch-a", Sequence: 1,
	}
	next := old
	next.Sequence = 2
	return old, next
}

// TestC2CursorProcessCrashHelper is selected only by the parent test process.
// It pauses inside the real atomic writer at one of its persistence boundaries;
// the parent then terminates this process without running deferred cleanup.
func TestC2CursorProcessCrashHelper(t *testing.T) {
	if os.Getenv(c2CursorCrashHelperEnv) != "1" {
		return
	}

	dir := os.Getenv(c2CursorCrashDirEnv)
	stage := os.Getenv(c2CursorCrashStageEnv)
	if dir == "" || stage == "" {
		t.Fatalf("missing crash helper configuration: dir=%q stage=%q", dir, stage)
	}
	_, next := c2CursorCrashFixture()
	path := filepath.Join(dir, "consumer.json")
	pause := func() {
		if _, err := fmt.Fprintln(os.Stdout, c2CursorCrashReady+stage); err != nil {
			t.Fatalf("announce cursor crash boundary: %v", err)
		}
		var input [1]byte
		_, _ = os.Stdin.Read(input[:]) // Parent keeps stdin open until Process.Kill.
		t.Fatalf("parent did not kill child at cursor boundary %q", stage)
	}

	var hooks cursorAtomicHooks
	switch stage {
	case "after-temp-fsync":
		hooks.syncTemp = func(f *os.File) error {
			if err := f.Sync(); err != nil {
				return err
			}
			pause()
			return nil
		}
	case "before-rename":
		hooks.rename = func(from, to string) error {
			pause()
			return os.Rename(from, to)
		}
	case "after-rename":
		hooks.rename = func(from, to string) error {
			if err := os.Rename(from, to); err != nil {
				return err
			}
			pause()
			return nil
		}
	case "after-dir-fsync":
		hooks.syncDir = func(dir string) error {
			if err := syncConsumerCursorDir(dir); err != nil {
				return err
			}
			pause()
			return nil
		}
	default:
		t.Fatalf("unknown cursor crash stage %q", stage)
	}

	if err := writeCursorAtomicWithHooks(path, next, hooks); err != nil {
		t.Fatalf("write cursor at %s: %v", stage, err)
	}
	t.Fatalf("cursor writer returned before parent killed child at %s", stage)
}

func c2CursorCrashKillAtBoundary(t *testing.T, dir, stage string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestC2CursorProcessCrashHelper$")
	cmd.Env = append(os.Environ(),
		c2CursorCrashHelperEnv+"=1",
		c2CursorCrashDirEnv+"="+dir,
		c2CursorCrashStageEnv+"="+stage,
	)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdin.Close()
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start cursor crash helper: %v", err)
	}

	scanner := bufio.NewScanner(stdout)
	ready := false
	var diagnostics strings.Builder
	for scanner.Scan() {
		line := scanner.Text()
		if line == c2CursorCrashReady+stage {
			ready = true
			break
		}
		diagnostics.WriteString(line + "\n")
	}
	if !ready {
		waitErr := cmd.Wait()
		t.Fatalf("child did not reach cursor boundary %s: wait=%v stdout=%s stderr=%s scanner=%v", stage, waitErr, diagnostics.String(), stderr.String(), scanner.Err())
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill cursor crash helper at %s: %v", stage, err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("cursor crash helper exited normally; expected Process.Kill")
	} else if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != -1 {
		t.Fatalf("cursor crash helper did not terminate by signal: %v", err)
	}
}

func TestC2CursorProcessCrashBoundaries(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Process.Kill signal semantics and cursor crash boundaries are tested on Unix")
	}

	for _, stage := range []string{"after-temp-fsync", "before-rename", "after-rename", "after-dir-fsync"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "consumer.json")
			old, next := c2CursorCrashFixture()
			if err := writeCursorAtomic(path, old); err != nil {
				t.Fatalf("write initial cursor: %v", err)
			}

			c2CursorCrashKillAtBoundary(t, dir, stage)

			content, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read cursor after child SIGKILL: %v", err)
			}
			var got RawCursor
			if err := json.Unmarshal(content, &got); err != nil {
				t.Fatalf("decode cursor after child SIGKILL: %v", err)
			}
			want := old
			wantTemps := 1
			if stage == "after-rename" || stage == "after-dir-fsync" {
				want = next
				wantTemps = 0
			}
			if got != want {
				t.Fatalf("cursor after SIGKILL at %s = %+v, want %+v", stage, got, want)
			}

			temps, err := filepath.Glob(filepath.Join(dir, ".cursor-*.tmp"))
			if err != nil {
				t.Fatalf("list cursor temporary files: %v", err)
			}
			if len(temps) != wantTemps {
				t.Fatalf("temporary cursor files after SIGKILL at %s = %v, want %d file(s)", stage, temps, wantTemps)
			}
			for _, temp := range temps {
				tempContent, err := os.ReadFile(temp)
				if err != nil {
					t.Fatalf("read orphan cursor temp %s: %v", temp, err)
				}
				var tempCursor RawCursor
				if err := json.Unmarshal(tempContent, &tempCursor); err != nil {
					t.Fatalf("decode orphan cursor temp %s: %v", temp, err)
				}
				if tempCursor != next {
					t.Fatalf("orphan cursor temp after %s = %+v, want %+v", stage, tempCursor, next)
				}
			}
		})
	}
}
