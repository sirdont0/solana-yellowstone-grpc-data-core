package ingest

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// This helper intentionally never closes its writer. The parent kills the
// process at a known persistence boundary, releasing the actual OS file lock.
func TestWALV2CrashProcessHelper(t *testing.T) {
	if os.Getenv("CORE_WAL_CRASH_HELPER") != "1" {
		return
	}
	dir, stage := os.Getenv("CORE_WAL_CRASH_DIR"), os.Getenv("CORE_WAL_CRASH_STAGE")
	w, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	size := 128
	if strings.HasPrefix(stage, "rotation-") {
		size = 1 << 20
	}
	if err := w.Append(walV2FixtureRecord(size, "first")); err != nil {
		t.Fatal(err)
	}
	pause := func() {
		fmt.Println("CRASH_READY " + w.journalID)
		var b [1]byte
		_, _ = os.Stdin.Read(b[:]) // Parent keeps pipe open until SIGKILL.
		t.Fatal("parent did not kill the writer")
	}
	w.v2.manifestWriter = func(dir string, manifest walV2Manifest) error {
		if strings.HasSuffix(stage, "before-current") {
			pause()
		}
		if err := writeWALV2Manifest(dir, manifest); err != nil {
			return err
		}
		if strings.HasSuffix(stage, "after-current") {
			pause()
		}
		return nil
	}
	if err := w.Append(walV2FixtureRecord(size, "second")); err != nil {
		t.Fatal(err)
	}
	pause() // Successful Append returned, with no graceful Close.
}

func TestWALV2ProcessKillRecovery(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("WAL writer locking is not implemented on Windows")
	}
	for _, stage := range []string{
		"append-before-current", "append-after-current", "append-returned",
		"rotation-before-current", "rotation-after-current", "rotation-returned",
	} {
		t.Run(stage, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "journal.v2")
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestWALV2CrashProcessHelper$")
			cmd.Env = append(os.Environ(), "CORE_WAL_CRASH_HELPER=1", "CORE_WAL_CRASH_DIR="+dir, "CORE_WAL_CRASH_STAGE="+stage)
			input, err := cmd.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			defer input.Close()
			output, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			var diagnostics, stderr strings.Builder
			cmd.Stderr = &stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer cmd.Process.Kill()
			scanner := bufio.NewScanner(output)
			var journalID string
			for scanner.Scan() {
				line := scanner.Text()
				if strings.HasPrefix(line, "CRASH_READY ") {
					journalID = strings.TrimPrefix(line, "CRASH_READY ")
					break
				}
				diagnostics.WriteString(line + "\n")
			}
			if journalID == "" {
				_ = cmd.Wait()
				t.Fatalf("child did not reach %s: %s %s (read=%v)", stage, diagnostics.String(), stderr.String(), scanner.Err())
			}
			if err := cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			if err := cmd.Wait(); err == nil {
				t.Fatal("child exited successfully instead of being killed")
			}
			want := uint64(1)
			if stage == "append-after-current" || strings.HasSuffix(stage, "returned") {
				want = 2
			}
			var recovered []Record
			w, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, func(r Record) error {
				recovered = append(recovered, r)
				return nil
			})
			if err != nil {
				t.Fatalf("recovery after kill at %s: %v", stage, err)
			}
			defer w.Close()
			if w.journalID != journalID || w.Count() != want || uint64(len(recovered)) != want {
				t.Fatalf("identity/count changed: journal=%s want=%s count=%d replay=%d want=%d", w.journalID, journalID, w.Count(), len(recovered), want)
			}
			for i, record := range recovered {
				if record.Sequence != uint64(i+1) || record.Detail != []string{"first", "second"}[i] {
					t.Fatalf("prefix changed at %d: sequence=%d detail=%q", i, record.Sequence, record.Detail)
				}
			}
			if err := w.Append(walV2FixtureRecord(128, "after-kill")); err != nil {
				t.Fatalf("append after kill: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			w2, err := OpenWALV2(dir, "source-a", "identity-a", 2<<20, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer w2.Close()
			page, err := w2.ReadAfter(want, 1)
			if err != nil || len(page) != 1 || page[0].Detail != "after-kill" || page[0].Sequence != want+1 {
				t.Fatalf("recovered writer did not persist new record: records=%v err=%v", page, err)
			}
		})
	}
}
