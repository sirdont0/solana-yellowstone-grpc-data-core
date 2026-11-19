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

const (
	c2WALV1CrashHelperEnv = "CORE_C2_WAL_V1_CRASH_HELPER"
	c2WALV1CrashDirEnv    = "CORE_C2_WAL_V1_CRASH_DIR"
	c2WALV1CrashStageEnv  = "CORE_C2_WAL_V1_CRASH_STAGE"
)

func c2WALV1CrashConfig(dir string) Config {
	return Config{
		SourceID: "aa", Endpoint: "127.0.0.1:1111", TokenHeader: "x-token",
		Commitment: "finalized", FilterVersion: "c2-wal-v1-process-crash",
		Programs: []string{PumpProgram}, DataDir: dir, DedupeLimit: 100, Streams: 1,
	}
}

// The helper is a separate process so the parent can SIGKILL the writer while
// its WAL lock and file descriptor are still held. It deliberately skips all
// graceful Store/WAL cleanup.
func TestC2WALV1CrashProcessHelper(t *testing.T) {
	if os.Getenv(c2WALV1CrashHelperEnv) != "1" {
		return
	}
	stage := os.Getenv(c2WALV1CrashStageEnv)
	store, err := OpenStore(c2WALV1CrashConfig(os.Getenv(c2WALV1CrashDirEnv)))
	if err != nil {
		t.Fatal(err)
	}
	store.SetDurableObserver(func(record Record) error {
		fmt.Printf("C2_WAL_V1_CALLBACK %s\n", record.DedupeKey)
		return nil
	})
	candidate := Record{
		Type: "update", Kind: "transaction", Source: "aa", Identity: store.cfg.Identity(),
		Generation: 2, Slot: 900, DedupeKey: "c2-wal-v1-crash-candidate", Digest: "candidate-digest",
	}
	pause := func() {
		fmt.Println("C2_WAL_V1_READY " + stage)
		var one [1]byte
		_, _ = os.Stdin.Read(one[:])
		t.Fatal("parent did not kill the WAL v1 crash helper")
	}

	switch stage {
	case "partial-write":
		store.wal.writeFrame = func(frame []byte) (int, error) {
			partial := len(frame) / 2
			n, writeErr := store.wal.f.Write(frame[:partial])
			if writeErr != nil {
				return n, writeErr
			}
			pause()
			return n, nil
		}
		_, err = store.AppendBatch([]Record{candidate})
	case "fsync-complete-before-return":
		store.wal.syncFrame = func() error {
			if syncErr := store.wal.f.Sync(); syncErr != nil {
				return syncErr
			}
			pause()
			return nil
		}
		_, err = store.AppendBatch([]Record{candidate})
	case "append-returned":
		accepted, appendErr := store.AppendBatch([]Record{candidate})
		if appendErr != nil || len(accepted) != 1 || !accepted[0] {
			t.Fatalf("candidate append: accepted=%v err=%v", accepted, appendErr)
		}
		pause()
	default:
		t.Fatalf("unknown crash stage %q", stage)
	}
	if err == nil {
		t.Fatal("AppendBatch unexpectedly returned before the parent killed the process")
	}
}

func TestC2WALV1ProcessCrashRecovery(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("WAL writer locking is not implemented on Windows")
	}
	for _, stage := range []string{"partial-write", "fsync-complete-before-return", "append-returned"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			cfg := c2WALV1CrashConfig(dir)
			seed, err := OpenStore(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err := seed.Mark("aa", 1, "baseline", "committed prefix before process crash"); err != nil {
				t.Fatal(err)
			}
			if err := seed.Close(); err != nil {
				t.Fatal(err)
			}
			walPath := filepath.Join(dir, "journal.wal")
			baseline, err := OpenWAL(walPath, nil)
			if err != nil {
				t.Fatal(err)
			}
			baselineCount := baseline.Count()
			if err := baseline.Close(); err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestC2WALV1CrashProcessHelper$")
			cmd.Env = append(os.Environ(),
				c2WALV1CrashHelperEnv+"=1",
				c2WALV1CrashDirEnv+"="+dir,
				c2WALV1CrashStageEnv+"="+stage,
			)
			input, err := cmd.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			defer input.Close()
			output, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			var stderr strings.Builder
			cmd.Stderr = &stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}

			scanner := bufio.NewScanner(output)
			var ready, callbackSeen bool
			var diagnostics strings.Builder
			for scanner.Scan() {
				line := scanner.Text()
				if strings.HasPrefix(line, "C2_WAL_V1_CALLBACK ") {
					callbackSeen = true
				}
				if line == "C2_WAL_V1_READY "+stage {
					ready = true
					break
				}
				diagnostics.WriteString(line + "\n")
			}
			if !ready {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
				t.Fatalf("child did not reach %s: stdout=%q stderr=%q scanner=%v", stage, diagnostics.String(), stderr.String(), scanner.Err())
			}
			if stage == "append-returned" && !callbackSeen {
				t.Fatal("successful AppendBatch returned without the durable observer callback")
			}
			if stage != "append-returned" && callbackSeen {
				t.Fatalf("durable observer ran before AppendBatch returned at %s", stage)
			}
			if err := cmd.Process.Kill(); err != nil {
				t.Fatalf("SIGKILL child at %s: %v", stage, err)
			}
			if err := cmd.Wait(); err == nil {
				t.Fatal("child exited successfully instead of being killed")
			}

			var recovered []Record
			wal, err := OpenWAL(walPath, func(record Record) error {
				recovered = append(recovered, record)
				return nil
			})
			if err != nil {
				t.Fatalf("reopen WAL after %s: %v", stage, err)
			}
			if stage == "partial-write" {
				if !wal.RecoveredTail {
					t.Fatal("partial frame was not reported as a recovered tail")
				}
				if wal.Count() != baselineCount+1 {
					t.Fatalf("partial frame changed committed prefix count: got=%d want=%d", wal.Count(), baselineCount+1)
				}
			} else {
				if wal.RecoveredTail {
					t.Fatal("complete fsynced frame was incorrectly treated as a torn tail")
				}
				if wal.Count() != baselineCount+2 {
					t.Fatalf("complete frame missing after %s: got=%d want=%d", stage, wal.Count(), baselineCount+2)
				}
			}
			candidateFound := false
			for _, record := range recovered {
				if record.DedupeKey == "c2-wal-v1-crash-candidate" {
					candidateFound = true
				}
			}
			if candidateFound != (stage != "partial-write") {
				t.Fatalf("candidate recovery=%v at stage=%s; recovered=%+v", candidateFound, stage, recovered)
			}
			if err := wal.Close(); err != nil {
				t.Fatal(err)
			}
			if stage == "partial-write" {
				reopened, err := OpenWAL(walPath, nil)
				if err != nil {
					t.Fatalf("second open after torn-tail repair: %v", err)
				}
				defer reopened.Close()
				if reopened.RecoveredTail || reopened.Count() != baselineCount+1 {
					t.Fatalf("torn-tail repair was not stable: recovered=%v count=%d", reopened.RecoveredTail, reopened.Count())
				}
			}
		})
	}
}
