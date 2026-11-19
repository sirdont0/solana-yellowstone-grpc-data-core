package core

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	lab "github.com/sirdont0/solana-yellowstone-grpc-data-core/internal/ingest"
	bolt "go.etcd.io/bbolt"
)

const (
	c2CrashConsumer = "c2durable"
	c2CrashReady    = "C2_CRASH_READY "
)

var c2CrashRollback = errors.New("injected application transaction rollback")

func c2CrashConfig(dataDir string, version int) Config {
	return Config{
		DataDir: dataDir, WALVersion: version, WALSegmentBytes: 128 << 20,
		Sources: []Source{{
			ID: "a", Endpoint: "127.0.0.1:1234", TokenHeader: "x-token",
			Commitment: "finalized", FilterVersion: "sample-1", Programs: []string{testProgram},
		}},
	}
}

func c2CrashOpenEngine(cfg Config) (*Engine, *lab.Store, error) {
	e, err := NewRaw(cfg)
	if err != nil {
		return nil, nil, err
	}
	store, err := lab.OpenStore(toLabConfig(cfg, cfg.Sources[0]))
	if err != nil {
		return nil, nil, err
	}
	e.stores["a"] = store
	return e, store, nil
}

// c2CrashBatch deliberately uses the public consumer reader. Reading a live
// Store via ReadRawFromConsumer would wait for EOF, so Next is bounded here.
func c2CrashBatch(e *Engine) ([]RawObservation, error) {
	r, err := e.OpenRawReaderFromConsumer(c2CrashConsumer, "a")
	if err != nil {
		return nil, err
	}
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return r.Next(ctx, 128)
}

func c2CrashFind(observations []RawObservation, kind string) (RawObservation, bool) {
	for _, ob := range observations {
		if ob.Kind == kind {
			return ob, true
		}
	}
	return RawObservation{}, false
}

func c2CrashOpenDB(path string) (*bolt.DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	return bolt.Open(path, 0600, &bolt.Options{Timeout: 2 * time.Second})
}

// The effect and its idempotency key commit in one durable bbolt transaction.
// The consumer epoch is omitted because application effect identity survives
// explicit consumer re-registration. NoSync is left at bbolt's default false.
func c2CrashApply(db *bolt.DB, ob RawObservation, rollback bool) error {
	cur := ob.Cursor
	cur.ConsumerEpoch = ""
	ob.Cursor = cur
	key, err := json.Marshal(cur)
	if err != nil {
		return err
	}
	value, err := json.Marshal(ob)
	if err != nil {
		return err
	}
	return db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("c2-effects"))
		if err != nil {
			return err
		}
		if existing := bucket.Get(key); existing != nil {
			if !bytes.Equal(existing, value) {
				return errors.New("different observation at committed cursor")
			}
			return nil
		}
		count, err := c2CrashCountBucket(bucket)
		if err != nil {
			return err
		}
		if err := bucket.Put(key, value); err != nil {
			return err
		}
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], count+1)
		if err := bucket.Put([]byte("_count"), encoded[:]); err != nil {
			return err
		}
		if rollback {
			return c2CrashRollback
		}
		return nil
	})
}

func c2CrashCountBucket(bucket *bolt.Bucket) (uint64, error) {
	if bucket == nil {
		return 0, nil
	}
	value := bucket.Get([]byte("_count"))
	if value == nil {
		return 0, nil
	}
	if len(value) != 8 {
		return 0, errors.New("invalid application effect count")
	}
	return binary.BigEndian.Uint64(value), nil
}

func c2CrashEffectCount(db *bolt.DB) (uint64, error) {
	var count uint64
	err := db.View(func(tx *bolt.Tx) error {
		var err error
		count, err = c2CrashCountBucket(tx.Bucket([]byte("c2-effects")))
		return err
	})
	return count, err
}

// This test runs only in a child test binary. Parent kills it with SIGKILL
// while both the Store and the application DB remain open, so no graceful
// Close, deferred cleanup, or artificial in-process restart can mask loss.
func TestC2CrashConsumerHelper(t *testing.T) {
	if os.Getenv("CORE_C2_CRASH_HELPER") != "1" {
		return
	}
	version, err := strconv.Atoi(os.Getenv("CORE_C2_CRASH_VERSION"))
	if err != nil {
		t.Fatal(err)
	}
	dir, stage := os.Getenv("CORE_C2_CRASH_DIR"), os.Getenv("CORE_C2_CRASH_STAGE")
	e, store, err := c2CrashOpenEngine(c2CrashConfig(dir, version))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Abort()
	batch, err := c2CrashBatch(e)
	if err != nil {
		t.Fatal(err)
	}
	ob, found := c2CrashFind(batch, "c2-first")
	if !found {
		t.Fatalf("target effect missing from consumer batch: %+v", batch)
	}
	db, err := c2CrashOpenDB(filepath.Join(dir, "app.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	switch stage {
	case "commit-before-ack":
		if err := c2CrashApply(db, ob, false); err != nil {
			t.Fatal(err)
		}
	case "ack-returned":
		if err := c2CrashApply(db, ob, false); err != nil {
			t.Fatal(err)
		}
		if err := e.Ack(c2CrashConsumer, ob.Cursor); err != nil {
			t.Fatal(err)
		}
	case "rollback-before-ack":
		if err := c2CrashApply(db, ob, true); !errors.Is(err, c2CrashRollback) {
			t.Fatalf("expected injected transaction rollback, got %v", err)
		}
	case "ack-write-denied":
		if err := c2CrashApply(db, ob, false); err != nil {
			t.Fatal(err)
		}
		// The existing .lock inode remains openable, but the atomic cursor
		// replacement cannot create a temporary file in this directory.
		cursorDir := filepath.Dir(e.consumerCursorPath(c2CrashConsumer, "a"))
		if err := os.Chmod(cursorDir, 0500); err != nil {
			t.Fatal(err)
		}
		ackErr := e.Ack(c2CrashConsumer, ob.Cursor)
		if err := os.Chmod(cursorDir, 0700); err != nil {
			t.Fatal(err)
		}
		if !errors.Is(ackErr, os.ErrPermission) {
			t.Fatalf("expected denied cursor temp file creation, got %v", ackErr)
		}
	default:
		t.Fatalf("unknown crash stage %q", stage)
	}
	encoded, err := json.Marshal(ob.Cursor)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(c2CrashReady + base64.StdEncoding.EncodeToString(encoded))
	var input [1]byte
	_, _ = os.Stdin.Read(input[:]) // Parent keeps stdin open until SIGKILL.
	t.Fatal("parent did not kill child at the declared persistence boundary")
}

func c2CrashKillAtReady(t *testing.T, dir string, version int, stage string) RawCursor {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestC2CrashConsumerHelper$")
	cmd.Env = append(os.Environ(),
		"CORE_C2_CRASH_HELPER=1", "CORE_C2_CRASH_DIR="+dir,
		"CORE_C2_CRASH_VERSION="+strconv.Itoa(version), "CORE_C2_CRASH_STAGE="+stage,
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
	var diagnostics strings.Builder
	cmd.Stderr = &diagnostics
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill() }()
	var cursor RawCursor
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, c2CrashReady) {
			diagnostics.WriteString(line + "\n")
			continue
		}
		payload, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(line, c2CrashReady))
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(payload, &cursor); err != nil {
			t.Fatal(err)
		}
		break
	}
	if cursor.Sequence == 0 || cursor.ConsumerEpoch == "" {
		_ = cmd.Wait()
		t.Fatalf("child did not reach %s: stderr/stdout=%s scanner=%v", stage, diagnostics.String(), scanner.Err())
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("child exited normally; no process crash occurred")
	} else if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != -1 {
		t.Fatalf("child did not terminate by signal: %v", err)
	}
	return cursor
}

func TestC2CrashDurableConsumerRecovery(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("WAL process locking and SIGKILL semantics are Unix-only")
	}
	for _, version := range []int{1, 2} {
		for _, stage := range []string{"commit-before-ack", "ack-returned", "rollback-before-ack", "ack-write-denied"} {
			t.Run(fmt.Sprintf("wal-v%d/%s", version, stage), func(t *testing.T) {
				if stage == "ack-write-denied" && os.Geteuid() == 0 {
					t.Skip("root bypasses directory write permissions")
				}
				dir := t.TempDir()
				cfg := c2CrashConfig(dir, version)
				seed, seedStore, err := c2CrashOpenEngine(cfg)
				if err != nil {
					t.Fatal(err)
				}
				start, err := seed.RegisterConsumer(c2CrashConsumer, "a", ConsumerFromBeginning)
				if err != nil {
					t.Fatal(err)
				}
				if err := seedStore.Mark("a", 10, "c2-first", "application effect"); err != nil {
					t.Fatal(err)
				}
				if err := seedStore.Mark("a", 11, "c2-second", "must survive ACK boundary"); err != nil {
					t.Fatal(err)
				}
				if err := seedStore.Close(); err != nil {
					t.Fatal(err)
				}

				crashCursor := c2CrashKillAtReady(t, dir, version, stage)
				if crashCursor.Source != "a" || crashCursor.ConsumerEpoch != start.ConsumerEpoch || crashCursor.JournalID != start.JournalID {
					t.Fatalf("crash cursor changed registration: got=%+v start=%+v", crashCursor, start)
				}
				recovered, recoveredStore, err := c2CrashOpenEngine(cfg)
				if err != nil {
					t.Fatalf("open Store after SIGKILL: %v", err)
				}
				defer recoveredStore.Abort()
				cursor, err := recovered.ConsumerCursor(c2CrashConsumer, "a")
				if err != nil {
					t.Fatal(err)
				}
				batch, err := c2CrashBatch(recovered)
				if err != nil {
					t.Fatal(err)
				}
				app, err := c2CrashOpenDB(filepath.Join(dir, "app.db"))
				if err != nil {
					t.Fatalf("open application DB after SIGKILL: %v", err)
				}
				defer app.Close()
				count, err := c2CrashEffectCount(app)
				if err != nil {
					t.Fatal(err)
				}

				var wantCursor RawCursor
				if stage == "ack-returned" {
					wantCursor = crashCursor
					if cursor != crashCursor || count != 1 {
						t.Fatalf("ACK or effect lost across SIGKILL: cursor=%+v want=%+v effects=%d", cursor, crashCursor, count)
					}
					if _, found := c2CrashFind(batch, "c2-first"); found {
						t.Fatal("durably acknowledged effect was redelivered")
					}
					if _, found := c2CrashFind(batch, "c2-second"); !found {
						t.Fatal("post-ACK record was skipped")
					}
					stale := crashCursor
					stale.Sequence--
					if err := recovered.Ack(c2CrashConsumer, stale); !errors.Is(err, ErrCursorRegression) {
						t.Fatalf("stale ACK after crash returned %v", err)
					}
				} else {
					wantCursor = crashCursor
					if cursor != start {
						t.Fatalf("unacknowledged cursor advanced across SIGKILL: got=%+v want=%+v", cursor, start)
					}
					if stage == "rollback-before-ack" && count != 0 || stage != "rollback-before-ack" && count != 1 {
						t.Fatalf("application DB rollback/commit lost boundary: effects=%d stage=%s", count, stage)
					}
					redelivered, found := c2CrashFind(batch, "c2-first")
					if !found || redelivered.Cursor != crashCursor {
						t.Fatalf("unacknowledged record not redelivered: got=%+v found=%v want=%+v", redelivered.Cursor, found, crashCursor)
					}
					if err := c2CrashApply(app, redelivered, false); err != nil {
						t.Fatal(err)
					}
					if count, err := c2CrashEffectCount(app); err != nil || count != 1 {
						t.Fatalf("redelivery double-applied effect: count=%d err=%v", count, err)
					}
					if err := recovered.Ack(c2CrashConsumer, redelivered.Cursor); err != nil {
						t.Fatal(err)
					}
				}
				if err := recoveredStore.Close(); err != nil {
					t.Fatal(err)
				}
				finalEngine, finalStore, err := c2CrashOpenEngine(cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer finalStore.Abort()
				if got, err := finalEngine.ConsumerCursor(c2CrashConsumer, "a"); err != nil || got != wantCursor {
					t.Fatalf("durable ACK changed on second restart: got=%+v want=%+v err=%v", got, wantCursor, err)
				}
			})
		}
	}
}
