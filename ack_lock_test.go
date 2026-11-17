package core

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	lab "github.com/sirdont0/solana-yellowstone-grpc-data-core/internal/ingest"
)

// The child process holds the lock while the parent attempts an older Ack.
// Without the OS lock the parent can overwrite a newer cursor using a stale
// read. This also checks that two Engine instances share the same ACK state.
func TestConsumerAckMonotonicAcrossEngineInstancesAndProcesses(t *testing.T) {
	if os.Getenv("CORE_ACK_LOCK_HELPER") == "1" {
		consumerAckLockHelper(t)
		return
	}

	cfg := configForTest(t)
	e1, err := NewRaw(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e2, err := NewRaw(cfg)
	if err != nil {
		t.Fatal(err)
	}
	store, err := lab.OpenStore(toLabConfig(cfg, cfg.Sources[0]))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Abort()
	e1.stores["a"] = store
	e2.stores["a"] = store
	if err := store.Mark("", 0, "ack_fixture", "second position"); err != nil {
		t.Fatal(err)
	}
	cp := store.Snapshot()
	if cp.WALRecords < 2 {
		t.Fatalf("fixture WAL has only %d records", cp.WALRecords)
	}
	older := RawCursor{Source: "a", Identity: cp.Identity, JournalID: cp.JournalID, Sequence: cp.WALRecords - 1}
	newer := older
	newer.Sequence++
	if err := e1.Ack("aggregator", older); err != nil {
		t.Fatal(err)
	}

	path := e1.consumerCursorPath("aggregator", "a")
	encoded, err := json.Marshal(newer)
	if err != nil {
		t.Fatal(err)
	}
	childCtx, killChild := context.WithTimeout(context.Background(), 5*time.Second)
	defer killChild()
	cmd := exec.CommandContext(childCtx, os.Args[0], "-test.run=^TestConsumerAckMonotonicAcrossEngineInstancesAndProcesses$")
	cmd.Env = append(os.Environ(), "CORE_ACK_LOCK_HELPER=1", "CORE_ACK_LOCK_PATH="+path, "CORE_ACK_LOCK_CURSOR="+string(encoded))
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	ready := make(chan string, 1)
	go func() {
		line, _ := bufio.NewReader(stdout).ReadString('\n')
		ready <- strings.TrimSpace(line)
	}()
	select {
	case line := <-ready:
		if line != "LOCKED" {
			t.Fatalf("helper did not acquire cursor lock: %q stderr=%s", line, stderr.String())
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("helper did not acquire cursor lock: stderr=%s", stderr.String())
	}

	ackDone := make(chan error, 1)
	go func() { ackDone <- e2.Ack("aggregator", older) }()
	select {
	case err := <-ackDone:
		t.Fatalf("Ack escaped another process's cursor lock: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := stdin.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-ackDone:
		if !errors.Is(err, ErrCursorRegression) {
			t.Fatalf("older Ack after newer write = %v; want %v", err, ErrCursorRegression)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Ack did not resume after helper released cursor lock")
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("cursor lock helper: %v; stderr=%s", err, stderr.String())
	}
	got, err := e1.ConsumerCursor("aggregator", "a")
	if err != nil || got != newer {
		t.Fatalf("durable cursor regressed: got=%+v want=%+v err=%v", got, newer, err)
	}
}

func consumerAckLockHelper(t *testing.T) {
	t.Helper()
	path := os.Getenv("CORE_ACK_LOCK_PATH")
	var newer RawCursor
	if err := json.Unmarshal([]byte(os.Getenv("CORE_ACK_LOCK_CURSOR")), &newer); err != nil {
		t.Fatal(err)
	}
	lock, err := lockConsumerCursor(path)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if _, err := fmt.Fprintln(os.Stdout, "LOCKED"); err != nil {
		t.Fatal(err)
	}
	if _, err := bufio.NewReader(os.Stdin).ReadByte(); err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if err := writeCursorAtomic(path, newer); err != nil {
		t.Fatal(err)
	}
}
