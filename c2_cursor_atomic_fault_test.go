package core

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestC2CursorAtomicFaultBoundaries(t *testing.T) {
	old := RawCursor{Source: "a", Identity: "identity", JournalID: "journal", ConsumerEpoch: "epoch", Sequence: 1}
	newCursor := old
	newCursor.Sequence = 2
	failure := errors.New("injected cursor persistence fault")
	for _, tc := range []struct {
		name  string
		hooks cursorAtomicHooks
		want  RawCursor
	}{
		{"temp-fsync", cursorAtomicHooks{syncTemp: func(*os.File) error { return failure }}, old},
		{"rename", cursorAtomicHooks{rename: func(string, string) error { return failure }}, old},
		{"dir-fsync-ambiguous", cursorAtomicHooks{syncDir: func(string) error { return failure }}, newCursor},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "consumer.json")
			if err := writeCursorAtomic(path, old); err != nil {
				t.Fatal(err)
			}
			if err := writeCursorAtomicWithHooks(path, newCursor, tc.hooks); !errors.Is(err, failure) {
				t.Fatalf("fault not reported: %v", err)
			}
			content, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var got RawCursor
			if err := json.Unmarshal(content, &got); err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("cursor state after fault=%+v, want=%+v", got, tc.want)
			}
			temps, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".cursor-*.tmp"))
			if err != nil {
				t.Fatal(err)
			}
			if len(temps) != 0 {
				t.Fatalf("cursor temporary files leaked: %v", temps)
			}
		})
	}
}
