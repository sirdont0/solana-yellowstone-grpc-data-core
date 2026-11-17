package ingest

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
)

func legacyV011IdentityFixture(c Config) string {
	v := struct {
		SourceID                                                     string `json:"SourceID,omitempty"`
		Endpoint, Commitment, FilterVersion, Mint, TokenAccountOwner string
		Programs                                                     []string
	}{c.SourceID, c.Endpoint, c.Commitment, c.FilterVersion, c.Mint, c.TokenAccountOwner, c.Programs}
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func TestLegacyV011IdentityRestartAndScopeRejection(t *testing.T) {
	c := recoveryConfig(t)
	oldID := legacyV011IdentityFixture(c)
	if c.Identity() != oldID {
		t.Fatalf("unchanged v0.1.1 identity=%s want=%s", c.Identity(), oldID)
	}
	// Build an actual legacy frame: the old WAL had neither sequence nor
	// journal_id fields. Current Append would add a sequence automatically.
	path := filepath.Join(c.DataDir, "journal.wal")
	if err := os.MkdirAll(c.DataDir, 0700); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(Record{Type: "marker", Kind: "legacy_fixture", Identity: oldID})
	if err != nil {
		t.Fatal(err)
	}
	frame := make([]byte, len(walMagic)+8+len(body))
	copy(frame, walMagic)
	binary.LittleEndian.PutUint32(frame[len(walMagic):], uint32(len(body)))
	binary.LittleEndian.PutUint32(frame[len(walMagic)+4:], crc32.ChecksumIEEE(body))
	copy(frame[len(walMagic)+8:], body)
	if err := os.WriteFile(path, frame, 0600); err != nil {
		t.Fatal(err)
	}
	s, err := OpenStore(c)
	if err != nil {
		t.Fatalf("unchanged legacy restart failed: %v", err)
	}
	records, err := s.ReadAfter(0, 10)
	if err != nil || len(records) < 2 || records[0].Kind != "legacy_fixture" {
		t.Fatalf("legacy replay lost: records=%v err=%v", records, err)
	}
	journalID := s.Snapshot().JournalID
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenWALReader(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(reader.index) != 0 {
		t.Fatal("unsequenced legacy sparse hint was trusted")
	}
	page, err := reader.ReadAfter(0, 10)
	if err != nil || len(page) < 2 || page[0].Kind != "legacy_fixture" {
		t.Fatalf("legacy sequential replay: page=%v err=%v", page, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenStore(c)
	if err != nil {
		t.Fatalf("second legacy restart failed: %v", err)
	}
	if s.Snapshot().JournalID != journalID {
		t.Fatal("legacy restart changed journal ID")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	changed := c
	changed.Programs = []string{Token2022Program}
	if s, err := OpenStore(changed); err == nil {
		s.Abort()
		t.Fatal("changed legacy programs accepted")
	}
	custom := c
	custom.Subscription = &pb.SubscribeRequest{Transactions: map[string]*pb.SubscribeRequestFilterTransactions{"all": {AccountInclude: []string{PumpProgram}}}}
	if s, err := OpenStore(custom); err == nil {
		s.Abort()
		t.Fatal("changed custom scope accepted")
	}
}

func TestWALFormatSwitchRejectsImplicitNewHistory(t *testing.T) {
	for _, first := range []int{1, 2} {
		t.Run(map[int]string{1: "v1-to-v2", 2: "v2-to-v1"}[first], func(t *testing.T) {
			c := recoveryConfig(t)
			c.WALVersion = first
			s, err := OpenStore(c)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Append(Record{Type: "marker", Kind: "prior_history"}); err != nil {
				t.Fatal(err)
			}
			journalID := s.Snapshot().JournalID
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			other := 3 - first
			c.WALVersion = other
			if s, err := OpenStore(c); err == nil {
				s.Abort()
				t.Fatal("implicit format switch accepted")
			} else if !strings.Contains(err.Error(), "WAL format") {
				t.Fatalf("missing migration guidance: %v", err)
			}
			otherPath := filepath.Join(c.DataDir, "journal.wal")
			if other == 2 {
				otherPath = filepath.Join(c.DataDir, "journal.v2")
			}
			if _, err := os.Stat(otherPath); !os.IsNotExist(err) {
				t.Fatalf("rejected switch created a journal: %v", err)
			}
			c.WALVersion = first
			s, err = OpenStore(c)
			if err != nil {
				t.Fatalf("original WAL cannot reopen: %v", err)
			}
			defer s.Close()
			if s.Snapshot().JournalID != journalID {
				t.Fatal("journal ID changed")
			}
			records, err := s.ReadAfter(0, 16)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, r := range records {
				found = found || r.Kind == "prior_history"
			}
			if !found {
				t.Fatal("original history disappeared")
			}
		})
	}
}

func TestConcurrentWALFormatOpensCannotSplitHistory(t *testing.T) {
	c := recoveryConfig(t)
	start := make(chan struct{})
	type result struct {
		store *Store
		err   error
	}
	out := make(chan result, 2)
	for _, version := range []int{1, 2} {
		candidate := c
		candidate.WALVersion = version
		go func() {
			<-start
			s, err := OpenStore(candidate)
			out <- result{s, err}
		}()
	}
	close(start)
	first, second := <-out, <-out
	succeeded := 0
	for _, r := range []result{first, second} {
		if r.err == nil {
			succeeded++
			if err := r.store.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if succeeded != 1 {
		t.Fatalf("concurrent format opens succeeded=%d, want exactly one (errors: %v, %v)", succeeded, first.err, second.err)
	}
	paths := 0
	for _, name := range []string{"journal.wal", "journal.v2"} {
		if _, err := os.Stat(filepath.Join(c.DataDir, name)); err == nil {
			paths++
		} else if !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
	if paths != 1 {
		t.Fatalf("created %d WAL format paths, want exactly one", paths)
	}
}

func TestCorruptSparseIndexHintFallsBackToVerifiedWAL(t *testing.T) {
	path := walFixture(t, walIndexStride*3+1)
	b, err := os.ReadFile(path + ".sidx")
	if err != nil {
		t.Fatal(err)
	}
	var cache walIndexCache
	if err := json.Unmarshal(b, &cache); err != nil {
		t.Fatal(err)
	}
	cache.Entries[1].Offset++
	b, err = json.Marshal(cache)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".sidx", b, 0600); err != nil {
		t.Fatal(err)
	}
	r, err := OpenWALReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if len(r.index) != 0 {
		t.Fatal("corrupt optional index was trusted")
	}
	records, err := r.ReadAfter(walIndexStride, 1)
	if err != nil || len(records) != 1 || records[0].Sequence != walIndexStride+1 {
		t.Fatalf("valid WAL unreadable: records=%v err=%v", records, err)
	}
}

func TestWALV2ConfigRejectsOversizedAdmittedMessage(t *testing.T) {
	c := recoveryConfig(t)
	c.WALVersion = 2
	c.WALSegmentBytes = 2 << 20
	c.BatchBytes = 1 << 20
	c.MaxMessageBytes = 3 << 20
	if err := c.Validate(); err == nil {
		t.Fatal("3 MiB accepted transport message cannot fit a 2 MiB WAL v2 segment")
	}
	c.WALSegmentBytes = walV2DefaultSegmentBytes
	c.BatchBytes = 16 << 20
	c.MaxMessageBytes = 64 << 20
	if err := c.Validate(); err != nil {
		t.Fatalf("default WAL v2 capacity rejected: %v", err)
	}
	const segmentHeaderEnvelope = 18
	minimum := int64(c.MaxMessageBytes) + 2*walV2MaxHeaderBytes + walV2FramePrefixBytes + 8 + segmentHeaderEnvelope + walV2FooterBytes
	for _, tc := range []struct {
		segment int64
		wantOK  bool
	}{{minimum - 1, false}, {minimum, true}, {minimum + 1, true}} {
		c.WALSegmentBytes = tc.segment
		if err := c.Validate(); (err == nil) != tc.wantOK {
			t.Fatalf("segment=%d accepted=%t want=%t err=%v", tc.segment, err == nil, tc.wantOK, err)
		}
	}
}
