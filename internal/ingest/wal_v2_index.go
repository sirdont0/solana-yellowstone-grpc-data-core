package ingest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
)

type walV2IndexCache struct {
	Version     int
	Segment     walV2SegmentMeta
	HeaderBytes int64
	Entries     []walIndex
	Checksum    string
}

func walV2IndexPath(dir string, id uint64) string {
	return filepath.Join(dir, "indexes", walV2SegmentFileName(id, true)+".sidx")
}

func saveWALV2Index(dir string, meta walV2SegmentMeta, headerBytes int64, entries []walIndex) error {
	cache := walV2IndexCache{Version: 1, Segment: meta, HeaderBytes: headerBytes, Entries: entries}
	encoded, err := json.Marshal(cache)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(encoded)
	cache.Checksum = hex.EncodeToString(digest[:])
	encoded, err = json.Marshal(cache)
	if err != nil {
		return err
	}
	return writeWALV2Atomic(walV2IndexPath(dir, meta.ID), encoded, 0600)
}

// Called only after verifying the segment's full content digest and footer.
// A sidecar is a disposable acceleration hint, never a replacement for the
// segment. Validate each seek anchor against the actual frame before using it.
func loadWALV2Index(dir string, file *os.File, meta walV2SegmentMeta, headerBytes int64) ([]walIndex, bool) {
	f, err := os.Open(walV2IndexPath(dir, meta.ID))
	if err != nil {
		return nil, false
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxWALIndexCacheBytes+1))
	if err != nil || len(b) > maxWALIndexCacheBytes {
		return nil, false
	}
	var cache walV2IndexCache
	if json.Unmarshal(b, &cache) != nil || cache.Version != 1 || cache.Segment != meta || cache.HeaderBytes != headerBytes || meta.Records == 0 || meta.Digest == "" {
		return nil, false
	}
	checksum := cache.Checksum
	cache.Checksum = ""
	encoded, err := json.Marshal(cache)
	if err != nil {
		return nil, false
	}
	digest := sha256.Sum256(encoded)
	if checksum != hex.EncodeToString(digest[:]) || uint64(len(cache.Entries)) != (meta.Records-1)/walIndexStride+1 {
		return nil, false
	}
	previous := headerBytes - 1
	for i, entry := range cache.Entries {
		if entry.Sequence != meta.FirstSequence+uint64(i)*walIndexStride || entry.Offset <= previous || entry.Offset >= meta.DataBytes || (i == 0 && entry.Offset != headerBytes) {
			return nil, false
		}
		r, _, _, err := readWALV2FrameAt(file, entry.Offset, meta.DataBytes)
		if err != nil || r.Sequence != entry.Sequence {
			return nil, false
		}
		previous = entry.Offset
	}
	return cache.Entries, true
}
