package ingest

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

var walMagic = []byte("YLABWAL1")

const maxFrame = 128 << 20
const maxWALReadBatchBytes = 128 << 20
const maxWALReadBatchRecords = 1024

// maxRecordMetadataBytes reserves room for JSON envelope fields around the
// base64-encoded protobuf payload. Config validation keeps the receive limit
// below the resulting raw-payload ceiling.
const maxRecordMetadataBytes = 1 << 20

type WAL struct {
	mu            sync.Mutex
	f             *os.File
	path          string
	journalID     string
	v2            *segmentedWAL
	writeFrame    func([]byte) (int, error)
	syncFrame     func() error
	indexReady    bool
	records       uint64
	offset        int64
	index         []walIndex
	lastSync      time.Duration
	totalSync     time.Duration
	maxSync       time.Duration
	syncCount     uint64
	syncBuckets   [8]uint64
	RecoveredTail bool
	failure       error
}

type walIndex struct {
	Sequence uint64
	Offset   int64
}

const walIndexStride = 1024

const walIndexCacheVersion = 1
const maxWALIndexCacheBytes = 64 << 20

// The sidecar is only a seek hint. WAL frames remain authoritative, and a
// missing, stale or malformed sidecar always falls back to sequential reads.
type walIndexCache struct {
	Version    int        `json:"version"`
	Size       int64      `json:"size"`
	ModifiedNS int64      `json:"modified_ns"`
	Entries    []walIndex `json:"entries"`
}

var ErrWALClosed = errors.New("WAL writer is closed")

// OpenWAL refuses concurrent writers and detects fully written corrupt records.
// An incomplete last frame from an interrupted write is truncated and reported
// so the caller can persist a continuity_break marker before resuming.
func OpenWAL(path string, visit func(Record) error) (*WAL, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	if err = lockWALWriter(f); err != nil {
		f.Close()
		return nil, fmt.Errorf("WAL already locked: %w", err)
	}
	w := &WAL{f: f, path: path}
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		w.Close()
		return nil, err
	}
	if size == 0 {
		if _, err = f.Write(walMagic); err != nil {
			w.Close()
			return nil, err
		}
		if err = f.Sync(); err != nil {
			w.Close()
			return nil, err
		}
		dir, e := os.Open(filepath.Dir(path))
		if e != nil {
			w.Close()
			return nil, e
		}
		e = dir.Sync()
		_ = dir.Close()
		if e != nil {
			w.Close()
			return nil, e
		}
		w.offset = int64(len(walMagic))
		w.indexReady = true
		return w, nil
	}
	if size < int64(len(walMagic)) {
		w.Close()
		return nil, errors.New("WAL has incomplete header")
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		w.Close()
		return nil, err
	}
	head := make([]byte, len(walMagic))
	if _, err = io.ReadFull(f, head); err != nil {
		w.Close()
		return nil, err
	}
	if string(head) != string(walMagic) {
		w.Close()
		return nil, errors.New("wrong WAL format")
	}
	good := int64(len(walMagic))
	for good < size {
		var frame [8]byte
		n, e := io.ReadFull(f, frame[:])
		if e != nil {
			if errors.Is(e, io.EOF) || errors.Is(e, io.ErrUnexpectedEOF) {
				_ = n
				w.RecoveredTail = true
				break
			}
			w.Close()
			return nil, e
		}
		length := binary.LittleEndian.Uint32(frame[:4])
		want := binary.LittleEndian.Uint32(frame[4:])
		if length == 0 || length > maxFrame {
			w.Close()
			return nil, fmt.Errorf("invalid WAL frame at %d size=%d", good, length)
		}
		if int64(length) > size-good-8 {
			w.RecoveredTail = true
			break
		}
		body := make([]byte, length)
		_, e = io.ReadFull(f, body)
		if e != nil {
			if errors.Is(e, io.EOF) || errors.Is(e, io.ErrUnexpectedEOF) {
				w.RecoveredTail = true
				break
			}
			w.Close()
			return nil, e
		}
		if crc32.ChecksumIEEE(body) != want {
			w.Close()
			return nil, fmt.Errorf("WAL checksum mismatch at %d", good)
		}
		var r Record
		if e = json.Unmarshal(body, &r); e != nil {
			w.Close()
			return nil, fmt.Errorf("WAL JSON at %d: %w", good, e)
		}
		expectedSequence := w.records + 1
		if r.Sequence == 0 {
			// v1 journals predate explicit cursors. Their append order is
			// immutable, so derive the same 1-based sequence while reading.
			r.Sequence = expectedSequence
		} else if r.Sequence != expectedSequence {
			w.Close()
			return nil, fmt.Errorf("WAL sequence mismatch at %d: got=%d want=%d", good, r.Sequence, expectedSequence)
		}
		if visit != nil {
			if e = visit(r); e != nil {
				w.Close()
				return nil, e
			}
		}
		if w.records%walIndexStride == 0 {
			w.index = append(w.index, walIndex{Sequence: expectedSequence, Offset: good})
		}
		good += 8 + int64(length)
		w.records++
	}
	if w.RecoveredTail {
		if err = f.Truncate(good); err != nil {
			w.Close()
			return nil, err
		}
		if err = f.Sync(); err != nil {
			w.Close()
			return nil, err
		}
	}
	w.offset = good
	if _, err = f.Seek(0, io.SeekEnd); err != nil {
		w.Close()
		return nil, err
	}
	w.indexReady = true
	return w, nil
}

func (w *WAL) Append(r Record) error {
	return w.AppendBatch([]Record{r})
}

// AppendBatch preserves input order and performs one fsync for the batch.
// A failed write/sync is fatal to the owning stream; callers must not claim
// durability for any record in a failed batch.
func (w *WAL) AppendBatch(records []Record) (err error) {
	if w != nil && w.v2 != nil {
		return w.v2.appendBatch(records)
	}
	if len(records) == 0 {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return errors.New("WAL closed")
	}
	if w.failure != nil {
		return w.failure
	}
	defer func() {
		if err != nil {
			w.failure = err
		}
	}()
	if uint64(len(records)) > ^uint64(0)-w.records {
		return errors.New("WAL sequence overflow")
	}
	// Encode and validate the entire batch before changing the journal. A
	// failure in a later record must not leave earlier valid frames behind.
	frames := make([][]byte, 0, len(records))
	nextOffset := w.offset
	newIndex := make([]walIndex, 0, (len(records)+walIndexStride-1)/walIndexStride)
	for i := range records {
		r := records[i]
		nextSequence := w.records + uint64(i) + 1
		if r.Sequence == 0 {
			r.Sequence = nextSequence
		} else if r.Sequence != nextSequence {
			return fmt.Errorf("WAL sequence mismatch: got=%d want=%d", r.Sequence, nextSequence)
		}
		b, e := json.Marshal(r)
		if e != nil {
			return e
		}
		if len(b) == 0 || len(b) > maxFrame {
			return fmt.Errorf("WAL record size %d invalid", len(b))
		}
		frame := make([]byte, 8+len(b))
		binary.LittleEndian.PutUint32(frame[:4], uint32(len(b)))
		binary.LittleEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(b))
		copy(frame[8:], b)
		if (nextSequence-1)%walIndexStride == 0 {
			newIndex = append(newIndex, walIndex{Sequence: nextSequence, Offset: nextOffset})
		}
		frames = append(frames, frame)
		nextOffset += int64(len(frame))
	}
	// If a short write or fsync fails, remove every frame from this batch.
	// The previous fsynced prefix is never truncated. Failure remains sticky
	// even after a successful rollback because the stream lost durability.
	rollback := func(cause error) error {
		truncateErr := w.f.Truncate(w.offset)
		_, seekErr := w.f.Seek(w.offset, io.SeekStart)
		syncErr := w.f.Sync()
		if truncateErr != nil || seekErr != nil || syncErr != nil {
			return errors.Join(cause, fmt.Errorf("WAL batch rollback failed: %w", errors.Join(truncateErr, seekErr, syncErr)))
		}
		return cause
	}
	if _, e := w.f.Seek(w.offset, io.SeekStart); e != nil {
		return e
	}
	write := w.writeFrame
	if write == nil {
		write = w.f.Write
	}
	for _, frame := range frames {
		n, e := write(frame)
		if e != nil || n != len(frame) {
			if e == nil {
				e = io.ErrShortWrite
			}
			return rollback(e)
		}
	}
	syncStarted := time.Now()
	sync := w.syncFrame
	if sync == nil {
		sync = w.f.Sync
	}
	syncErr := sync()
	syncDuration := time.Since(syncStarted)
	w.lastSync = syncDuration
	w.totalSync += syncDuration
	w.syncCount++
	// Exclusive buckets: <=1/5/10/25/50/100/500ms, then >500ms.
	syncThresholds := [...]time.Duration{time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond, 25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 500 * time.Millisecond}
	syncBucket := len(syncThresholds)
	for i, threshold := range syncThresholds {
		if syncDuration <= threshold {
			syncBucket = i
			break
		}
	}
	w.syncBuckets[syncBucket]++
	if syncDuration > w.maxSync {
		w.maxSync = syncDuration
	}
	if syncErr != nil {
		return rollback(syncErr)
	}
	w.records += uint64(len(records))
	w.offset = nextOffset
	w.index = append(w.index, newIndex...)
	return nil
}
func (w *WAL) Count() uint64 {
	if w != nil && w.v2 != nil {
		return w.v2.count()
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.records
}

func (w *WAL) SyncStats() (last, total, max time.Duration, count uint64) {
	if w != nil && w.v2 != nil {
		return w.v2.syncStats()
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastSync, w.totalSync, w.maxSync, w.syncCount
}

func (w *WAL) SyncHistogram() [8]uint64 {
	if w != nil && w.v2 != nil {
		return w.v2.syncHistogram()
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.syncBuckets
}

// ReadAfter returns a bounded copy of committed records after sequence. The
// sparse index keeps live raw consumers from rescanning the full journal on
// every poll while adding one offset per 1024 records.
func (w *WAL) ReadAfter(sequence uint64, limit int) ([]Record, error) {
	if w != nil && w.v2 != nil {
		return w.v2.readAfter(sequence, limit)
	}
	if limit <= 0 {
		limit = 128
	}
	if limit > maxWALReadBatchRecords {
		limit = maxWALReadBatchRecords
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil, errors.New("WAL closed")
	}
	if sequence > w.records {
		return nil, fmt.Errorf("WAL cursor %d exceeds record count %d", sequence, w.records)
	}
	if sequence == w.records {
		return nil, nil
	}
	startSequence := uint64(1)
	startOffset := int64(len(walMagic))
	if len(w.index) > 0 {
		index := sort.Search(len(w.index), func(i int) bool { return w.index[i].Sequence > sequence }) - 1
		if index >= 0 {
			startSequence = w.index[index].Sequence
			startOffset = w.index[index].Offset
		}
	}
	remaining := w.records - sequence
	capacity := limit
	if remaining < uint64(capacity) {
		capacity = int(remaining)
	}
	results := make([]Record, 0, capacity)
	var readBytes int64
	for current, offset := startSequence, startOffset; current <= w.records && len(results) < limit; current++ {
		var frame [8]byte
		if _, err := w.f.ReadAt(frame[:], offset); err != nil {
			return nil, err
		}
		length := binary.LittleEndian.Uint32(frame[:4])
		if length == 0 || length > maxFrame {
			return nil, fmt.Errorf("invalid WAL frame at %d", offset)
		}
		frameBytes := int64(8) + int64(length)
		if len(results) > 0 && readBytes+frameBytes > maxWALReadBatchBytes {
			break
		}
		body := make([]byte, length)
		if _, err := w.f.ReadAt(body, offset+8); err != nil {
			return nil, err
		}
		if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(frame[4:]) {
			return nil, fmt.Errorf("WAL CRC at %d", offset)
		}
		var record Record
		if err := json.Unmarshal(body, &record); err != nil {
			return nil, err
		}
		if record.Sequence == 0 {
			record.Sequence = current
		} else if record.Sequence != current {
			return nil, fmt.Errorf("WAL sequence mismatch at %d: got=%d want=%d", offset, record.Sequence, current)
		}
		if current > sequence {
			results = append(results, record)
			readBytes += frameBytes
		}
		offset += 8 + int64(length)
	}
	return results, nil
}

func (w *WAL) persistIndexCache() error {
	stat, err := w.f.Stat()
	if err != nil {
		return err
	}
	cache := walIndexCache{
		Version:    walIndexCacheVersion,
		Size:       stat.Size(),
		ModifiedNS: stat.ModTime().UnixNano(),
		Entries:    w.index,
	}
	dir := filepath.Dir(w.path)
	tmp, err := os.CreateTemp(dir, ".wal-index-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := json.NewEncoder(tmp).Encode(cache); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), w.path+".sidx")
}

func loadWALIndexCache(path string, f *os.File) []walIndex {
	stat, err := f.Stat()
	if err != nil {
		return nil
	}
	indexFile, err := os.Open(path + ".sidx")
	if err != nil {
		return nil
	}
	defer indexFile.Close()
	indexStat, err := indexFile.Stat()
	if err != nil || indexStat.Size() > maxWALIndexCacheBytes {
		return nil
	}
	var cache walIndexCache
	if err := json.NewDecoder(indexFile).Decode(&cache); err != nil {
		return nil
	}
	if cache.Version != walIndexCacheVersion || cache.Size != stat.Size() || cache.ModifiedNS != stat.ModTime().UnixNano() {
		return nil
	}
	var previous int64
	for i, entry := range cache.Entries {
		if entry.Sequence != uint64(i)*walIndexStride+1 || entry.Offset < int64(len(walMagic)) || entry.Offset >= cache.Size || (i > 0 && entry.Offset <= previous) {
			return nil
		}
		if i == 0 && entry.Offset != int64(len(walMagic)) {
			return nil
		}
		if !validWALIndexEntry(f, cache.Size, entry) {
			return nil
		}
		previous = entry.Offset
	}
	return cache.Entries
}

// A sidecar is an optional hint. Require a real, checksummed frame with an
// explicit sequence at each hinted offset. Legacy unsequenced WAL records
// cannot authenticate arbitrary offsets, so they use sequential reading.
func validWALIndexEntry(f *os.File, size int64, entry walIndex) bool {
	var frame [8]byte
	if _, err := f.ReadAt(frame[:], entry.Offset); err != nil {
		return false
	}
	length := binary.LittleEndian.Uint32(frame[:4])
	if length == 0 || length > maxFrame || int64(length) > size-entry.Offset-8 {
		return false
	}
	body := make([]byte, length)
	if _, err := f.ReadAt(body, entry.Offset+8); err != nil {
		return false
	}
	if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(frame[4:]) {
		return false
	}
	var record struct {
		Sequence uint64 `json:"sequence"`
	}
	return json.Unmarshal(body, &record) == nil && record.Sequence == entry.Sequence
}

func (w *WAL) Close() error {
	if w == nil {
		return nil
	}
	if w.v2 != nil {
		return w.v2.close()
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	// Cache construction is best effort; failure must never invalidate a
	// durable WAL or prevent the exclusive file lock from being released.
	if w.failure == nil && w.indexReady && w.path != "" {
		_ = w.persistIndexCache()
	}
	// Closing the descriptor releases the lock; never unlock before close.
	err := w.f.Close()
	w.f = nil
	return err
}

// WALReader holds a nonblocking shared lock for preflight and replay. A live
// writer is rejected instead of racing an append or tail repair.
type WALReader struct {
	f           *os.File
	v2          *segmentedWALReader
	index       []walIndex
	offset      int64
	sequence    uint64
	maxSequence uint64
	bounded     bool
	initialized bool
	// Migration refuses future envelope fields it cannot faithfully copy.
	strictJSONFields bool
}
type WALReadReport struct {
	TruncatedTail bool
	ValidBytes    int64
	Records       uint64
}

func OpenWALReader(path string) (*WALReader, error) {
	if stat, err := os.Stat(path); err == nil && stat.IsDir() {
		reader, err := openWALV2Reader(path)
		if err != nil {
			return nil, err
		}
		return &WALReader{v2: reader, maxSequence: reader.last, bounded: true}, nil
	} else if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if err = lockWALReader(f); err != nil {
		f.Close()
		return nil, fmt.Errorf("WAL has an active writer: %w", err)
	}
	return &WALReader{f: f, index: loadWALIndexCache(path, f)}, nil
}
func (r *WALReader) Close() error {
	if r == nil {
		return nil
	}
	if r.v2 != nil {
		return r.v2.close()
	}
	if r.f == nil {
		return nil
	}
	return r.f.Close()
}

// ReadAfter advances this reader sequentially and returns records after the
// requested cursor. Reusing one reader avoids rescanning a long v1 journal for
// each consumer batch.
func (r *WALReader) ReadAfter(sequence uint64, limit int) ([]Record, error) {
	if r.v2 != nil {
		if r.bounded && sequence > r.maxSequence {
			return nil, fmt.Errorf("WAL cursor %d exceeds durable record count %d", sequence, r.maxSequence)
		}
		if limit <= 0 {
			limit = 128
		}
		if r.bounded {
			remaining := r.maxSequence - sequence
			if remaining == 0 {
				return nil, nil
			}
			if uint64(limit) > remaining {
				limit = int(remaining)
			}
		}
		return r.v2.readAfter(sequence, limit)
	}
	if limit <= 0 {
		limit = 128
	}
	if limit > maxWALReadBatchRecords {
		limit = maxWALReadBatchRecords
	}
	if !r.initialized {
		if _, err := r.f.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		header := make([]byte, len(walMagic))
		if _, err := io.ReadFull(r.f, header); err != nil {
			return nil, err
		}
		if string(header) != string(walMagic) {
			return nil, errors.New("wrong WAL format")
		}
		r.offset = int64(len(walMagic))
		r.initialized = true
	}
	if sequence < r.sequence {
		return nil, fmt.Errorf("WAL reader cannot rewind from %d to %d", r.sequence, sequence)
	}
	if r.bounded && sequence > r.maxSequence {
		return nil, fmt.Errorf("WAL cursor %d exceeds durable record count %d", sequence, r.maxSequence)
	}
	// A fresh closed reader can resume near its cursor using an index written
	// at clean WAL close. Keep sequential state for normal subsequent pages.
	if sequence > r.sequence && len(r.index) > 0 {
		index := sort.Search(len(r.index), func(i int) bool { return r.index[i].Sequence-1 > sequence }) - 1
		if index >= 0 && r.index[index].Sequence-1 > r.sequence {
			r.sequence = r.index[index].Sequence - 1
			r.offset = r.index[index].Offset
		}
	}
	readOne := func() (Record, error) {
		var frame [8]byte
		n, err := r.f.ReadAt(frame[:], r.offset)
		if errors.Is(err, io.EOF) && n == 0 {
			return Record{}, io.EOF
		}
		if err != nil {
			return Record{}, fmt.Errorf("WAL truncated at %d: %w", r.offset, err)
		}
		length := binary.LittleEndian.Uint32(frame[:4])
		if length == 0 || length > maxFrame {
			return Record{}, fmt.Errorf("invalid WAL frame at %d", r.offset)
		}
		body := make([]byte, length)
		if _, err := r.f.ReadAt(body, r.offset+8); err != nil {
			return Record{}, fmt.Errorf("WAL truncated at %d: %w", r.offset, err)
		}
		if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(frame[4:]) {
			return Record{}, fmt.Errorf("WAL CRC at %d", r.offset)
		}
		var record Record
		if err := json.Unmarshal(body, &record); err != nil {
			return Record{}, err
		}
		expected := r.sequence + 1
		if record.Sequence == 0 {
			record.Sequence = expected
		} else if record.Sequence != expected {
			return Record{}, fmt.Errorf("WAL sequence mismatch at %d: got=%d want=%d", r.offset, record.Sequence, expected)
		}
		r.sequence = expected
		r.offset += 8 + int64(length)
		return record, nil
	}
	for r.sequence < sequence {
		if r.bounded && r.sequence >= r.maxSequence {
			return nil, fmt.Errorf("WAL cursor %d exceeds durable record count %d", sequence, r.maxSequence)
		}
		if _, err := readOne(); err != nil {
			if err == io.EOF {
				return nil, fmt.Errorf("WAL cursor %d exceeds journal end %d", sequence, r.sequence)
			}
			return nil, err
		}
	}
	results := make([]Record, 0, limit)
	var readBytes int64
	for len(results) < limit && (!r.bounded || r.sequence < r.maxSequence) {
		var frame [8]byte
		n, frameErr := r.f.ReadAt(frame[:], r.offset)
		if errors.Is(frameErr, io.EOF) && n == 0 {
			return results, nil
		}
		if frameErr != nil {
			if len(results) > 0 {
				return results, nil
			}
			return nil, fmt.Errorf("WAL truncated at %d: %w", r.offset, frameErr)
		}
		length := binary.LittleEndian.Uint32(frame[:4])
		if length == 0 || length > maxFrame {
			if len(results) > 0 {
				return results, nil
			}
			return nil, fmt.Errorf("invalid WAL frame at %d", r.offset)
		}
		frameBytes := int64(8) + int64(length)
		if len(results) > 0 && readBytes+frameBytes > maxWALReadBatchBytes {
			return results, nil
		}
		record, err := readOne()
		if err == io.EOF {
			return results, nil
		}
		if err != nil {
			// Deliver the verified prefix first. The offending frame remains at
			// the same offset and will produce an error on the next ReadAfter.
			if len(results) > 0 {
				return results, nil
			}
			return nil, err
		}
		results = append(results, record)
		readBytes += frameBytes
	}
	return results, nil
}

func ReadWAL(path string, visit func(Record) error) error {
	r, err := OpenWALReader(path)
	if err != nil {
		return err
	}
	defer r.Close()
	_, err = r.Read(false, visit)
	return err
}

// Read verifies the entire immutable prefix. Tolerant mode ignores only an
// incomplete final frame, never a bad header, invalid length, JSON or CRC.
func (r *WALReader) Read(tolerant bool, visit func(Record) error) (report WALReadReport, err error) {
	if r.v2 != nil {
		sequence := uint64(0)
		for sequence < r.maxSequence {
			records, readErr := r.v2.readAfter(sequence, maxWALReadBatchRecords)
			if readErr != nil {
				return report, readErr
			}
			if len(records) == 0 {
				return report, fmt.Errorf("WAL v2 ended at sequence %d before manifest head %d", sequence, r.maxSequence)
			}
			for _, record := range records {
				if visit != nil {
					if err := visit(record); err != nil {
						return report, err
					}
				}
				sequence = record.Sequence
				report.Records++
			}
		}
		for _, segment := range append(append([]walV2Segment(nil), r.v2.sealed...), r.v2.active) {
			report.ValidBytes += segment.meta.DataBytes
			if segment.meta.Digest != "" {
				report.ValidBytes += walV2FooterBytes
			}
		}
		return report, nil
	}
	f := r.f
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return
	}
	stat, err := f.Stat()
	if err != nil {
		return report, err
	}
	header := make([]byte, len(walMagic))
	if _, err = io.ReadFull(f, header); err != nil {
		return
	}
	if string(header) != string(walMagic) {
		return report, errors.New("wrong WAL format")
	}
	report.ValidBytes = int64(len(walMagic))
	tail := func(e error) (WALReadReport, error) {
		if tolerant && (errors.Is(e, io.EOF) || errors.Is(e, io.ErrUnexpectedEOF)) {
			report.TruncatedTail = true
			return report, nil
		}
		return report, fmt.Errorf("WAL truncated at %d: %w", report.ValidBytes, e)
	}
	for {
		var frame [8]byte
		_, err = io.ReadFull(f, frame[:])
		if errors.Is(err, io.EOF) {
			return report, nil
		}
		if err != nil {
			return tail(err)
		}
		length := binary.LittleEndian.Uint32(frame[:4])
		if length == 0 || length > maxFrame {
			return report, fmt.Errorf("invalid WAL frame at %d", report.ValidBytes)
		}
		if int64(length) > stat.Size()-report.ValidBytes-8 {
			return tail(io.ErrUnexpectedEOF)
		}
		body := make([]byte, length)
		if _, err = io.ReadFull(f, body); err != nil {
			return tail(err)
		}
		if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(frame[4:]) {
			return report, fmt.Errorf("WAL CRC at %d", report.ValidBytes)
		}
		var rec Record
		if err = json.Unmarshal(body, &rec); err != nil {
			return report, err
		}
		if r.strictJSONFields {
			decoder := json.NewDecoder(bytes.NewReader(body))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&rec); err != nil {
				return report, fmt.Errorf("migration cannot preserve unsupported v1 JSON field at offset %d: %w", report.ValidBytes, err)
			}
		}
		expectedSequence := report.Records + 1
		if rec.Sequence == 0 {
			rec.Sequence = expectedSequence
		} else if rec.Sequence != expectedSequence {
			return report, fmt.Errorf("WAL sequence mismatch at %d: got=%d want=%d", report.ValidBytes, rec.Sequence, expectedSequence)
		}
		if visit != nil {
			if err = visit(rec); err != nil {
				return report, err
			}
		}
		report.ValidBytes += 8 + int64(length)
		report.Records++
	}
}
