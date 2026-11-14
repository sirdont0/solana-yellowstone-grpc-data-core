package ingest

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

var walMagic = []byte("YLABWAL1")

const maxFrame = 128 << 20

type WAL struct {
	mu            sync.Mutex
	f             *os.File
	records       uint64
	RecoveredTail bool
}

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
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("WAL already locked: %w", err)
	}
	w := &WAL{f: f}
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
		if visit != nil {
			if e = visit(r); e != nil {
				w.Close()
				return nil, e
			}
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
	if _, err = f.Seek(0, io.SeekEnd); err != nil {
		w.Close()
		return nil, err
	}
	return w, nil
}

func (w *WAL) Append(r Record) error {
	return w.AppendBatch([]Record{r})
}

// AppendBatch preserves input order and performs one fsync for the batch.
// A failed write/sync is fatal to the owning stream; callers must not claim
// durability for any record in a failed batch.
func (w *WAL) AppendBatch(records []Record) error {
	if len(records) == 0 {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, r := range records {
		b, e := json.Marshal(r)
		if e != nil {
			return e
		}
		if len(b) == 0 || len(b) > maxFrame {
			return fmt.Errorf("WAL record size %d invalid", len(b))
		}
		var frame [8]byte
		binary.LittleEndian.PutUint32(frame[:4], uint32(len(b)))
		binary.LittleEndian.PutUint32(frame[4:], crc32.ChecksumIEEE(b))
		if _, e = w.f.Write(frame[:]); e != nil {
			return e
		}
		if _, e = w.f.Write(b); e != nil {
			return e
		}
	}
	if e := w.f.Sync(); e != nil {
		return e
	}
	w.records += uint64(len(records))
	return nil
}
func (w *WAL) Count() uint64 { w.mu.Lock(); defer w.mu.Unlock(); return w.records }
func (w *WAL) Close() error {
	if w == nil || w.f == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	_ = syscall.Flock(int(w.f.Fd()), syscall.LOCK_UN)
	e := w.f.Close()
	w.f = nil
	return e
}

func ReadWAL(path string, visit func(Record) error) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	header := make([]byte, len(walMagic))
	if _, e = io.ReadFull(f, header); e != nil {
		return e
	}
	if string(header) != string(walMagic) {
		return errors.New("wrong WAL format")
	}
	offset := int64(len(walMagic))
	for {
		var frame [8]byte
		_, e = io.ReadFull(f, frame[:])
		if errors.Is(e, io.EOF) {
			return nil
		}
		if e != nil {
			return fmt.Errorf("WAL truncated at %d: %w", offset, e)
		}
		length := binary.LittleEndian.Uint32(frame[:4])
		if length == 0 || length > maxFrame {
			return fmt.Errorf("invalid WAL frame at %d", offset)
		}
		body := make([]byte, length)
		if _, e = io.ReadFull(f, body); e != nil {
			return fmt.Errorf("WAL truncated at %d: %w", offset, e)
		}
		if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(frame[4:]) {
			return fmt.Errorf("WAL CRC at %d", offset)
		}
		var rec Record
		if e = json.Unmarshal(body, &rec); e != nil {
			return e
		}
		if e = visit(rec); e != nil {
			return e
		}
		offset += 8 + int64(length)
	}
}
