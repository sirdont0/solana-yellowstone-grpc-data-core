package ingest

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
)

var (
	walV2SegmentMagic = []byte("YLABV2SG")
	walV2FooterMagic  = []byte("YLABV2FT")
	walV2CRC          = crc32.MakeTable(crc32.Castagnoli)
)

const (
	walV2Version = 2
	// walV2EnvelopeSchemaVersion versions the JSON metadata contract independently
	// from the physical segment format. The physical framing remains version 2.
	walV2EnvelopeSchemaVersion   = 1
	walV2DefaultSegmentBytes     = int64(256 << 20)
	walV2MaxHeaderBytes          = 1 << 20
	walV2FramePrefixBytes        = 16
	walV2FooterBytes             = 8 + 5*8 + sha256.Size + 4
	walV2GeyserDescriptorSHA256  = "20146a8e752395fa29404cb7577b1455626aa6f99ddab8d663f56fc380e43f4d"
	walV2StorageDescriptorSHA256 = "fb8563bc72daf6d1996ae5b00e5315dece42619b407ab1160f21aa364d110ec5"
	// Legacy pins are intentionally separate from the current pins. If the
	// reviewed descriptors ever change, old unbound journals must fail closed
	// instead of being silently attributed to the new descriptors.
	walV2LegacyGeyserDescriptorSHA256  = "20146a8e752395fa29404cb7577b1455626aa6f99ddab8d663f56fc380e43f4d"
	walV2LegacyStorageDescriptorSHA256 = "fb8563bc72daf6d1996ae5b00e5315dece42619b407ab1160f21aa364d110ec5"
)

type walV2Header struct {
	Version                 int    `json:"version"`
	Segment                 uint64 `json:"segment"`
	Source                  string `json:"source"`
	Identity                string `json:"identity"`
	JournalID               string `json:"journal_id"`
	FirstSequence           uint64 `json:"first_sequence"`
	EnvelopeSchemaVersion   int    `json:"envelope_schema_version,omitempty"`
	GeyserDescriptorSHA256  string `json:"geyser_descriptor_sha256,omitempty"`
	StorageDescriptorSHA256 string `json:"storage_descriptor_sha256,omitempty"`
}

type walV2SegmentMeta struct {
	ID            uint64 `json:"id"`
	File          string `json:"file"`
	FirstSequence uint64 `json:"first_sequence"`
	LastSequence  uint64 `json:"last_sequence"`
	Records       uint64 `json:"records"`
	DataBytes     int64  `json:"data_bytes"`
	Digest        string `json:"digest"`
}

type walV2ActiveMeta struct {
	ID             uint64 `json:"id"`
	FirstSequence  uint64 `json:"first_sequence"`
	LastSequence   uint64 `json:"last_sequence"`
	Records        uint64 `json:"records"`
	CommittedBytes int64  `json:"committed_bytes"`
}

type walV2Manifest struct {
	Version                 int                `json:"version"`
	Generation              uint64             `json:"generation"`
	Source                  string             `json:"source"`
	Identity                string             `json:"identity"`
	JournalID               string             `json:"journal_id"`
	EnvelopeSchemaVersion   int                `json:"envelope_schema_version,omitempty"`
	GeyserDescriptorSHA256  string             `json:"geyser_descriptor_sha256,omitempty"`
	StorageDescriptorSHA256 string             `json:"storage_descriptor_sha256,omitempty"`
	Sealed                  []walV2SegmentMeta `json:"sealed,omitempty"`
	Active                  walV2ActiveMeta    `json:"active"`
	Checksum                string             `json:"checksum"`
}

type walV2Footer struct {
	Segment       uint64
	FirstSequence uint64
	LastSequence  uint64
	Records       uint64
	DataBytes     uint64
	Digest        [sha256.Size]byte
}

type walV2Segment struct {
	meta        walV2SegmentMeta
	headerBytes int64
	index       []walIndex
	indexCached bool
}

type segmentedWAL struct {
	mu              sync.Mutex
	dir             string
	lock            *os.File
	active          *os.File
	manifest        walV2Manifest
	sealed          []walV2Segment
	activeHeader    walV2Header
	activeHeaderLen int64
	activeIndex     []walIndex
	maxSegmentBytes int64
	// manifestWriter is an instance-local fault seam for durability tests.
	manifestWriter func(string, walV2Manifest) error
	recoveredTail  bool
	closed         bool
	failure        error
	lastSync       time.Duration
	totalSync      time.Duration
	maxSync        time.Duration
	syncCount      uint64
	syncBuckets    [8]uint64
}

type segmentedWALReader struct {
	lock   *os.File
	dir    string
	sealed []walV2Segment
	active walV2Segment
	last   uint64
}

// walV2CurrentDescriptorBinding verifies that the generated descriptors in
// this binary still match the reviewed schema pins. Journals bind both files
// because Record.Raw contains Geyser messages and downstream storage decoding
// uses the storage descriptor as part of the same ingestion contract.
func walV2CurrentDescriptorBinding() (string, string, error) {
	walV2DescriptorBindingOnce.Do(func() {
		geyserBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(protodesc.ToFileDescriptorProto(pb.File_solanacore_geyser_proto))
		if err != nil {
			walV2DescriptorBindingErr = fmt.Errorf("WAL v2 marshal local Geyser descriptor: %w", err)
			return
		}
		storageBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(protodesc.ToFileDescriptorProto(pb.File_solanacore_storage_proto))
		if err != nil {
			walV2DescriptorBindingErr = fmt.Errorf("WAL v2 marshal local storage descriptor: %w", err)
			return
		}
		geyserDigest := fmt.Sprintf("%x", sha256.Sum256(geyserBytes))
		storageDigest := fmt.Sprintf("%x", sha256.Sum256(storageBytes))
		if geyserDigest != walV2GeyserDescriptorSHA256 || storageDigest != walV2StorageDescriptorSHA256 {
			walV2DescriptorBindingErr = fmt.Errorf("WAL v2 local protobuf descriptors differ from reviewed pins: geyser=%s storage=%s", geyserDigest, storageDigest)
			return
		}
		walV2DescriptorBindingGeyser = geyserDigest
		walV2DescriptorBindingStorage = storageDigest
	})
	return walV2DescriptorBindingGeyser, walV2DescriptorBindingStorage, walV2DescriptorBindingErr
}

var (
	walV2DescriptorBindingOnce    sync.Once
	walV2DescriptorBindingGeyser  string
	walV2DescriptorBindingStorage string
	walV2DescriptorBindingErr     error
)

func normalizeWALV2Envelope(schemaVersion *int, geyserDigest, storageDigest *string) error {
	// Pre-freeze v2 journals had no explicit envelope marker. Their bytes were
	// written by this repository's pinned descriptors, so the all-absent form
	// maps to the sole supported schema. A partial binding is ambiguous and is
	// rejected rather than guessed.
	if *schemaVersion == 0 && *geyserDigest == "" && *storageDigest == "" {
		*schemaVersion = walV2EnvelopeSchemaVersion
		*geyserDigest = walV2LegacyGeyserDescriptorSHA256
		*storageDigest = walV2LegacyStorageDescriptorSHA256
	}
	if *schemaVersion != walV2EnvelopeSchemaVersion {
		return fmt.Errorf("unsupported WAL v2 envelope schema version %d", *schemaVersion)
	}
	if *geyserDigest == "" || *storageDigest == "" {
		return errors.New("WAL v2 envelope descriptor binding is incomplete")
	}
	localGeyser, localStorage, err := walV2CurrentDescriptorBinding()
	if err != nil {
		return err
	}
	if *geyserDigest != localGeyser || *storageDigest != localStorage {
		return fmt.Errorf("WAL v2 protobuf descriptor mismatch: journal geyser=%s storage=%s, local geyser=%s storage=%s", *geyserDigest, *storageDigest, localGeyser, localStorage)
	}
	return nil
}

func normalizeWALV2HeaderEnvelope(header *walV2Header) error {
	return normalizeWALV2Envelope(&header.EnvelopeSchemaVersion, &header.GeyserDescriptorSHA256, &header.StorageDescriptorSHA256)
}

func normalizeWALV2ManifestEnvelope(manifest *walV2Manifest) error {
	return normalizeWALV2Envelope(&manifest.EnvelopeSchemaVersion, &manifest.GeyserDescriptorSHA256, &manifest.StorageDescriptorSHA256)
}

// OpenWALV2 opens an opt-in segmented journal. Version 1 remains readable via
// OpenWAL; this format uses immutable sealed segments and an atomically
// published manifest. The caller owns source/identity and supplies a visitor
// to rebuild indexes before the writer is returned.
func OpenWALV2(path, source, identity string, maxSegmentBytes int64, visit func(Record) error) (*WAL, error) {
	if strings.TrimSpace(path) == "" || strings.TrimSpace(identity) == "" {
		return nil, errors.New("WAL v2 requires a path and subscription identity")
	}
	if maxSegmentBytes <= 0 {
		maxSegmentBytes = walV2DefaultSegmentBytes
	}
	if maxSegmentBytes < 2<<20 || maxSegmentBytes > 4<<30 {
		return nil, errors.New("WAL v2 segment size must be between 2 MiB and 4 GiB")
	}
	if err := os.MkdirAll(filepath.Join(path, "segments"), 0700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(path, "manifests"), 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(path, "LOCK"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := lockWALWriter(lock); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("WAL v2 already has a writer: %w", err)
	}
	w := &segmentedWAL{dir: path, lock: lock, maxSegmentBytes: maxSegmentBytes}
	if err := w.open(source, identity, visit); err != nil {
		_ = w.closeLocked()
		return nil, err
	}
	return &WAL{
		path:          path,
		v2:            w,
		journalID:     w.manifest.JournalID,
		RecoveredTail: w.recoveredTail,
	}, nil
}

func (w *segmentedWAL) open(source, identity string, visit func(Record) error) error {
	manifest, exists, err := readWALV2Current(w.dir)
	if err != nil {
		return err
	}
	if !exists {
		// The first segment header is fsynced before the initial CURRENT is
		// published. A crash in that window leaves an uncommitted, header-only
		// journal whose identity can be preserved without discarding data.
		bootstrap, recoverable, err := recoverWALV2BootstrapWithoutCurrent(w.dir, source, identity)
		if err != nil {
			return err
		}
		if recoverable {
			if err := w.persistManifest(bootstrap); err != nil {
				return err
			}
			w.manifest = bootstrap
			return w.recoverActive()
		}
		if err := ensureWALV2DirectoryEmpty(w.dir); err != nil {
			return err
		}
		journalID, err := newJournalID()
		if err != nil {
			return err
		}
		header := walV2Header{Version: walV2Version, Segment: 1, Source: source, Identity: identity, JournalID: journalID, FirstSequence: 1}
		if err := normalizeWALV2HeaderEnvelope(&header); err != nil {
			return err
		}
		headerBytes, err := encodeWALV2Header(header)
		if err != nil {
			return err
		}
		activePath := walV2SegmentPath(w.dir, 1, false)
		f, err := os.OpenFile(activePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if err != nil {
			return err
		}
		if err := writeWALV2All(f, headerBytes); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return err
		}
		if err := syncWALV2Dir(filepath.Join(w.dir, "segments")); err != nil {
			_ = f.Close()
			return err
		}
		w.active = f
		w.activeHeader = header
		w.activeHeaderLen = int64(len(headerBytes))
		w.manifest = walV2Manifest{
			Version: walV2Version, Generation: 1, Source: source, Identity: identity, JournalID: journalID,
			EnvelopeSchemaVersion: header.EnvelopeSchemaVersion, GeyserDescriptorSHA256: header.GeyserDescriptorSHA256, StorageDescriptorSHA256: header.StorageDescriptorSHA256,
			Active: walV2ActiveMeta{ID: 1, FirstSequence: 1, CommittedBytes: int64(len(headerBytes))},
		}
		return w.persistManifest(w.manifest)
	}
	if manifest.Source != source || manifest.Identity != identity {
		return errors.New("WAL v2 source or subscription identity differs from configuration")
	}
	w.manifest = manifest
	// Recovery may truncate an uncommitted active tail or repair an empty
	// header. Verify the already committed sealed chain before any such write.
	if err := w.verifySealedChain(); err != nil {
		return err
	}
	if err := w.recoverActive(); err != nil {
		return err
	}
	if err := w.validateAndVisit(visit); err != nil {
		return err
	}
	for _, segment := range w.sealed {
		if !segment.indexCached {
			// Cache publication is best effort and never changes committed data.
			_ = saveWALV2Index(w.dir, segment.meta, segment.headerBytes, segment.index)
		}
	}
	// Old manifests are no longer needed after a verified reopen. Keep a
	// short diagnostic history; recovery still uses only CURRENT and never
	// silently falls back to an older generation.
	_ = pruneWALV2Manifests(w.dir, w.manifest.Generation)
	return nil
}

func (w *segmentedWAL) verifySealedChain() error {
	nextSegment, nextSequence := uint64(1), uint64(1)
	verified := make([]walV2Segment, 0, len(w.manifest.Sealed))
	for _, meta := range w.manifest.Sealed {
		if meta.ID != nextSegment || meta.FirstSequence != nextSequence ||
			meta.Records == 0 || meta.LastSequence < meta.FirstSequence ||
			meta.LastSequence-meta.FirstSequence+1 != meta.Records {
			return errors.New("WAL v2 sealed segment sequence/ID chain is invalid")
		}
		segment, err := openAndVerifyWALV2Sealed(w.dir, meta, w.manifest, nil)
		if err != nil {
			return fmt.Errorf("WAL v2 sealed segment %d: %w", meta.ID, err)
		}
		verified = append(verified, segment)
		if meta.LastSequence == ^uint64(0) || nextSegment == ^uint64(0) {
			return errors.New("WAL v2 sealed segment sequence/ID exhausted")
		}
		nextSegment, nextSequence = nextSegment+1, meta.LastSequence+1
	}
	if w.manifest.Active.ID != nextSegment || w.manifest.Active.FirstSequence != nextSequence {
		return errors.New("WAL v2 active segment does not follow sealed chain")
	}
	w.sealed = verified
	return nil
}

func (w *segmentedWAL) recoverActive() error {
	active := w.manifest.Active
	openPath := walV2SegmentPath(w.dir, active.ID, false)
	sealedPath := walV2SegmentPath(w.dir, active.ID, true)
	_, openErr := os.Stat(openPath)
	_, sealedErr := os.Stat(sealedPath)
	openExists := openErr == nil
	sealedExists := sealedErr == nil
	if openErr != nil && !errors.Is(openErr, os.ErrNotExist) {
		return openErr
	}
	if sealedErr != nil && !errors.Is(sealedErr, os.ErrNotExist) {
		return sealedErr
	}
	if openExists && sealedExists {
		return errors.New("WAL v2 active segment exists in both open and sealed form")
	}
	if sealedExists {
		meta, index, err := verifyWALV2Sealed(w.dir, active, w.manifest)
		if err != nil {
			return fmt.Errorf("WAL v2 interrupted rotation: %w", err)
		}
		w.sealed = append(w.sealed, walV2Segment{meta: meta, headerBytes: indexHeaderBytes(w.dir, meta.ID), index: index})
		nextID := active.ID + 1
		if nextID == 0 {
			return errors.New("WAL v2 segment ID exhausted")
		}
		nextFirst := active.LastSequence + 1
		newHeader := walV2Header{Version: walV2Version, Segment: nextID, Source: w.manifest.Source, Identity: w.manifest.Identity, JournalID: w.manifest.JournalID, FirstSequence: nextFirst}
		headerBytes, err := encodeWALV2Header(newHeader)
		if err != nil {
			return err
		}
		w.manifest.Sealed = append(w.manifest.Sealed, meta)
		w.manifest.Active = walV2ActiveMeta{ID: nextID, FirstSequence: nextFirst, LastSequence: nextFirst - 1, CommittedBytes: int64(len(headerBytes))}
		w.manifest.Generation++
		if err := w.persistManifest(w.manifest); err != nil {
			return err
		}
		return w.createActive(newHeader, headerBytes)
	}
	if !openExists {
		if active.Records != 0 {
			return errors.New("WAL v2 committed active segment is missing")
		}
		header := walV2Header{Version: walV2Version, Segment: active.ID, Source: w.manifest.Source, Identity: w.manifest.Identity, JournalID: w.manifest.JournalID, FirstSequence: active.FirstSequence}
		headerBytes, err := encodeWALV2Header(header)
		if err != nil {
			return err
		}
		if int64(len(headerBytes)) != active.CommittedBytes {
			legacyBytes, legacyErr := encodeWALV2LegacyHeader(header)
			if legacyErr != nil {
				return legacyErr
			}
			if int64(len(legacyBytes)) != active.CommittedBytes {
				return errors.New("WAL v2 active manifest header size mismatch")
			}
			headerBytes = legacyBytes
		}
		return w.createActive(header, headerBytes)
	}
	f, err := os.OpenFile(openPath, os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	// Rotation publishes CURRENT before creating the next empty .open. A
	// crash while writing its header can therefore leave a short file. Only
	// an exact prefix of the expected header is safe to finish in place.
	if active.Records == 0 {
		expectedHeader := walV2Header{Version: walV2Version, Segment: active.ID, Source: w.manifest.Source, Identity: w.manifest.Identity, JournalID: w.manifest.JournalID, FirstSequence: active.FirstSequence}
		expectedBytes, encodeErr := encodeWALV2Header(expectedHeader)
		if encodeErr != nil {
			_ = f.Close()
			return encodeErr
		}
		if active.CommittedBytes != int64(len(expectedBytes)) {
			legacyBytes, legacyErr := encodeWALV2LegacyHeader(expectedHeader)
			if legacyErr != nil {
				_ = f.Close()
				return legacyErr
			}
			if active.CommittedBytes != int64(len(legacyBytes)) {
				_ = f.Close()
				return errors.New("WAL v2 empty active manifest header size mismatch")
			}
			expectedBytes = legacyBytes
		}
		stat, statErr := f.Stat()
		if statErr != nil {
			_ = f.Close()
			return statErr
		}
		if stat.Size() < int64(len(expectedBytes)) {
			partial := make([]byte, stat.Size())
			n, err := f.ReadAt(partial, 0)
			if err != nil && !errors.Is(err, io.EOF) {
				_ = f.Close()
				return err
			}
			if n != len(partial) {
				_ = f.Close()
				return io.ErrUnexpectedEOF
			}
			if !bytes.Equal(partial, expectedBytes[:len(partial)]) {
				_ = f.Close()
				return errors.New("WAL v2 short empty active header is not an expected prefix")
			}
			if _, err := f.WriteAt(expectedBytes, 0); err != nil {
				_ = f.Close()
				return fmt.Errorf("WAL v2 repair short empty active header: %w", err)
			}
			if err := f.Sync(); err != nil {
				_ = f.Close()
				return fmt.Errorf("WAL v2 sync repaired active header: %w", err)
			}
			w.recoveredTail = true
		}
	}
	header, headerLen, err := readWALV2Header(f)
	if err != nil {
		_ = f.Close()
		return err
	}
	if err := validateWALV2Header(header, w.manifest, active.ID, active.FirstSequence); err != nil {
		_ = f.Close()
		return err
	}
	stat, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	if stat.Size() < active.CommittedBytes || active.CommittedBytes < headerLen {
		_ = f.Close()
		return errors.New("WAL v2 active segment is shorter than its committed manifest offset")
	}
	index, count, last, err := scanWALV2Data(f, header, headerLen, active.CommittedBytes, active.FirstSequence, nil)
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("WAL v2 active segment: %w", err)
	}
	if count != active.Records || last != active.LastSequence {
		_ = f.Close()
		return errors.New("WAL v2 active segment does not match committed manifest sequence")
	}
	// A rejected recovery must preserve the file for diagnosis. Validate the
	// entire committed prefix before removing any uncommitted bytes.
	if stat.Size() > active.CommittedBytes {
		if err := f.Truncate(active.CommittedBytes); err != nil {
			_ = f.Close()
			return fmt.Errorf("WAL v2 truncate uncommitted active tail: %w", err)
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return fmt.Errorf("WAL v2 sync active tail recovery: %w", err)
		}
		w.recoveredTail = true
	}
	if _, err := f.Seek(active.CommittedBytes, io.SeekStart); err != nil {
		_ = f.Close()
		return err
	}
	w.active = f
	w.activeHeader = header
	w.activeHeaderLen = headerLen
	w.activeIndex = index
	return nil
}

func (w *segmentedWAL) createActive(header walV2Header, headerBytes []byte) error {
	if err := normalizeWALV2HeaderEnvelope(&header); err != nil {
		return err
	}
	path := walV2SegmentPath(w.dir, header.Segment, false)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	if err := writeWALV2All(f, headerBytes); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := syncWALV2Dir(filepath.Join(w.dir, "segments")); err != nil {
		_ = f.Close()
		return err
	}
	w.active = f
	w.activeHeader = header
	w.activeHeaderLen = int64(len(headerBytes))
	w.activeIndex = nil
	return nil
}

func (w *segmentedWAL) validateAndVisit(visit func(Record) error) error {
	expectedSequence := uint64(1)
	expectedSegment := uint64(1)
	for i, meta := range w.manifest.Sealed {
		if meta.ID != expectedSegment || meta.FirstSequence != expectedSequence || meta.Records == 0 || meta.LastSequence < meta.FirstSequence {
			return errors.New("WAL v2 sealed segment sequence/ID chain is invalid")
		}
		if i >= len(w.sealed) || w.sealed[i].meta != meta {
			segment, err := openAndVerifyWALV2Sealed(w.dir, meta, w.manifest, nil)
			if err != nil {
				return fmt.Errorf("WAL v2 sealed segment %d: %w", meta.ID, err)
			}
			if i < len(w.sealed) {
				w.sealed[i] = segment
			} else {
				w.sealed = append(w.sealed, segment)
			}
		}
		expectedSequence = meta.LastSequence + 1
		expectedSegment++
	}
	active := w.manifest.Active
	if active.ID != expectedSegment || active.FirstSequence != expectedSequence || active.LastSequence != active.FirstSequence+active.Records-1 {
		if active.Records == 0 && active.LastSequence == active.FirstSequence-1 && active.ID == expectedSegment && active.FirstSequence == expectedSequence {
			// Empty active segment follows the sealed prefix.
		} else {
			return errors.New("WAL v2 active segment sequence/ID chain is invalid")
		}
	}
	index, count, last, err := scanWALV2Data(w.active, w.activeHeader, w.activeHeaderLen, active.CommittedBytes, active.FirstSequence, nil)
	if err != nil {
		return fmt.Errorf("WAL v2 active segment replay: %w", err)
	}
	if count != active.Records || last != active.LastSequence {
		return errors.New("WAL v2 active segment replay does not match manifest")
	}
	w.activeIndex = index
	// Publish records to a rebuild visitor only after the entire committed
	// chain has passed integrity and sequence validation. This keeps failed
	// opens from leaking a verified prefix into an external state builder.
	if visit != nil {
		for _, segment := range w.sealed {
			if err := replayVerifiedWALV2Sealed(w.dir, segment, w.manifest, visit); err != nil {
				return fmt.Errorf("WAL v2 sealed segment %d replay: %w", segment.meta.ID, err)
			}
		}
		if _, _, _, err := scanWALV2Data(w.active, w.activeHeader, w.activeHeaderLen, active.CommittedBytes, active.FirstSequence, visit); err != nil {
			return fmt.Errorf("WAL v2 active segment replay: %w", err)
		}
	}
	return nil
}

// ErrWALV2CommitUncertain indicates a terminal writer error after the CURRENT
// pointer may have advanced. A failed append may be visible after reopening;
// callers must reconcile by journal sequence before retrying it.
var ErrWALV2CommitUncertain = errors.New("WAL v2 commit outcome is uncertain; reopen the journal before retry")

func (w *segmentedWAL) persistManifest(manifest walV2Manifest) error {
	if err := normalizeWALV2ManifestEnvelope(&manifest); err != nil {
		return err
	}
	if w.manifestWriter != nil {
		return w.manifestWriter(w.dir, manifest)
	}
	if err := writeWALV2Manifest(w.dir, manifest); err != nil {
		return err
	}
	// Cleanup is best-effort: a failed deletion must never turn a committed
	// batch into an ambiguous write outcome. Reopen retries cleanup.
	_ = pruneWALV2Manifests(w.dir, manifest.Generation)
	return nil
}

// pruneWALV2Manifests retains CURRENT and two predecessor generations. Only
// files with the canonical generation naming scheme are eligible; temporary
// files and unexpected names are left for explicit operator inspection.
func pruneWALV2Manifests(dir string, current uint64) error {
	if current <= 3 {
		return nil
	}
	manifestDir := filepath.Join(dir, "manifests")
	entries, err := os.ReadDir(manifestDir)
	if err != nil {
		return err
	}
	var removed bool
	for _, entry := range entries {
		name := entry.Name()
		if !entry.Type().IsRegular() || !strings.HasPrefix(name, "manifest-") || !strings.HasSuffix(name, ".json") {
			continue
		}
		generationText := strings.TrimSuffix(strings.TrimPrefix(name, "manifest-"), ".json")
		generation, err := strconv.ParseUint(generationText, 10, 64)
		if err != nil || name != walV2ManifestFileName(generation) || generation >= current-2 {
			continue
		}
		if err := os.Remove(filepath.Join(manifestDir, name)); err != nil {
			return err
		}
		removed = true
	}
	if removed {
		return syncWALV2Dir(manifestDir)
	}
	return nil
}

// The manifest and CURRENT are published in separate atomic writes. A failure
// after CURRENT rename can leave either generation durable across a crash. Only
// an observed old pointer proves it is safe to truncate the proposed batch.
func walV2CurrentMayHaveAdvanced(dir string, previous, candidate uint64) (bool, error) {
	pointer, err := os.ReadFile(filepath.Join(dir, "CURRENT"))
	if err != nil {
		return true, fmt.Errorf("WAL v2 cannot inspect CURRENT after commit error: %w", err)
	}
	generation, err := strconv.ParseUint(strings.TrimSpace(string(pointer)), 10, 64)
	if err != nil || generation == 0 {
		return true, errors.New("WAL v2 CURRENT is invalid after commit error")
	}
	if generation == previous {
		return false, nil
	}
	if generation == candidate {
		return true, nil
	}
	return true, fmt.Errorf("WAL v2 CURRENT unexpectedly references generation %d after attempted generation %d", generation, candidate)
}

func walV2BatchBytes(frames [][]byte) (int64, error) {
	var total int64
	for _, frame := range frames {
		if int64(len(frame)) > int64(^uint64(0)>>1)-total {
			return 0, errors.New("WAL v2 encoded batch byte count overflow")
		}
		total += int64(len(frame))
	}
	return total, nil
}

func walV2DataLimit(segmentBytes int64) int64 {
	// Every active segment can eventually be sealed. Reserve the footer now
	// so an on-disk .seg never exceeds the configured physical segment limit.
	return segmentBytes - walV2FooterBytes
}

func (w *segmentedWAL) appendBatch(records []Record) error {
	if len(records) == 0 {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrWALClosed
	}
	if w.failure != nil {
		return w.failure
	}
	if w.active == nil {
		return ErrWALClosed
	}
	frames, encodedRecords, err := encodeWALV2BatchBounded(records, w.manifest, walV2DataLimit(w.maxSegmentBytes))
	if err != nil {
		return err
	}
	batchBytes, err := walV2BatchBytes(frames)
	if err != nil {
		return err
	}
	dataLimit := walV2DataLimit(w.maxSegmentBytes)
	if batchBytes > dataLimit-w.manifest.Active.CommittedBytes {
		if w.manifest.Active.Records == 0 {
			return fmt.Errorf("WAL v2 batch size %d exceeds empty segment capacity %d", batchBytes, dataLimit-w.activeHeaderLen)
		}
		// Preflight the exact next header before sealing the current segment.
		// A too-large batch is a validation error and must not rotate or poison
		// an otherwise writable journal.
		nextID := w.manifest.Active.ID + 1
		if nextID == 0 {
			return errors.New("WAL v2 segment ID exhausted")
		}
		nextFirst := w.manifest.Active.LastSequence + 1
		nextHeader := walV2Header{Version: walV2Version, Segment: nextID, Source: w.manifest.Source, Identity: w.manifest.Identity, JournalID: w.manifest.JournalID, FirstSequence: nextFirst}
		nextHeaderBytes, err := encodeWALV2Header(nextHeader)
		if err != nil {
			return err
		}
		if batchBytes > dataLimit-int64(len(nextHeaderBytes)) {
			return fmt.Errorf("WAL v2 batch size %d exceeds next segment capacity %d", batchBytes, dataLimit-int64(len(nextHeaderBytes)))
		}
		if err := w.rotateLocked(); err != nil {
			w.failure = err
			return err
		}
	}
	oldOffset := w.manifest.Active.CommittedBytes
	if _, err := w.active.Seek(oldOffset, io.SeekStart); err != nil {
		w.failure = err
		return err
	}
	rollback := func(cause error) error {
		truncateErr := w.active.Truncate(oldOffset)
		_, seekErr := w.active.Seek(oldOffset, io.SeekStart)
		syncErr := w.active.Sync()
		if truncateErr != nil || seekErr != nil || syncErr != nil {
			return errors.Join(cause, fmt.Errorf("WAL v2 batch rollback failed: %w", errors.Join(truncateErr, seekErr, syncErr)))
		}
		return cause
	}
	for _, frame := range frames {
		if err := writeWALV2All(w.active, frame); err != nil {
			w.failure = rollback(err)
			return w.failure
		}
	}
	syncStarted := time.Now()
	syncErr := w.active.Sync()
	w.recordSync(time.Since(syncStarted))
	if syncErr != nil {
		w.failure = rollback(syncErr)
		return w.failure
	}
	newActive := w.manifest.Active
	newActive.CommittedBytes += batchBytes
	newActive.Records += uint64(len(encodedRecords))
	newActive.LastSequence = encodedRecords[len(encodedRecords)-1].Sequence
	newManifest := cloneWALV2Manifest(w.manifest)
	newManifest.Generation++
	newManifest.Active = newActive
	if err := w.persistManifest(newManifest); err != nil {
		mayHavePublished, inspectErr := walV2CurrentMayHaveAdvanced(w.dir, w.manifest.Generation, newManifest.Generation)
		if mayHavePublished {
			// CURRENT may already point to this fsynced data. Never truncate it:
			// startup will select the surviving generation and recover its tail.
			w.failure = errors.Join(ErrWALV2CommitUncertain, err, inspectErr)
			return w.failure
		}
		w.failure = rollback(err)
		return w.failure
	}
	for i, record := range encodedRecords {
		sequence := record.Sequence
		if (sequence-w.manifest.Active.FirstSequence)%walIndexStride == 0 {
			w.activeIndex = append(w.activeIndex, walIndex{Sequence: sequence, Offset: oldOffset + frameOffset(frames, i)})
		}
	}
	w.manifest = newManifest
	return nil
}

func (w *segmentedWAL) rotateLocked() error {
	active := w.manifest.Active
	digest, err := hashWALV2Prefix(w.active, active.CommittedBytes)
	if err != nil {
		return err
	}
	footer := walV2Footer{Segment: active.ID, FirstSequence: active.FirstSequence, LastSequence: active.LastSequence, Records: active.Records, DataBytes: uint64(active.CommittedBytes), Digest: digest}
	footerBytes := encodeWALV2Footer(footer)
	if _, err := w.active.Seek(active.CommittedBytes, io.SeekStart); err != nil {
		return err
	}
	if err := writeWALV2All(w.active, footerBytes); err != nil {
		return err
	}
	syncStarted := time.Now()
	err = w.active.Sync()
	w.recordSync(time.Since(syncStarted))
	if err != nil {
		return err
	}
	if err := w.active.Close(); err != nil {
		w.active = nil
		return err
	}
	w.active = nil
	if err := os.Rename(walV2SegmentPath(w.dir, active.ID, false), walV2SegmentPath(w.dir, active.ID, true)); err != nil {
		return err
	}
	if err := syncWALV2Dir(filepath.Join(w.dir, "segments")); err != nil {
		return err
	}
	sealed := walV2SegmentMeta{
		ID: active.ID, File: walV2SegmentFileName(active.ID, true), FirstSequence: active.FirstSequence,
		LastSequence: active.LastSequence, Records: active.Records, DataBytes: active.CommittedBytes, Digest: hex.EncodeToString(digest[:]),
	}
	nextID := active.ID + 1
	if nextID == 0 {
		return errors.New("WAL v2 segment ID exhausted")
	}
	nextFirst := active.LastSequence + 1
	nextHeader := walV2Header{Version: walV2Version, Segment: nextID, Source: w.manifest.Source, Identity: w.manifest.Identity, JournalID: w.manifest.JournalID, FirstSequence: nextFirst}
	nextHeaderBytes, err := encodeWALV2Header(nextHeader)
	if err != nil {
		return err
	}
	newManifest := cloneWALV2Manifest(w.manifest)
	newManifest.Generation++
	newManifest.Sealed = append(newManifest.Sealed, sealed)
	newManifest.Active = walV2ActiveMeta{ID: nextID, FirstSequence: nextFirst, LastSequence: nextFirst - 1, CommittedBytes: int64(len(nextHeaderBytes))}
	if err := w.persistManifest(newManifest); err != nil {
		mayHavePublished, inspectErr := walV2CurrentMayHaveAdvanced(w.dir, w.manifest.Generation, newManifest.Generation)
		if mayHavePublished {
			return errors.Join(ErrWALV2CommitUncertain, err, inspectErr)
		}
		return err
	}
	w.manifest = newManifest
	w.sealed = append(w.sealed, walV2Segment{meta: sealed, headerBytes: w.activeHeaderLen, index: append([]walIndex(nil), w.activeIndex...)})
	return w.createActive(nextHeader, nextHeaderBytes)
}

func (w *segmentedWAL) readAfter(sequence uint64, limit int) ([]Record, error) {
	if limit <= 0 {
		limit = 128
	}
	if limit > maxWALReadBatchRecords {
		limit = maxWALReadBatchRecords
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil, ErrWALClosed
	}
	if w.failure != nil {
		return nil, w.failure
	}
	if w.active == nil {
		return nil, ErrWALClosed
	}
	last := w.manifest.Active.LastSequence
	if sequence > last {
		return nil, fmt.Errorf("WAL v2 cursor %d exceeds record count %d", sequence, last)
	}
	return readWALV2After(w.dir, w.sealed, walV2Segment{meta: walV2SegmentMeta{
		ID: w.manifest.Active.ID, FirstSequence: w.manifest.Active.FirstSequence, LastSequence: w.manifest.Active.LastSequence,
		Records: w.manifest.Active.Records, DataBytes: w.manifest.Active.CommittedBytes,
	}, headerBytes: w.activeHeaderLen, index: w.activeIndex}, w.active, sequence, limit)
}

func (w *segmentedWAL) count() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.manifest.Active.LastSequence
}

func (w *segmentedWAL) syncStats() (last, total, max time.Duration, count uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastSync, w.totalSync, w.maxSync, w.syncCount
}

func (w *segmentedWAL) syncHistogram() [8]uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.syncBuckets
}

func (w *segmentedWAL) recordSync(duration time.Duration) {
	w.lastSync = duration
	w.totalSync += duration
	w.syncCount++
	thresholds := [...]time.Duration{time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond, 25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 500 * time.Millisecond}
	bucket := len(thresholds)
	for i, threshold := range thresholds {
		if duration <= threshold {
			bucket = i
			break
		}
	}
	w.syncBuckets[bucket]++
	if duration > w.maxSync {
		w.maxSync = duration
	}
}

func (w *segmentedWAL) close() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closeLocked()
}

func (w *segmentedWAL) closeLocked() error {
	if w.closed {
		return nil
	}
	w.closed = true
	var activeErr, lockErr error
	if w.active != nil {
		activeErr = w.active.Close()
		w.active = nil
	}
	if w.lock != nil {
		lockErr = w.lock.Close()
		w.lock = nil
	}
	return errors.Join(activeErr, lockErr)
}

func openWALV2Reader(path string) (*segmentedWALReader, error) {
	lock, err := os.OpenFile(filepath.Join(path, "LOCK"), os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	if err := lockWALReader(lock); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("WAL v2 has an active writer: %w", err)
	}
	manifest, exists, err := readWALV2Current(path)
	if err != nil || !exists {
		_ = lock.Close()
		if err != nil {
			return nil, err
		}
		return nil, errors.New("WAL v2 CURRENT manifest is missing")
	}
	reader := &segmentedWALReader{lock: lock, dir: path, last: manifest.Active.LastSequence}
	for _, meta := range manifest.Sealed {
		segment, err := openAndVerifyWALV2Sealed(path, meta, manifest, nil)
		if err != nil {
			_ = lock.Close()
			return nil, fmt.Errorf("WAL v2 sealed segment %d: %w", meta.ID, err)
		}
		reader.sealed = append(reader.sealed, segment)
	}
	activePath := walV2SegmentPath(path, manifest.Active.ID, false)
	f, err := os.Open(activePath)
	if err != nil {
		_ = lock.Close()
		return nil, err
	}
	header, headerBytes, err := readWALV2Header(f)
	if err != nil {
		_ = f.Close()
		_ = lock.Close()
		return nil, err
	}
	if err := validateWALV2Header(header, manifest, manifest.Active.ID, manifest.Active.FirstSequence); err != nil {
		_ = f.Close()
		_ = lock.Close()
		return nil, err
	}
	index, count, last, err := scanWALV2Data(f, header, headerBytes, manifest.Active.CommittedBytes, manifest.Active.FirstSequence, nil)
	_ = f.Close()
	if err != nil || count != manifest.Active.Records || last != manifest.Active.LastSequence {
		_ = lock.Close()
		if err != nil {
			return nil, err
		}
		return nil, errors.New("WAL v2 active segment does not match manifest")
	}
	reader.active = walV2Segment{meta: walV2SegmentMeta{
		ID: manifest.Active.ID, FirstSequence: manifest.Active.FirstSequence, LastSequence: manifest.Active.LastSequence,
		Records: manifest.Active.Records, DataBytes: manifest.Active.CommittedBytes,
	}, headerBytes: headerBytes, index: index}
	return reader, nil
}

func (r *segmentedWALReader) close() error {
	if r == nil || r.lock == nil {
		return nil
	}
	err := r.lock.Close()
	r.lock = nil
	return err
}

func (r *segmentedWALReader) readAfter(sequence uint64, limit int) ([]Record, error) {
	if limit <= 0 {
		limit = 128
	}
	if limit > maxWALReadBatchRecords {
		limit = maxWALReadBatchRecords
	}
	if sequence > r.last {
		return nil, fmt.Errorf("WAL v2 cursor %d exceeds record count %d", sequence, r.last)
	}
	return readWALV2After(r.dir, r.sealed, r.active, nil, sequence, limit)
}

func readWALV2After(dir string, sealed []walV2Segment, active walV2Segment, activeFile *os.File, sequence uint64, limit int) ([]Record, error) {
	results := make([]Record, 0, limit)
	var readBytes int64
	// Sealed ranges are ordered and disjoint. Seek straight to the first
	// matching segment without copying or walking the entire history on every
	// page (including tail polling when there are no new records).
	start := sort.Search(len(sealed), func(i int) bool { return sealed[i].meta.LastSequence > sequence })
	for i := start; i <= len(sealed); i++ {
		segment := active
		if i < len(sealed) {
			segment = sealed[i]
		}
		if segment.meta.LastSequence <= sequence || len(results) >= limit {
			continue
		}
		var file *os.File
		if segment.meta.ID == active.meta.ID && activeFile != nil {
			file = activeFile
		} else if segment.meta.File == "" {
			var err error
			file, err = os.Open(walV2SegmentPath(dir, segment.meta.ID, false))
			if err != nil {
				return results, err
			}
		} else {
			var err error
			file, err = os.Open(filepath.Join(dir, "segments", segment.meta.File))
			if err != nil {
				return results, err
			}
		}
		batch, used, err := readWALV2SegmentAfter(file, segment, sequence, limit-len(results), maxWALReadBatchBytes-readBytes)
		if activeFile == nil || segment.meta.ID != active.meta.ID {
			_ = file.Close()
		}
		if err != nil {
			if len(results) > 0 {
				return results, nil
			}
			return nil, err
		}
		results = append(results, batch...)
		readBytes += used
		if len(results) >= limit || readBytes >= maxWALReadBatchBytes {
			break
		}
	}
	return results, nil
}

func readWALV2SegmentAfter(file *os.File, segment walV2Segment, after uint64, limit int, maxBytes int64) ([]Record, int64, error) {
	if limit <= 0 || maxBytes <= 0 || segment.meta.LastSequence <= after {
		return nil, 0, nil
	}
	offset := segment.headerBytes
	sequence := segment.meta.FirstSequence - 1
	if len(segment.index) > 0 {
		idx := sort.Search(len(segment.index), func(i int) bool { return segment.index[i].Sequence > after }) - 1
		if idx >= 0 {
			sequence = segment.index[idx].Sequence - 1
			offset = segment.index[idx].Offset
		}
	}
	results := make([]Record, 0, limit)
	var used int64
	for sequence < segment.meta.LastSequence && len(results) < limit {
		record, next, frameBytes, err := readWALV2FrameAt(file, offset, segment.meta.DataBytes)
		if err != nil {
			return results, used, err
		}
		if record.Sequence != sequence+1 {
			return results, used, fmt.Errorf("WAL v2 sequence mismatch at offset %d: got=%d want=%d", offset, record.Sequence, sequence+1)
		}
		sequence = record.Sequence
		offset = next
		if sequence > after {
			if len(results) > 0 && used+frameBytes > maxBytes {
				return results, used, nil
			}
			results = append(results, record)
			used += frameBytes
		}
	}
	return results, used, nil
}

func scanWALV2Data(file *os.File, header walV2Header, headerBytes, dataBytes int64, firstSequence uint64, visit func(Record) error) ([]walIndex, uint64, uint64, error) {
	if dataBytes < headerBytes {
		return nil, 0, 0, errors.New("committed data ends inside segment header")
	}
	index := make([]walIndex, 0)
	offset := headerBytes
	sequence := firstSequence - 1
	for offset < dataBytes {
		if (sequence-(firstSequence-1))%walIndexStride == 0 {
			index = append(index, walIndex{Sequence: sequence + 1, Offset: offset})
		}
		record, next, _, err := readWALV2FrameAt(file, offset, dataBytes)
		if err != nil {
			return nil, sequence - (firstSequence - 1), sequence, err
		}
		if record.Sequence != sequence+1 {
			return nil, sequence - (firstSequence - 1), sequence, fmt.Errorf("sequence mismatch: got=%d want=%d", record.Sequence, sequence+1)
		}
		if record.JournalID != header.JournalID || (record.Identity != "" && record.Identity != header.Identity) || (record.Source != "" && header.Source != "" && record.Source != header.Source) {
			return nil, sequence - (firstSequence - 1), sequence, errors.New("record identity differs from segment header")
		}
		if visit != nil {
			if err := visit(record); err != nil {
				return nil, sequence - (firstSequence - 1), sequence, err
			}
		}
		sequence++
		offset = next
	}
	if offset != dataBytes {
		return nil, sequence - (firstSequence - 1), sequence, errors.New("frame crosses committed segment boundary")
	}
	return index, sequence - (firstSequence - 1), sequence, nil
}

func readWALV2FrameAt(file *os.File, offset, end int64) (Record, int64, int64, error) {
	if end-offset < 8 {
		return Record{}, offset, 0, io.ErrUnexpectedEOF
	}
	var prefix [8]byte
	if _, err := file.ReadAt(prefix[:], offset); err != nil {
		return Record{}, offset, 0, err
	}
	length := binary.LittleEndian.Uint32(prefix[:4])
	if length == 0 || length > maxFrame || int64(length)+8 > end-offset {
		return Record{}, offset, 0, errors.New("invalid or truncated WAL v2 frame length")
	}
	body := make([]byte, length)
	if _, err := file.ReadAt(body, offset+8); err != nil {
		return Record{}, offset, 0, err
	}
	if crc32.Checksum(body, walV2CRC) != binary.LittleEndian.Uint32(prefix[4:]) {
		return Record{}, offset, 0, errors.New("WAL v2 frame CRC32C mismatch")
	}
	record, err := decodeWALV2Frame(body)
	if err != nil {
		return Record{}, offset, 0, err
	}
	frameBytes := int64(8) + int64(length)
	return record, offset + frameBytes, frameBytes, nil
}

func encodeWALV2Batch(records []Record, manifest walV2Manifest) ([][]byte, []Record, error) {
	return encodeWALV2BatchBounded(records, manifest, int64(^uint64(0)>>1))
}

// The bounded variant prevents allocating an entire impossible batch before
// checking segment capacity. Individual records are still bounded by maxFrame.
func encodeWALV2BatchBounded(records []Record, manifest walV2Manifest, maxBytes int64) ([][]byte, []Record, error) {
	frames := make([][]byte, 0, len(records))
	encoded := make([]Record, 0, len(records))
	last := manifest.Active.LastSequence
	if uint64(len(records)) > ^uint64(0)-last {
		return nil, nil, errors.New("WAL v2 sequence overflow")
	}
	var used int64
	for i, original := range records {
		record := original
		sequence := last + uint64(i) + 1
		if record.Sequence == 0 {
			record.Sequence = sequence
		} else if record.Sequence != sequence {
			return nil, nil, fmt.Errorf("WAL v2 sequence mismatch: got=%d want=%d", record.Sequence, sequence)
		}
		if record.JournalID == "" {
			record.JournalID = manifest.JournalID
		} else if record.JournalID != manifest.JournalID {
			return nil, nil, errors.New("WAL v2 record journal ID differs from manifest")
		}
		if record.Identity == "" {
			record.Identity = manifest.Identity
		} else if record.Identity != manifest.Identity {
			return nil, nil, errors.New("WAL v2 record identity differs from manifest")
		}
		if record.Source != "" && manifest.Source != "" && record.Source != manifest.Source {
			return nil, nil, errors.New("WAL v2 record source differs from manifest")
		}
		// The raw payload plus fixed envelopes is a lower bound on frame size.
		// Reject it before duplicating a large payload into an encoded frame.
		minimumFrameBytes := int64(len(record.Raw)) + walV2FramePrefixBytes + 8
		if minimumFrameBytes > maxBytes-used {
			return nil, nil, fmt.Errorf("WAL v2 batch cannot fit segment: at least %d bytes needed, %d available", minimumFrameBytes, maxBytes-used)
		}
		frame, err := encodeWALV2Frame(record)
		if err != nil {
			return nil, nil, err
		}
		if int64(len(frame)) > maxBytes-used {
			return nil, nil, fmt.Errorf("WAL v2 encoded batch exceeds segment data limit %d", maxBytes)
		}
		used += int64(len(frame))
		frames = append(frames, frame)
		encoded = append(encoded, record)
	}
	return frames, encoded, nil
}

func encodeWALV2Frame(record Record) ([]byte, error) {
	raw := record.Raw
	record.Raw = nil
	metadata, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	if len(metadata) > walV2MaxHeaderBytes || len(raw) > maxFrame {
		return nil, errors.New("WAL v2 record metadata or payload exceeds limit")
	}
	body := make([]byte, walV2FramePrefixBytes+len(metadata)+len(raw))
	binary.LittleEndian.PutUint64(body[:8], record.Sequence)
	binary.LittleEndian.PutUint32(body[8:12], uint32(len(metadata)))
	copy(body[12:12+len(metadata)], metadata)
	binary.LittleEndian.PutUint32(body[12+len(metadata):16+len(metadata)], uint32(len(raw)))
	copy(body[16+len(metadata):], raw)
	if len(body) > maxFrame {
		return nil, fmt.Errorf("WAL v2 frame size %d exceeds %d", len(body), maxFrame)
	}
	frame := make([]byte, 8+len(body))
	binary.LittleEndian.PutUint32(frame[:4], uint32(len(body)))
	binary.LittleEndian.PutUint32(frame[4:8], crc32.Checksum(body, walV2CRC))
	copy(frame[8:], body)
	return frame, nil
}

func decodeWALV2Frame(body []byte) (Record, error) {
	if len(body) < walV2FramePrefixBytes {
		return Record{}, errors.New("WAL v2 frame payload is too short")
	}
	sequence := binary.LittleEndian.Uint64(body[:8])
	metadataLen := binary.LittleEndian.Uint32(body[8:12])
	if metadataLen == 0 || metadataLen > walV2MaxHeaderBytes || uint64(metadataLen)+walV2FramePrefixBytes > uint64(len(body)) {
		return Record{}, errors.New("WAL v2 metadata length is invalid")
	}
	metadataEnd := 12 + int(metadataLen)
	rawLen := binary.LittleEndian.Uint32(body[metadataEnd : metadataEnd+4])
	if uint64(metadataEnd+4)+uint64(rawLen) != uint64(len(body)) {
		return Record{}, errors.New("WAL v2 raw payload length is invalid")
	}
	var record Record
	if err := decodeWALV2JSON(body[12:metadataEnd], &record); err != nil {
		return Record{}, fmt.Errorf("WAL v2 record metadata: %w", err)
	}
	if record.Sequence != sequence || sequence == 0 {
		return Record{}, errors.New("WAL v2 sequence envelope mismatch")
	}
	record.Raw = append([]byte(nil), body[metadataEnd+4:]...)
	return record, nil
}

func decodeWALV2JSON(encoded []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func encodeWALV2Header(header walV2Header) ([]byte, error) {
	if err := normalizeWALV2HeaderEnvelope(&header); err != nil {
		return nil, err
	}
	metadata, err := json.Marshal(header)
	if err != nil {
		return nil, err
	}
	return encodeWALV2HeaderMetadata(metadata)
}

// encodeWALV2LegacyHeader reproduces the pre-freeze v2 header exactly. It is
// used only to repair a legacy empty active header whose manifest committed
// offset records the old, shorter header length.
func encodeWALV2LegacyHeader(header walV2Header) ([]byte, error) {
	legacy := struct {
		Version       int    `json:"version"`
		Segment       uint64 `json:"segment"`
		Source        string `json:"source"`
		Identity      string `json:"identity"`
		JournalID     string `json:"journal_id"`
		FirstSequence uint64 `json:"first_sequence"`
	}{
		Version: header.Version, Segment: header.Segment, Source: header.Source, Identity: header.Identity,
		JournalID: header.JournalID, FirstSequence: header.FirstSequence,
	}
	metadata, err := json.Marshal(legacy)
	if err != nil {
		return nil, err
	}
	return encodeWALV2HeaderMetadata(metadata)
}

func encodeWALV2HeaderMetadata(metadata []byte) ([]byte, error) {
	if len(metadata) == 0 || len(metadata) > walV2MaxHeaderBytes {
		return nil, errors.New("WAL v2 segment header metadata size is invalid")
	}
	encoded := make([]byte, 8+2+4+len(metadata)+4)
	copy(encoded[:8], walV2SegmentMagic)
	binary.LittleEndian.PutUint16(encoded[8:10], walV2Version)
	binary.LittleEndian.PutUint32(encoded[10:14], uint32(len(metadata)))
	copy(encoded[14:14+len(metadata)], metadata)
	binary.LittleEndian.PutUint32(encoded[14+len(metadata):], crc32.Checksum(metadata, walV2CRC))
	return encoded, nil
}

func readWALV2Header(file *os.File) (walV2Header, int64, error) {
	var prefix [14]byte
	if _, err := file.ReadAt(prefix[:], 0); err != nil {
		return walV2Header{}, 0, err
	}
	if string(prefix[:8]) != string(walV2SegmentMagic) {
		return walV2Header{}, 0, errors.New("wrong WAL v2 segment header magic")
	}
	physicalVersion := binary.LittleEndian.Uint16(prefix[8:10])
	if physicalVersion != walV2Version {
		return walV2Header{}, 0, fmt.Errorf("unsupported WAL v2 segment format version %d", physicalVersion)
	}
	length := binary.LittleEndian.Uint32(prefix[10:14])
	if length == 0 || length > walV2MaxHeaderBytes {
		return walV2Header{}, 0, errors.New("WAL v2 segment header length is invalid")
	}
	metadata := make([]byte, length)
	if _, err := file.ReadAt(metadata, 14); err != nil {
		return walV2Header{}, 0, err
	}
	var checksum [4]byte
	if _, err := file.ReadAt(checksum[:], 14+int64(length)); err != nil {
		return walV2Header{}, 0, err
	}
	if crc32.Checksum(metadata, walV2CRC) != binary.LittleEndian.Uint32(checksum[:]) {
		return walV2Header{}, 0, errors.New("WAL v2 segment header CRC32C mismatch")
	}
	var header walV2Header
	if err := decodeWALV2JSON(metadata, &header); err != nil {
		return walV2Header{}, 0, err
	}
	if header.Version != walV2Version {
		return walV2Header{}, 0, fmt.Errorf("unsupported WAL v2 JSON header version %d", header.Version)
	}
	if header.Segment == 0 || header.FirstSequence == 0 || header.Identity == "" || header.JournalID == "" {
		return walV2Header{}, 0, errors.New("WAL v2 segment header fields are invalid")
	}
	if err := normalizeWALV2HeaderEnvelope(&header); err != nil {
		return walV2Header{}, 0, err
	}
	return header, int64(14 + length + 4), nil
}

func validateWALV2Header(header walV2Header, manifest walV2Manifest, segment, firstSequence uint64) error {
	if err := normalizeWALV2HeaderEnvelope(&header); err != nil {
		return err
	}
	if err := normalizeWALV2ManifestEnvelope(&manifest); err != nil {
		return err
	}
	if header.Version != walV2Version || header.Segment != segment || header.FirstSequence != firstSequence ||
		header.Source != manifest.Source || header.Identity != manifest.Identity || header.JournalID != manifest.JournalID ||
		header.EnvelopeSchemaVersion != manifest.EnvelopeSchemaVersion || header.GeyserDescriptorSHA256 != manifest.GeyserDescriptorSHA256 ||
		header.StorageDescriptorSHA256 != manifest.StorageDescriptorSHA256 {
		return errors.New("WAL v2 segment header differs from manifest identity")
	}
	return nil
}

func encodeWALV2Footer(footer walV2Footer) []byte {
	encoded := make([]byte, walV2FooterBytes)
	copy(encoded[:8], walV2FooterMagic)
	offset := 8
	for _, value := range []uint64{footer.Segment, footer.FirstSequence, footer.LastSequence, footer.Records, footer.DataBytes} {
		binary.LittleEndian.PutUint64(encoded[offset:offset+8], value)
		offset += 8
	}
	copy(encoded[offset:offset+sha256.Size], footer.Digest[:])
	offset += sha256.Size
	binary.LittleEndian.PutUint32(encoded[offset:], crc32.Checksum(encoded[:offset], walV2CRC))
	return encoded
}

func readWALV2Footer(file *os.File, offset int64) (walV2Footer, error) {
	var footer walV2Footer
	encoded := make([]byte, walV2FooterBytes)
	if _, err := file.ReadAt(encoded, offset); err != nil {
		return footer, err
	}
	if string(encoded[:8]) != string(walV2FooterMagic) {
		return footer, errors.New("WAL v2 sealed segment footer magic mismatch")
	}
	checksumOffset := len(encoded) - 4
	if crc32.Checksum(encoded[:checksumOffset], walV2CRC) != binary.LittleEndian.Uint32(encoded[checksumOffset:]) {
		return footer, errors.New("WAL v2 sealed segment footer CRC32C mismatch")
	}
	values := make([]uint64, 5)
	position := 8
	for i := range values {
		values[i] = binary.LittleEndian.Uint64(encoded[position : position+8])
		position += 8
	}
	footer.Segment, footer.FirstSequence, footer.LastSequence, footer.Records, footer.DataBytes = values[0], values[1], values[2], values[3], values[4]
	copy(footer.Digest[:], encoded[position:position+sha256.Size])
	return footer, nil
}

func openAndVerifyWALV2Sealed(dir string, meta walV2SegmentMeta, manifest walV2Manifest, visit func(Record) error) (walV2Segment, error) {
	path := filepath.Join(dir, "segments", meta.File)
	file, err := os.Open(path)
	if err != nil {
		return walV2Segment{}, err
	}
	defer file.Close()
	header, headerBytes, err := readWALV2Header(file)
	if err != nil {
		return walV2Segment{}, err
	}
	if err := validateWALV2Header(header, manifest, meta.ID, meta.FirstSequence); err != nil {
		return walV2Segment{}, err
	}
	footer, err := readWALV2Footer(file, meta.DataBytes)
	if err != nil {
		return walV2Segment{}, err
	}
	digest, err := hashWALV2Prefix(file, meta.DataBytes)
	if err != nil {
		return walV2Segment{}, err
	}
	stat, err := file.Stat()
	if err != nil {
		return walV2Segment{}, err
	}
	if stat.Size() != meta.DataBytes+walV2FooterBytes || footer.Segment != meta.ID || footer.FirstSequence != meta.FirstSequence ||
		footer.LastSequence != meta.LastSequence || footer.Records != meta.Records || footer.DataBytes != uint64(meta.DataBytes) ||
		footer.Digest != digest || (meta.Digest != "" && hex.EncodeToString(digest[:]) != meta.Digest) {
		return walV2Segment{}, errors.New("sealed segment footer, digest or manifest mismatch")
	}
	// Hash verification above binds the cache to the actual immutable bytes.
	index, cached := loadWALV2Index(dir, file, meta, headerBytes)
	if !cached {
		rebuilt, count, last, err := scanWALV2Data(file, header, headerBytes, meta.DataBytes, meta.FirstSequence, nil)
		if err != nil {
			return walV2Segment{}, err
		}
		if count != meta.Records || last != meta.LastSequence {
			return walV2Segment{}, errors.New("sealed segment record count or sequence mismatch")
		}
		index = rebuilt
	}
	if visit != nil {
		if _, _, _, err := scanWALV2Data(file, header, headerBytes, meta.DataBytes, meta.FirstSequence, visit); err != nil {
			return walV2Segment{}, err
		}
	}
	return walV2Segment{meta: meta, headerBytes: headerBytes, index: index, indexCached: cached}, nil
}

// replayVerifiedWALV2Sealed is called only after validateAndVisit has checked
// all sealed segments and the active tail. It avoids rehashing large sealed
// files while delivering the already validated committed record stream.
func replayVerifiedWALV2Sealed(dir string, segment walV2Segment, manifest walV2Manifest, visit func(Record) error) error {
	file, err := os.Open(filepath.Join(dir, "segments", segment.meta.File))
	if err != nil {
		return err
	}
	defer file.Close()
	header, headerBytes, err := readWALV2Header(file)
	if err != nil {
		return err
	}
	if err := validateWALV2Header(header, manifest, segment.meta.ID, segment.meta.FirstSequence); err != nil {
		return err
	}
	if headerBytes != segment.headerBytes {
		return errors.New("WAL v2 verified sealed header size changed before replay")
	}
	_, count, last, err := scanWALV2Data(file, header, headerBytes, segment.meta.DataBytes, segment.meta.FirstSequence, visit)
	if err != nil {
		return err
	}
	if count != segment.meta.Records || last != segment.meta.LastSequence {
		return errors.New("WAL v2 verified sealed sequence changed before replay")
	}
	return nil
}

func verifyWALV2Sealed(dir string, active walV2ActiveMeta, manifest walV2Manifest) (walV2SegmentMeta, []walIndex, error) {
	meta := walV2SegmentMeta{ID: active.ID, File: walV2SegmentFileName(active.ID, true), FirstSequence: active.FirstSequence, LastSequence: active.LastSequence, Records: active.Records, DataBytes: active.CommittedBytes}
	segment, err := openAndVerifyWALV2Sealed(dir, meta, manifest, nil)
	if err != nil {
		return walV2SegmentMeta{}, nil, err
	}
	file, err := os.Open(filepath.Join(dir, "segments", meta.File))
	if err != nil {
		return walV2SegmentMeta{}, nil, err
	}
	digest, hashErr := hashWALV2Prefix(file, meta.DataBytes)
	closeErr := file.Close()
	if err := errors.Join(hashErr, closeErr); err != nil {
		return walV2SegmentMeta{}, nil, err
	}
	meta.Digest = hex.EncodeToString(digest[:])
	return meta, segment.index, nil
}

func hashWALV2Prefix(file *os.File, length int64) ([sha256.Size]byte, error) {
	h := sha256.New()
	buffer := make([]byte, 1<<20)
	var offset int64
	for offset < length {
		want := int64(len(buffer))
		if length-offset < want {
			want = length - offset
		}
		n, err := file.ReadAt(buffer[:want], offset)
		if err != nil && !errors.Is(err, io.EOF) {
			return [sha256.Size]byte{}, err
		}
		if int64(n) != want {
			return [sha256.Size]byte{}, io.ErrUnexpectedEOF
		}
		_, _ = h.Write(buffer[:n])
		offset += int64(n)
	}
	var digest [sha256.Size]byte
	copy(digest[:], h.Sum(nil))
	return digest, nil
}

func readWALV2Current(dir string) (walV2Manifest, bool, error) {
	current, err := os.ReadFile(filepath.Join(dir, "CURRENT"))
	if errors.Is(err, os.ErrNotExist) {
		return walV2Manifest{}, false, nil
	}
	if err != nil {
		return walV2Manifest{}, false, err
	}
	generation, err := strconv.ParseUint(strings.TrimSpace(string(current)), 10, 64)
	if err != nil || generation == 0 {
		return walV2Manifest{}, false, errors.New("WAL v2 CURRENT pointer is invalid")
	}
	manifest, err := readWALV2ManifestGeneration(dir, generation)
	if err != nil {
		return walV2Manifest{}, false, err
	}
	return manifest, true, nil
}

func readWALV2ManifestGeneration(dir string, generation uint64) (walV2Manifest, error) {
	path := filepath.Join(dir, "manifests", walV2ManifestFileName(generation))
	encoded, err := os.ReadFile(path)
	if err != nil {
		return walV2Manifest{}, err
	}
	var manifest walV2Manifest
	if err := decodeWALV2JSON(encoded, &manifest); err != nil {
		return walV2Manifest{}, fmt.Errorf("WAL v2 manifest decode: %w", err)
	}
	checksum := manifest.Checksum
	manifest.Checksum = ""
	canonical, err := json.Marshal(manifest)
	if err != nil {
		return walV2Manifest{}, err
	}
	digest := sha256.Sum256(canonical)
	if checksum != hex.EncodeToString(digest[:]) {
		return walV2Manifest{}, errors.New("WAL v2 manifest checksum is invalid")
	}
	if manifest.Version != walV2Version {
		return walV2Manifest{}, fmt.Errorf("unsupported WAL v2 manifest version %d", manifest.Version)
	}
	if manifest.Generation != generation || manifest.JournalID == "" || manifest.Identity == "" {
		return walV2Manifest{}, errors.New("WAL v2 manifest identity or generation is invalid")
	}
	manifest.Checksum = checksum
	if err := normalizeWALV2ManifestEnvelope(&manifest); err != nil {
		return walV2Manifest{}, err
	}
	if err := validateWALV2Manifest(manifest); err != nil {
		return walV2Manifest{}, err
	}
	return manifest, nil
}

// recoverWALV2BootstrapWithoutCurrent accepts only the initial, header-only
// segment. Neither an extra segment nor a committed-looking record can be
// attributed safely without CURRENT, so both remain hard errors.
func recoverWALV2BootstrapWithoutCurrent(dir, source, identity string) (walV2Manifest, bool, error) {
	segments, err := os.ReadDir(filepath.Join(dir, "segments"))
	if err != nil {
		return walV2Manifest{}, false, err
	}
	if len(segments) == 0 {
		return walV2Manifest{}, false, nil
	}
	if len(segments) != 1 || segments[0].Name() != walV2SegmentFileName(1, false) || !segments[0].Type().IsRegular() {
		return walV2Manifest{}, false, errors.New("WAL v2 has ambiguous segments without CURRENT")
	}
	rootEntries, err := os.ReadDir(dir)
	if err != nil {
		return walV2Manifest{}, false, err
	}
	for _, entry := range rootEntries {
		switch entry.Name() {
		case "LOCK":
			if !entry.Type().IsRegular() {
				return walV2Manifest{}, false, errors.New("WAL v2 bootstrap LOCK is not a regular file")
			}
		case "segments", "manifests":
			if !entry.IsDir() {
				return walV2Manifest{}, false, errors.New("WAL v2 bootstrap layout is invalid")
			}
		default:
			return walV2Manifest{}, false, errors.New("WAL v2 has unexpected files without CURRENT")
		}
	}
	f, err := os.Open(walV2SegmentPath(dir, 1, false))
	if err != nil {
		return walV2Manifest{}, false, err
	}
	defer f.Close()
	header, headerBytes, err := readWALV2Header(f)
	if err != nil {
		return walV2Manifest{}, false, fmt.Errorf("WAL v2 bootstrap header: %w", err)
	}
	stat, err := f.Stat()
	if err != nil {
		return walV2Manifest{}, false, err
	}
	if header.Segment != 1 || header.FirstSequence != 1 || header.Source != source || header.Identity != identity || stat.Size() != headerBytes {
		return walV2Manifest{}, false, errors.New("WAL v2 bootstrap header identity or length mismatch")
	}
	manifest := walV2Manifest{
		Version: walV2Version, Generation: 1, Source: source, Identity: identity, JournalID: header.JournalID,
		EnvelopeSchemaVersion: header.EnvelopeSchemaVersion, GeyserDescriptorSHA256: header.GeyserDescriptorSHA256, StorageDescriptorSHA256: header.StorageDescriptorSHA256,
		Active: walV2ActiveMeta{ID: 1, FirstSequence: 1, CommittedBytes: headerBytes},
	}
	manifestEntries, err := os.ReadDir(filepath.Join(dir, "manifests"))
	if err != nil {
		return walV2Manifest{}, false, err
	}
	var hasManifest bool
	for _, entry := range manifestEntries {
		if !entry.Type().IsRegular() {
			return walV2Manifest{}, false, errors.New("WAL v2 bootstrap manifest directory contains a non-file")
		}
		if entry.Name() == walV2ManifestFileName(1) {
			hasManifest = true
			continue
		}
		// Atomic writes may leave an unpublished temporary manifest or CURRENT.
		if strings.HasPrefix(entry.Name(), ".wal-v2-") && strings.HasSuffix(entry.Name(), ".tmp") {
			continue
		}
		return walV2Manifest{}, false, errors.New("WAL v2 bootstrap has an unexpected manifest")
	}
	if hasManifest {
		existing, err := readWALV2ManifestGeneration(dir, 1)
		if err != nil {
			return walV2Manifest{}, false, fmt.Errorf("WAL v2 bootstrap manifest: %w", err)
		}
		if existing.Source != manifest.Source || existing.Identity != manifest.Identity || existing.JournalID != manifest.JournalID ||
			existing.EnvelopeSchemaVersion != manifest.EnvelopeSchemaVersion || existing.GeyserDescriptorSHA256 != manifest.GeyserDescriptorSHA256 || existing.StorageDescriptorSHA256 != manifest.StorageDescriptorSHA256 ||
			existing.Active != manifest.Active || len(existing.Sealed) != 0 {
			return walV2Manifest{}, false, errors.New("WAL v2 bootstrap manifest differs from segment header")
		}
	}
	return manifest, true, nil
}

func validateWALV2Manifest(manifest walV2Manifest) error {
	if err := normalizeWALV2ManifestEnvelope(&manifest); err != nil {
		return err
	}
	sequence, segmentID := uint64(1), uint64(1)
	for _, meta := range manifest.Sealed {
		if meta.ID != segmentID || meta.File != walV2SegmentFileName(meta.ID, true) || meta.FirstSequence != sequence || meta.Records == 0 || meta.LastSequence < meta.FirstSequence || meta.LastSequence-meta.FirstSequence+1 != meta.Records || meta.DataBytes <= 0 || len(meta.Digest) != sha256.Size*2 {
			return errors.New("WAL v2 sealed manifest entries are inconsistent")
		}
		sequence = meta.LastSequence + 1
		segmentID++
	}
	active := manifest.Active
	if active.ID != segmentID || active.FirstSequence != sequence || active.CommittedBytes <= 0 {
		return errors.New("WAL v2 active manifest entry is inconsistent")
	}
	if active.Records == 0 {
		if active.LastSequence != active.FirstSequence-1 {
			return errors.New("WAL v2 empty active segment has an invalid last sequence")
		}
	} else if active.LastSequence < active.FirstSequence || active.LastSequence-active.FirstSequence+1 != active.Records {
		return errors.New("WAL v2 active sequence range is inconsistent")
	}
	return nil
}

func writeWALV2Manifest(dir string, manifest walV2Manifest) error {
	if err := normalizeWALV2ManifestEnvelope(&manifest); err != nil {
		return err
	}
	manifest.Checksum = ""
	canonical, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(canonical)
	manifest.Checksum = hex.EncodeToString(digest[:])
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	manifestPath := filepath.Join(dir, "manifests", walV2ManifestFileName(manifest.Generation))
	if err := writeWALV2Atomic(manifestPath, encoded, 0600); err != nil {
		return err
	}
	return writeWALV2Atomic(filepath.Join(dir, "CURRENT"), []byte(strconv.FormatUint(manifest.Generation, 10)+"\n"), 0600)
}

func writeWALV2Atomic(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".wal-v2-*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err := f.Chmod(mode); err != nil {
		_ = f.Close()
		return err
	}
	if err := writeWALV2All(f, data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncWALV2Dir(dir)
}

func ensureWALV2DirectoryEmpty(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name() == "LOCK" || entry.Name() == "segments" || entry.Name() == "manifests" {
			if entry.IsDir() {
				children, readErr := os.ReadDir(filepath.Join(dir, entry.Name()))
				if readErr != nil {
					return readErr
				}
				if len(children) != 0 {
					return errors.New("WAL v2 has files but no CURRENT manifest")
				}
			}
			continue
		}
		return errors.New("WAL v2 directory has unexpected files without CURRENT")
	}
	return nil
}

func cloneWALV2Manifest(manifest walV2Manifest) walV2Manifest {
	manifest.Sealed = append([]walV2SegmentMeta(nil), manifest.Sealed...)
	return manifest
}

func walV2SegmentFileName(id uint64, sealed bool) string {
	extension := "open"
	if sealed {
		extension = "seg"
	}
	return fmt.Sprintf("segment-%020d.%s", id, extension)
}

func walV2SegmentPath(dir string, id uint64, sealed bool) string {
	return filepath.Join(dir, "segments", walV2SegmentFileName(id, sealed))
}

func walV2ManifestFileName(generation uint64) string {
	return fmt.Sprintf("manifest-%020d.json", generation)
}

func writeWALV2All(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func syncWALV2Dir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func frameOffset(frames [][]byte, index int) int64 {
	var offset int64
	for i := 0; i < index; i++ {
		offset += int64(len(frames[i]))
	}
	return offset
}

func indexHeaderBytes(dir string, segmentID uint64) int64 {
	file, err := os.Open(walV2SegmentPath(dir, segmentID, true))
	if err != nil {
		return 0
	}
	defer file.Close()
	_, size, err := readWALV2Header(file)
	if err != nil {
		return 0
	}
	return size
}
