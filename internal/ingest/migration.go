package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"os"
	"path/filepath"
)

// WALMigrationReport describes an explicit, verified copy from a legacy v1
// journal to a new v2 journal. Journal IDs are intentionally different, so
// consumers must start or import their own positions in the new destination.
type WALMigrationReport struct {
	Source        string `json:"source"`
	Identity      string `json:"identity"`
	OldJournalID  string `json:"old_journal_id,omitempty"`
	NewJournalID  string `json:"new_journal_id"`
	Records       uint64 `json:"records"`
	ContentSHA256 string `json:"content_sha256"`
	Target        string `json:"target"`
}

// CheckWALV1MigrationEnvelope verifies that every v1 record is representable
// by the current Record schema. A normal WAL reader can tolerate unknown JSON
// fields, but a migration must fail closed rather than silently discard them.
func CheckWALV1MigrationEnvelope(sourceWAL string) error {
	reader, err := OpenWALReader(sourceWAL)
	if err != nil {
		return err
	}
	defer reader.Close()
	if reader.v2 != nil {
		return errors.New("migration envelope preflight requires a v1 WAL")
	}
	reader.strictJSONFields = true
	_, err = reader.Read(false, nil)
	return err
}

// MigrateWALV1ToV2 copies a closed v1 WAL into a brand-new source directory
// under destinationRoot. It keeps the original source locked for reading,
// verifies every source frame and every target frame, and only then publishes
// the new source directory with one rename. The existing WAL and ACKs are never
// changed. destinationRoot must differ from the source's parent root; the
// destination source directory must not already exist.
//
// The resulting v2 journal preserves source-local Sequence and all observation
// fields except JournalID (which changes by design) and an empty legacy
// Identity (which is filled with the verified chosen identity). It does not
// copy source checkpoints, dedupe caches or consumers: Store rebuilds derived
// state on first open and consumers must explicitly register in the new root.
func MigrateWALV1ToV2(ctx context.Context, sourceWAL, destinationRoot, source, expectedIdentity string, maxSegmentBytes int64) (WALMigrationReport, error) {
	var result WALMigrationReport
	if ctx == nil || sourceWAL == "" || destinationRoot == "" || source == "" {
		return result, errors.New("migration requires context, source WAL, destination root and source ID")
	}
	if maxSegmentBytes <= 0 {
		maxSegmentBytes = walV2DefaultSegmentBytes
	}
	if maxSegmentBytes < 2<<20 || maxSegmentBytes > 4<<30 {
		return result, errors.New("migration segment size must be 2 MiB..4 GiB")
	}
	if info, err := os.Lstat(sourceWAL); err != nil {
		return result, err
	} else if !info.Mode().IsRegular() {
		return result, errors.New("migration requires a regular v1 WAL file")
	}
	sourceRoot, err := filepath.Abs(filepath.Dir(filepath.Dir(sourceWAL)))
	if err != nil {
		return result, err
	}
	targetRoot, err := filepath.Abs(destinationRoot)
	if err != nil {
		return result, err
	}
	if sourceRoot == targetRoot {
		return result, errors.New("migration destination must be a different data root")
	}
	target := filepath.Join(targetRoot, source)
	if _, err := os.Lstat(target); err == nil {
		return result, fmt.Errorf("migration destination already exists: %s", target)
	} else if !errors.Is(err, os.ErrNotExist) {
		return result, err
	}
	reader, err := OpenWALReader(sourceWAL)
	if err != nil {
		return result, err
	}
	defer reader.Close()
	// A v1 frame may contain fields added by a newer writer. Since Record
	// cannot represent them, never certify a lossy v1-to-v2 migration.
	reader.strictJSONFields = true
	// The reader's shared advisory lock prevents a cooperating writer from
	// changing source contents between preflight and the second replay pass.
	var count uint64
	var identity, oldJournalID string
	if expectedIdentity != "" {
		identity = expectedIdentity
	}
	_, err = reader.Read(false, func(record Record) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if record.Source != "" && record.Source != source {
			return fmt.Errorf("migration source mismatch at sequence %d: %q", record.Sequence, record.Source)
		}
		if record.Identity != "" {
			if identity != "" && record.Identity != identity {
				return fmt.Errorf("migration subscription identity changes at sequence %d", record.Sequence)
			}
			identity = record.Identity
		}
		if record.JournalID != "" {
			if oldJournalID != "" && oldJournalID != record.JournalID {
				return fmt.Errorf("migration journal ID changes at sequence %d", record.Sequence)
			}
			oldJournalID = record.JournalID
		}
		count++
		return nil
	})
	if err != nil {
		return result, fmt.Errorf("migration source preflight: %w", err)
	}
	if identity == "" {
		return result, errors.New("source WAL has no subscription identity; provide the expected identity explicitly")
	}
	if err := os.MkdirAll(targetRoot, 0700); err != nil {
		return result, err
	}
	staging, err := os.MkdirTemp(targetRoot, "."+source+"-migrating-*")
	if err != nil {
		return result, err
	}
	defer func() {
		if staging != "" {
			_ = os.RemoveAll(staging)
		}
	}()
	stagedJournal := filepath.Join(staging, "journal.v2")
	writer, err := OpenWALV2(stagedJournal, source, identity, maxSegmentBytes, nil)
	if err != nil {
		return result, err
	}
	newJournalID := writer.journalID
	var srcHash hash.Hash = sha256.New()
	batch := make([]Record, 0, 128)
	var batchBytes int64
	maxBatchBytes := min(int64(16<<20), walV2DataLimit(maxSegmentBytes)-walV2MaxHeaderBytes)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := writer.AppendBatch(batch); err != nil {
			return err
		}
		batch = batch[:0]
		batchBytes = 0
		return nil
	}
	_, err = reader.Read(false, func(record Record) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		record.Identity = identity
		record.JournalID = ""
		if err := hashMigratedRecord(srcHash, record); err != nil {
			return err
		}
		sized := record
		sized.JournalID = newJournalID
		frameBytes, err := migratedFrameBytes(sized)
		if err != nil {
			return err
		}
		if len(batch) > 0 && (batchBytes+frameBytes > maxBatchBytes || len(batch) == cap(batch)) {
			if err := flush(); err != nil {
				return err
			}
		}
		batch = append(batch, record)
		batchBytes += frameBytes
		if batchBytes > maxBatchBytes {
			return flush()
		}
		return nil
	})
	if err == nil {
		err = flush()
	}
	closeErr := writer.Close()
	if err != nil || closeErr != nil {
		return result, fmt.Errorf("migration source copy: %w", errors.Join(err, closeErr))
	}
	var destCount uint64
	var dstHash hash.Hash = sha256.New()
	err = ReadWAL(stagedJournal, func(record Record) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if record.JournalID != newJournalID || record.Identity != identity {
			return errors.New("migration target journal or identity mismatch")
		}
		record.JournalID = ""
		if err := hashMigratedRecord(dstHash, record); err != nil {
			return err
		}
		destCount++
		return nil
	})
	if err != nil {
		return result, fmt.Errorf("migration target verification: %w", err)
	}
	if destCount != count || !equalSHA256(srcHash, dstHash) {
		return result, fmt.Errorf("migration verification mismatch: source=%d target=%d", count, destCount)
	}
	// The format marker is part of the published source directory. Publish it
	// only after the copied v2 journal has been fully replay-verified, and make
	// it durable before the directory itself becomes visible at the target.
	if err := writeWALFormatMarker(staging, newWALFormatMarker(2, source, identity, newJournalID)); err != nil {
		return result, fmt.Errorf("write migrated WAL format marker: %w", err)
	}
	if _, err := os.Lstat(target); err == nil {
		return result, fmt.Errorf("migration destination appeared during copy: %s", target)
	} else if !errors.Is(err, os.ErrNotExist) {
		return result, err
	}
	if err := os.Rename(staging, target); err != nil {
		return result, fmt.Errorf("publish migrated source: %w", err)
	}
	staging = "" // Ownership transferred to the final destination.
	if err := syncWALV2Dir(targetRoot); err != nil {
		return result, fmt.Errorf("migration published but destination directory sync failed (inspect target before retry): %w", err)
	}
	result = WALMigrationReport{
		Source: source, Identity: identity, OldJournalID: oldJournalID,
		NewJournalID: newJournalID, Records: count, ContentSHA256: hex.EncodeToString(srcHash.Sum(nil)),
		Target: target,
	}
	return result, nil
}

func migratedFrameBytes(record Record) (int64, error) {
	raw := record.Raw
	record.Raw = nil
	metadata, err := json.Marshal(record)
	if err != nil {
		return 0, err
	}
	if len(metadata) > walV2MaxHeaderBytes {
		return 0, errors.New("migration record metadata exceeds WAL v2 limit")
	}
	return int64(8 + walV2FramePrefixBytes + len(metadata) + len(raw)), nil
}

func hashMigratedRecord(h hash.Hash, record Record) error {
	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}
	var size [8]byte
	binary.LittleEndian.PutUint64(size[:], uint64(len(encoded)))
	if _, err := h.Write(size[:]); err != nil {
		return err
	}
	_, err = h.Write(encoded)
	return err
}

func equalSHA256(a, b hash.Hash) bool {
	left, right := a.Sum(nil), b.Sum(nil)
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
