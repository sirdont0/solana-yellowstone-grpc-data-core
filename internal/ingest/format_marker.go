package ingest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	walFormatMarkerName          = ".wal-format.json"
	walFormatMarkerSchemaVersion = 1
	maxWALFormatMarkerBytes      = 64 << 10
)

// walFormatMarker permanently binds a source directory to one journal
// container and its source/subscription identity. The checksum detects
// accidental or partial edits; the WAL remains the authoritative event log.
type walFormatMarker struct {
	SchemaVersion int    `json:"schema_version"`
	WALVersion    int    `json:"wal_version"`
	SourceID      string `json:"source_id"`
	Identity      string `json:"identity"`
	JournalID     string `json:"journal_id"`
	Checksum      string `json:"checksum"`
}

type walFormatMarkerPayload struct {
	SchemaVersion int    `json:"schema_version"`
	WALVersion    int    `json:"wal_version"`
	SourceID      string `json:"source_id"`
	Identity      string `json:"identity"`
	JournalID     string `json:"journal_id"`
}

func normalizedWALVersion(version int) int {
	if version == 2 {
		return 2
	}
	return 1
}

func newWALFormatMarker(version int, sourceID, identity, journalID string) walFormatMarker {
	marker := walFormatMarker{
		SchemaVersion: walFormatMarkerSchemaVersion,
		WALVersion:    normalizedWALVersion(version),
		SourceID:      sourceID,
		Identity:      identity,
		JournalID:     journalID,
	}
	marker.Checksum = walFormatMarkerChecksum(marker)
	return marker
}

func walFormatMarkerChecksum(marker walFormatMarker) string {
	payload := walFormatMarkerPayload{
		SchemaVersion: marker.SchemaVersion,
		WALVersion:    marker.WALVersion,
		SourceID:      marker.SourceID,
		Identity:      marker.Identity,
		JournalID:     marker.JournalID,
	}
	encoded, _ := json.Marshal(payload)
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func validateWALFormatMarker(marker walFormatMarker) error {
	if marker.SchemaVersion != walFormatMarkerSchemaVersion {
		return fmt.Errorf("unsupported WAL format marker schema version %d", marker.SchemaVersion)
	}
	if marker.WALVersion != 1 && marker.WALVersion != 2 {
		return fmt.Errorf("invalid WAL format marker version %d", marker.WALVersion)
	}
	if marker.Identity == "" || marker.JournalID == "" {
		return errors.New("WAL format marker is missing identity or journal ID")
	}
	if decoded, err := hex.DecodeString(marker.Identity); err != nil || len(decoded) != sha256.Size {
		return errors.New("WAL format marker has an invalid subscription identity")
	}
	if decoded, err := hex.DecodeString(marker.JournalID); err != nil || len(decoded) != 16 {
		return errors.New("WAL format marker has an invalid journal ID")
	}
	if decoded, err := hex.DecodeString(marker.Checksum); err != nil || len(decoded) != sha256.Size {
		return errors.New("WAL format marker has an invalid checksum")
	}
	if marker.Checksum != walFormatMarkerChecksum(marker) {
		return errors.New("WAL format marker checksum mismatch")
	}
	return nil
}

func marshalWALFormatMarker(marker walFormatMarker) ([]byte, error) {
	if err := validateWALFormatMarker(marker); err != nil {
		return nil, err
	}
	encoded, err := json.MarshalIndent(marker, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}

func readWALFormatMarker(dataDir string) (walFormatMarker, bool, error) {
	var marker walFormatMarker
	path := filepath.Join(dataDir, walFormatMarkerName)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return marker, false, nil
	}
	if err != nil {
		return marker, false, fmt.Errorf("stat WAL format marker: %w", err)
	}
	if !info.Mode().IsRegular() {
		return marker, true, errors.New("WAL format marker is not a regular file")
	}
	if info.Size() > maxWALFormatMarkerBytes {
		return marker, true, errors.New("WAL format marker exceeds size limit")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return marker, true, fmt.Errorf("read WAL format marker: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&marker); err != nil {
		return walFormatMarker{}, true, fmt.Errorf("decode WAL format marker: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return walFormatMarker{}, true, errors.New("WAL format marker contains trailing JSON")
		}
		return walFormatMarker{}, true, fmt.Errorf("decode WAL format marker trailer: %w", err)
	}
	if err := validateWALFormatMarker(marker); err != nil {
		return walFormatMarker{}, true, fmt.Errorf("invalid WAL format marker: %w", err)
	}
	canonical, err := marshalWALFormatMarker(marker)
	if err != nil {
		return walFormatMarker{}, true, err
	}
	if !bytes.Equal(data, canonical) {
		return walFormatMarker{}, true, errors.New("WAL format marker is not canonical JSON")
	}
	return marker, true, nil
}

func writeWALFormatMarker(dataDir string, marker walFormatMarker) error {
	if marker.SchemaVersion == 0 {
		marker.SchemaVersion = walFormatMarkerSchemaVersion
	}
	if marker.WALVersion == 0 {
		marker.WALVersion = 1
	}
	marker.Checksum = walFormatMarkerChecksum(marker)
	encoded, err := marshalWALFormatMarker(marker)
	if err != nil {
		return err
	}
	path := filepath.Join(dataDir, walFormatMarkerName)
	if existing, exists, err := readWALFormatMarker(dataDir); err != nil {
		return err
	} else if exists {
		if existing == marker {
			return nil
		}
		return errors.New("refusing to overwrite conflicting WAL format marker")
	}
	tmp, err := os.CreateTemp(dataDir, ".wal-format-*.tmp")
	if err != nil {
		return fmt.Errorf("create WAL format marker temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(encoded); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write WAL format marker: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync WAL format marker: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close WAL format marker: %w", err)
	}
	// Store opens hold .wal-format.lock, so another cooperating writer cannot
	// publish between this check and rename. Never replace a marker already
	// present at the publication point.
	if _, err := os.Lstat(path); err == nil {
		return errors.New("WAL format marker appeared before publication; refusing to overwrite it")
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("check WAL format marker publication path: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("publish WAL format marker: %w", err)
	}
	dir, err := os.Open(dataDir)
	if err != nil {
		return fmt.Errorf("open WAL format marker directory: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync WAL format marker directory: %w", err)
	}
	return nil
}

func inspectWALFormat(c Config) (walFormatMarker, bool, bool, error) {
	marker, markerExists, err := readWALFormatMarker(c.DataDir)
	if err != nil {
		return walFormatMarker{}, markerExists, false, err
	}
	if c.WALVersion < 0 || c.WALVersion > 2 {
		return marker, markerExists, false, fmt.Errorf("invalid WAL version %d", c.WALVersion)
	}
	version := normalizedWALVersion(c.WALVersion)
	journalPath := filepath.Join(c.DataDir, "journal.wal")
	otherPath := filepath.Join(c.DataDir, "journal.v2")
	journalIsDir := false
	otherFormat := "v2"
	if version == 2 {
		journalPath, otherPath = otherPath, journalPath
		journalIsDir = true
		otherFormat = "v1"
	}
	if markerExists {
		if marker.WALVersion != version || marker.SourceID != c.SourceID || marker.Identity != c.Identity() {
			return marker, true, false, fmt.Errorf("WAL format marker conflict: marker binds v%d source %q identity %s; requested v%d source %q identity %s", marker.WALVersion, marker.SourceID, marker.Identity, version, c.SourceID, c.Identity())
		}
	}
	if _, err := os.Lstat(otherPath); err == nil {
		return marker, markerExists, false, fmt.Errorf("WAL format mismatch: existing %s journal at %s; requested v%d. Reopen with the original WALVersion or explicitly migrate history into a new DataDir", otherFormat, otherPath, version)
	} else if !errors.Is(err, os.ErrNotExist) {
		return marker, markerExists, false, fmt.Errorf("check alternate WAL format at %s: %w", otherPath, err)
	}
	info, err := os.Lstat(journalPath)
	if errors.Is(err, os.ErrNotExist) {
		if markerExists {
			return marker, true, false, fmt.Errorf("WAL format marker references a missing journal at %s; refusing to create replacement history", journalPath)
		}
		return marker, false, false, nil
	}
	if err != nil {
		return marker, markerExists, false, fmt.Errorf("check selected WAL journal at %s: %w", journalPath, err)
	}
	if journalIsDir && !info.IsDir() {
		return marker, markerExists, true, fmt.Errorf("WAL v2 journal path %s is not a directory", journalPath)
	}
	if !journalIsDir && !info.Mode().IsRegular() {
		return marker, markerExists, true, fmt.Errorf("WAL v1 journal path %s is not a regular file", journalPath)
	}
	return marker, markerExists, true, nil
}
