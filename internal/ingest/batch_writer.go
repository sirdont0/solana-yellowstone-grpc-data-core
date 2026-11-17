package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type queuedRecord struct {
	record     Record
	bytes      int64
	receivedAt time.Time
}

// walV2BatchedRecordUpperBound estimates the final frame after Store has added
// sequence and journal ID. The allowance also covers conversion to a bounded
// duplicate/conflict marker during Store.AppendBatch. The writer still performs
// exact framing validation; this bound only chooses safe batch boundaries.
func walV2BatchedRecordUpperBound(record Record) (int64, error) {
	metadata := record
	metadata.Raw = nil
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return 0, err
	}
	frame := int64(len(record.Raw) + len(encoded) + walV2FramePrefixBytes + 8 + 256)
	if frame < 8192 {
		frame = 8192
	}
	return frame, nil
}

// Max configured segment bytes include the v2 footer. Batch admission uses a
// conservative header/metadata reserve; a single legitimate max-size message
// may exceed this soft batching threshold and is then flushed immediately.
func walV2SafeBatchBytes(segmentBytes int64) int64 {
	if segmentBytes <= 0 {
		segmentBytes = walV2DefaultSegmentBytes
	}
	return segmentBytes - 2*walV2MaxHeaderBytes - walV2FooterBytes - 4096
}

// startBatchedWriter decouples Yellowstone Recv from disk writes. Within one
// source, writes and durable callbacks retain receive order. Queue saturation
// blocks Recv rather than silently dropping observations.
func (r *Runner) startBatchedWriter(cancel context.CancelFunc) (chan queuedRecord, <-chan error, <-chan struct{}, *ByteBudget) {
	queueBytes := r.Config.QueueBytes
	if queueBytes <= 0 {
		queueBytes = 128 << 20
	}
	batchBytes := r.Config.BatchBytes
	if batchBytes <= 0 {
		batchBytes = 16 << 20
	}
	if r.Config.WALVersion == 2 {
		if safe := walV2SafeBatchBytes(r.Config.WALSegmentBytes); safe > 0 && safe < batchBytes {
			batchBytes = safe
		}
	}
	const batchSize = 128
	const batchWait = 5 * time.Millisecond
	// Keep the handoff unbuffered. The byte budget and writer batch already
	// bound retained data; a buffered channel could accept a record after the
	// writer has failed and closed its stopped signal.
	in := make(chan queuedRecord)
	budget := r.QueueBudget
	if budget == nil {
		budget = NewByteBudget(queueBytes)
		r.QueueBudget = budget
	}
	done := make(chan error, 1)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		timer := time.NewTimer(time.Hour)
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		defer timer.Stop()
		batch := make([]queuedRecord, 0, batchSize)
		var batchSizeBytes int64
		var batchFrameBytes int64
		frameBytes := func(record Record) (int64, error) {
			if r.Config.WALVersion != 2 {
				return 0, nil
			}
			return walV2BatchedRecordUpperBound(record)
		}
		flush := func() error {
			if len(batch) == 0 {
				return nil
			}
			records := make([]Record, len(batch))
			for i := range batch {
				records[i] = batch[i].record
			}
			accepted, err := r.Store.AppendBatch(records)
			r.writerFlushed(batch)
			// A conflict later in the batch may return an error after earlier
			// records were successfully fsynced. Deliver those accepted records
			// before stopping. A failed fsync returns nil accepted and delivers none.
			if r.OnLive != nil {
				for i, ok := range accepted {
					if ok {
						r.OnLive(batch[i].record)
					}
				}
			}
			budget.Release(batchSizeBytes)
			batchSizeBytes = 0
			batchFrameBytes = 0
			batch = batch[:0]
			if err != nil {
				return err
			}
			return nil
		}
		fail := func(err error) {
			if err != nil {
				cancel()
			}
			done <- err
		}
		for {
			if len(batch) == 0 {
				item, ok := <-in
				if !ok {
					fail(nil)
					return
				}
				batch = append(batch, item)
				if size, err := frameBytes(item.record); err != nil {
					budget.Release(item.bytes)
					fail(fmt.Errorf("estimate WAL v2 batch size: %w", err))
					return
				} else {
					batchFrameBytes += size
				}
				r.writerQueued(item.receivedAt)
				batchSizeBytes += item.bytes
				timer.Reset(batchWait)
			}
			select {
			case item, ok := <-in:
				if !ok {
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					fail(flush())
					return
				}
				itemFrameBytes, err := frameBytes(item.record)
				if err != nil {
					budget.Release(item.bytes)
					fail(fmt.Errorf("estimate WAL v2 batch size: %w", err))
					return
				}
				if r.Config.WALVersion == 2 && len(batch) > 0 && (batchFrameBytes+itemFrameBytes > batchBytes || batchFrameBytes+itemFrameBytes > walV2SafeBatchBytes(r.Config.WALSegmentBytes)) {
					if err := flush(); err != nil {
						budget.Release(item.bytes)
						fail(err)
						return
					}
				}
				batch = append(batch, item)
				batchFrameBytes += itemFrameBytes
				r.writerQueued(item.receivedAt)
				batchSizeBytes += item.bytes
				if len(batch) >= batchSize || batchSizeBytes >= batchBytes || (r.Config.WALVersion == 2 && batchFrameBytes >= batchBytes) {
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					if err := flush(); err != nil {
						fail(err)
						return
					}
				}
			case <-timer.C:
				if err := flush(); err != nil {
					fail(err)
					return
				}
			}
		}
	}()
	return in, done, stopped, budget
}
