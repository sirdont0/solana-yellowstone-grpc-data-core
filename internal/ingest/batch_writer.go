package ingest

import (
	"context"
	"time"
)

// startBatchedWriter decouples Yellowstone Recv from disk writes. Within one
// source, writes and durable callbacks retain receive order. Queue saturation
// blocks Recv rather than silently dropping observations.
func (r *Runner) startBatchedWriter(cancel context.CancelFunc) (chan Record, <-chan error, <-chan struct{}) {
	const queueCapacity = 4096
	const batchSize = 128
	const batchWait = 5 * time.Millisecond
	in := make(chan Record, queueCapacity)
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
		batch := make([]Record, 0, batchSize)
		flush := func() error {
			if len(batch) == 0 {
				return nil
			}
			accepted, err := r.Store.AppendBatch(batch)
			// A conflict later in the batch may return an error after earlier
			// records were successfully fsynced. Deliver those accepted records
			// before stopping. A failed fsync returns nil accepted and delivers none.
			if r.OnLive != nil {
				for i, ok := range accepted {
					if ok {
						r.OnLive(batch[i])
					}
				}
			}
			if err != nil {
				return err
			}
			batch = batch[:0]
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
				rec, ok := <-in
				if !ok {
					fail(nil)
					return
				}
				batch = append(batch, rec)
				timer.Reset(batchWait)
			}
			select {
			case rec, ok := <-in:
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
				batch = append(batch, rec)
				if len(batch) >= batchSize {
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
	return in, done, stopped
}
