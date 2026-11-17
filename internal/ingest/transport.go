package ingest

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var errForcedDrop = errors.New("injected stream disconnect")
var errIdle = errors.New("stream idle watchdog fired")
var errDurableWriterFailed = errors.New("durable WAL writer failed")
var ErrReplayRejected = errors.New("yellowstone provider rejected requested replay cursor")

type Dialer func(context.Context, Config) (*grpc.ClientConn, error)

func GRPCDial(ctx context.Context, c Config) (*grpc.ClientConn, error) {
	target := c.Endpoint
	secure := true
	if strings.Contains(target, "://") {
		u, e := url.Parse(target)
		if e != nil {
			return nil, e
		}
		target = u.Host
		secure = u.Scheme != "http" && u.Scheme != "grpc"
	}
	if !secure && c.Token != "" {
		return nil, errors.New("refusing to send Yellowstone provider token over plaintext gRPC")
	}
	var cred credentials.TransportCredentials
	if secure {
		cred = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	} else {
		cred = insecure.NewCredentials()
	}
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := grpc.NewClient("passthrough:///"+target, grpc.WithTransportCredentials(cred), grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 20 * time.Second, Timeout: 10 * time.Second, PermitWithoutStream: true}), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(c.MaxMessageBytes)))
	if err != nil {
		return nil, err
	}
	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return conn, nil
		}
		if !conn.WaitForStateChange(dialCtx, state) {
			conn.Close()
			return nil, dialCtx.Err()
		}
	}
}

func auth(ctx context.Context, c Config) context.Context {
	if c.Token == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, strings.ToLower(c.TokenHeader), c.Token)
}

func SubscribeRequest(c Config, from *uint64, withAccounts bool) *pb.SubscribeRequest {
	commitment := c.CommitmentEnum()
	if c.Subscription != nil {
		p := proto.Clone(c.Subscription).(*pb.SubscribeRequest)
		p.Commitment = &commitment
		p.FromSlot = from
		p.Ping = nil
		if !withAccounts {
			p.Accounts = nil
			p.AccountsDataSlice = nil
		}
		return p
	}
	p := &pb.SubscribeRequest{Commitment: &commitment, FromSlot: from, Transactions: map[string]*pb.SubscribeRequestFilterTransactions{"pump": {Vote: boolp(false), Failed: boolp(false), AccountInclude: append([]string(nil), c.Programs...)}}, Slots: map[string]*pb.SubscribeRequestFilterSlots{"progress": {}}, BlocksMeta: map[string]*pb.SubscribeRequestFilterBlocksMeta{"meta": {}}}
	if withAccounts && c.Mint != "" {
		filter := &pb.SubscribeRequestFilterAccountsFilter{Filter: &pb.SubscribeRequestFilterAccountsFilter_Memcmp{Memcmp: &pb.SubscribeRequestFilterAccountsFilterMemcmp{Offset: 0, Data: &pb.SubscribeRequestFilterAccountsFilterMemcmp_Base58{Base58: c.Mint}}}}
		p.Accounts = map[string]*pb.SubscribeRequestFilterAccounts{"mint_accounts": {Owner: []string{c.TokenAccountOwner}, Filters: []*pb.SubscribeRequestFilterAccountsFilter{filter}}}
	}
	return p
}
func boolp(v bool) *bool { return &v }

// Runner performs one independently reconnecting stream per source. It never
// guesses gaps from sparse transaction slot jumps. Stream interruption always
// creates a durable uncertainty marker, even when replay subsequently succeeds.
type Runner struct {
	Config      Config
	Store       *Store
	QueueBudget *ByteBudget
	Dial        Dialer
	Forced      atomic.Bool
	OnLive      func(Record)
	// OnProvisional runs immediately after Recv/decode, before WAL fsync.
	// It may observe duplicates and data lost in a crash; never treat it as verified.
	OnProvisional   func(Record)
	queueAgeMu      sync.Mutex
	queueOldest     time.Time
	queuePending    int
	queueLastAge    time.Duration
	queueMaxAge     time.Duration
	queueAgeBuckets [8]uint64
}

// WriterQueueStats measures time from receiving an update until its WAL batch
// finishes. A failed batch is also counted: latency never implies durability.
type WriterQueueStats struct {
	OldestAge  time.Duration
	LastAge    time.Duration
	MaxAge     time.Duration
	Pending    int
	AgeBuckets [8]uint64 // <=1,5,10,25,50,100,500 ms, then >500 ms
}

func (r *Runner) WriterQueueStats() WriterQueueStats {
	r.queueAgeMu.Lock()
	defer r.queueAgeMu.Unlock()
	s := WriterQueueStats{LastAge: r.queueLastAge, MaxAge: r.queueMaxAge, Pending: r.queuePending, AgeBuckets: r.queueAgeBuckets}
	if !r.queueOldest.IsZero() {
		s.OldestAge = time.Since(r.queueOldest)
	}
	return s
}

func (r *Runner) writerQueued(at time.Time) {
	if at.IsZero() {
		at = time.Now()
	}
	r.queueAgeMu.Lock()
	if r.queueOldest.IsZero() {
		r.queueOldest = at
	}
	r.queuePending++
	r.queueAgeMu.Unlock()
}

func (r *Runner) writerFlushed(batch []queuedRecord) {
	now := time.Now()
	thresholds := [...]time.Duration{time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond, 25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 500 * time.Millisecond}
	r.queueAgeMu.Lock()
	for _, item := range batch {
		at := item.receivedAt
		if at.IsZero() {
			at = now
		}
		age := now.Sub(at)
		if age < 0 {
			age = 0
		}
		r.queueLastAge = age
		if age > r.queueMaxAge {
			r.queueMaxAge = age
		}
		bucket := len(thresholds)
		for i, limit := range thresholds {
			if age <= limit {
				bucket = i
				break
			}
		}
		r.queueAgeBuckets[bucket]++
	}
	r.queuePending -= len(batch)
	if r.queuePending == 0 {
		r.queueOldest = time.Time{}
	}
	r.queueAgeMu.Unlock()
}

func NewRunner(c Config, s *Store) *Runner {
	queueBytes := c.QueueBytes
	if queueBytes <= 0 {
		queueBytes = 128 << 20
	}
	return &Runner{Config: c, Store: s, QueueBudget: NewByteBudget(queueBytes), Dial: GRPCDial}
}

func (r *Runner) Run(ctx context.Context) error {
	if r.Dial == nil {
		r.Dial = GRPCDial
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, r.Config.Streams)
	var wg sync.WaitGroup
	for i := 0; i < r.Config.Streams; i++ {
		source := string(rune('A' + i))
		if r.Config.SourceID != "" {
			source = r.Config.SourceID
			if r.Config.Streams > 1 {
				source = fmt.Sprintf("%s-%c", r.Config.SourceID, 'A'+i)
			}
		}
		wg.Add(1)
		go func() { defer wg.Done(); errCh <- r.runSource(ctx, source) }()
	}
	var first error
	for i := 0; i < r.Config.Streams; i++ {
		e := <-errCh
		if e != nil && first == nil {
			first = e
			cancel()
		}
	}
	wg.Wait()
	return first
}

func (r *Runner) runSource(ctx context.Context, source string) error {
	var reconnects int
	disableReplay := false
	for {
		if ctx.Err() != nil {
			return nil
		}
		st := r.Store.Snapshot().Sources[source]
		var gen uint64 = 1
		if st != nil {
			gen = st.Generation + 1
		}
		var from *uint64
		if !disableReplay {
			if frontier := replayFrontier(r.Config, st); frontier > 0 {
				v := frontier
				if v > r.Config.ReplayOverlap {
					v -= r.Config.ReplayOverlap
				} else {
					v = 0
				}
				from = &v
			}
			if from == nil && r.Config.FromSlot != nil {
				v := *r.Config.FromSlot
				from = &v
			}
		}
		if err := r.Store.Mark(source, gen, "connection_attempt", fmt.Sprintf("from_slot_requested=%v", fromString(from))); err != nil {
			return err
		}
		err := r.runOnce(ctx, source, gen, from)
		if ctx.Err() != nil {
			if errors.Is(err, errDurableWriterFailed) {
				markErr := r.Store.Mark(source, gen, "continuity_broken", "durable writer failed during shutdown; recovery required")
				return errors.Join(err, markErr)
			}
			return nil
		}
		if err == nil {
			err = io.EOF
		}
		if errors.Is(err, errForcedDrop) {
			r.Forced.Store(true)
		}
		if e := r.Store.Mark(source, gen, "disconnect", sanitizeErr(err, r.Config.Token)); e != nil {
			return e
		}
		if e := r.Store.Mark(source, gen, "continuity_broken", "stream interrupted; replay/RPC reconciliation required: "+sanitizeErr(err, r.Config.Token)); e != nil {
			return e
		}
		if errors.Is(err, errDurableWriterFailed) {
			return err // Storage/integrity failure is terminal; reconnect cannot fix it.
		}
		if r.Config.Logger != nil {
			r.Config.Logger.Warn("source disconnected", "source", source, "generation", gen, "error", sanitizeErr(err, r.Config.Token))
		}
		if from != nil && isReplayRejection(err) {
			if e := r.Store.Mark(source, gen, "replay_rejected", "provider rejected from_slot; coverage cannot be restored without an independent backfill"); e != nil {
				return e
			}
			if !r.Config.AllowLiveFallbackOnReplayRejection {
				return fmt.Errorf("%w: %w", ErrReplayRejected, err)
			}
			disableReplay = true
		} else if isTerminalSubscriptionError(err) {
			// Retrying an invalid subscription or credentials indefinitely cannot
			// restore coverage. The durable disconnect/break markers above retain
			// the exact uncertainty for the caller.
			return err
		}
		reconnects++
		if r.Config.MaxReconnects > 0 && reconnects >= r.Config.MaxReconnects {
			return fmt.Errorf("stream %s max reconnects reached: %s", source, sanitizeErr(err, r.Config.Token))
		}
		wait := reconnectDelay(reconnects)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
	}
}

// replayFrontier returns the minimum observed slot among every update class
// explicitly requested by a custom subscription. A class with no observed
// slot keeps the frontier at zero: unrelated progress must never advance a
// class whose replay position is unknown. The legacy handler subscription is
// transaction-oriented; its optional mint-account filter also constrains it.
func replayFrontier(c Config, st *SourceState) uint64 {
	if st == nil {
		return 0
	}
	var frontiers []uint64
	add := func(requested bool, slot uint64) {
		if requested {
			frontiers = append(frontiers, slot)
		}
	}
	if c.Subscription == nil {
		// The legacy handler profile subscribes to transactions and optionally
		// mint accounts. Its slot and block-meta updates are auxiliary signals,
		// not independent application data classes.
		add(true, st.LastTxSlot)
		add(c.Mint != "", st.LastAccountSlot)
	} else {
		sub := c.Subscription
		add(len(sub.Transactions) > 0, st.LastTxSlot)
		add(len(sub.TransactionsStatus) > 0, st.LastTxStatusSlot)
		add(len(sub.Accounts) > 0, st.LastAccountSlot)
		add(len(sub.Slots) > 0, st.LastSlotUpdateSlot)
		add(len(sub.BlocksMeta) > 0, st.LastMetaSlot)
		add(len(sub.Blocks) > 0, st.LastBlockSlot)
		add(len(sub.Entry) > 0, st.LastEntrySlot)
		add(len(sub.BlockFooter) > 0, st.LastBlockFooterSlot)
	}
	if len(frontiers) == 0 {
		return 0
	}
	frontier := frontiers[0]
	for _, slot := range frontiers[1:] {
		if slot == 0 {
			return 0
		}
		if slot < frontier {
			frontier = slot
		}
	}
	return frontier
}

func (r *Runner) runOnce(parent context.Context, source string, gen uint64, from *uint64) (retErr error) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	conn, err := r.Dial(ctx, r.Config)
	if err != nil {
		return err
	}
	defer conn.Close()
	client := pb.NewGeyserClient(conn)
	ctx = auth(ctx, r.Config)
	if from != nil {
		probeCtx, probeCancel := context.WithTimeout(ctx, 4*time.Second)
		info, e := client.SubscribeReplayInfo(probeCtx, &pb.SubscribeReplayInfoRequest{})
		probeCancel()
		if e == nil {
			if e = r.Store.Mark(source, gen, "replay_info", fmt.Sprintf("first_available=%d requested=%d", info.GetFirstAvailable(), *from)); e != nil {
				return e
			}
			if *from < info.GetFirstAvailable() {
				if e = r.Store.Mark(source, gen, "replay_retention_exceeded", fmt.Sprintf("requested=%d first_available=%d", *from, info.GetFirstAvailable())); e != nil {
					return e
				}
				f := info.GetFirstAvailable()
				from = &f
			}
		} else {
			if e = r.Store.Mark(source, gen, "replay_info_unavailable", status.Convert(e).Code().String()); e != nil {
				return e
			}
		}
	}
	stream, err := client.Subscribe(ctx)
	if err != nil {
		return err
	}
	var sendMu sync.Mutex
	send := func(req *pb.SubscribeRequest) error { sendMu.Lock(); defer sendMu.Unlock(); return stream.Send(req) }
	if err = send(SubscribeRequest(r.Config, from, true)); err != nil {
		return err
	}
	if err = r.Store.Mark(source, gen, "connection_open", fmt.Sprintf("requested_from=%s", fromString(from))); err != nil {
		return err
	}
	// Keep Recv independent of synchronous disk flush. The bounded queue gives
	// explicit backpressure if the writer cannot keep up.
	writeQueue, writeDone, writerStopped, queueBudget := r.startBatchedWriter(cancel)
	defer func() {
		close(writeQueue)
		if writeErr := <-writeDone; writeErr != nil {
			retErr = fmt.Errorf("%w: %w", errDurableWriterFailed, writeErr)
		}
	}()
	var lastRecv atomic.Int64
	lastRecv.Store(time.Now().UnixNano())
	var pingSeq atomic.Int32
	var watchdogErr atomic.Bool
	// Ping/watchdog remains independent from the ordered WAL writer.
	go func() {
		pingTick := time.NewTicker(r.Config.PingInterval)
		defer pingTick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-pingTick.C:
				if time.Since(time.Unix(0, lastRecv.Load())) > r.Config.IdleTimeout {
					watchdogErr.Store(true)
					cancel()
					return
				}
				id := pingSeq.Add(1)
				if e := send(&pb.SubscribeRequest{Ping: &pb.SubscribeRequestPing{Id: id}}); e != nil {
					cancel()
					return
				}
			}
		}
	}()
	observed := 0
	for {
		u, e := stream.Recv()
		if e != nil {
			if watchdogErr.Load() {
				return errIdle
			}
			return e
		}
		receivedAt := time.Now()
		lastRecv.Store(receivedAt.UnixNano())
		if u.GetPing() != nil {
			// SubscribeUpdatePing has no ID. Allocate a client request ID; the
			// server echoes that ID in SubscribeUpdatePong.
			if e = send(&pb.SubscribeRequest{Ping: &pb.SubscribeRequestPing{Id: pingSeq.Add(1)}}); e != nil {
				return e
			}
		}
		// Reserve the encoded protobuf payload before FromUpdate allocates a
		// second full-size byte slice and its envelope. This accounts for the
		// one in-flight update per source during expensive serialization.
		reservedBytes := int64(proto.Size(u)) + 512
		if e = queueBudget.AcquireUntil(context.Background(), reservedBytes, writerStopped); e != nil {
			if errors.Is(e, ErrQueueRecordTooLarge) {
				return e
			}
			return errDurableWriterFailed
		}
		rec, e := FromUpdateAt(r.Config, source, gen, u, receivedAt)
		if e != nil {
			queueBudget.Release(reservedBytes)
			return e
		}
		recordBytes := RecordQueueBytes(rec)
		if recordBytes > reservedBytes {
			_, limit := queueBudget.Snapshot()
			if recordBytes > limit {
				queueBudget.Release(reservedBytes)
				return ErrQueueRecordTooLarge
			}
			if e = queueBudget.AcquireUntil(context.Background(), recordBytes-reservedBytes, writerStopped); e != nil {
				queueBudget.Release(reservedBytes)
				if errors.Is(e, ErrQueueRecordTooLarge) {
					return e
				}
				return errDurableWriterFailed
			}
		} else if recordBytes < reservedBytes {
			queueBudget.Release(reservedBytes - recordBytes)
		}
		if r.OnProvisional != nil {
			r.OnProvisional(rec)
		}
		// Once Recv succeeded, cancellation must not silently discard the
		// observation. The writer drains this queue before runOnce returns.
		// A stopped writer is a fatal error, even during graceful shutdown.
		select {
		case writeQueue <- queuedRecord{record: rec, bytes: recordBytes, receivedAt: receivedAt}:
		case <-writerStopped:
			queueBudget.Release(recordBytes)
			return errDurableWriterFailed
		}
		observed++
		if r.Config.ForceDropAfter > 0 && !r.Forced.Load() && observed >= r.Config.ForceDropAfter {
			return errForcedDrop
		}
	}
}

// Full bounded jitter avoids synchronized retries across clients.
func reconnectDelay(attempt int) time.Duration {
	base := time.Duration(1<<min(max(attempt-1, 0), 4)) * 250 * time.Millisecond
	return base/2 + time.Duration(rand.Int64N(int64(base/2)+1))
}

func fromString(v *uint64) string {
	if v == nil {
		return "none"
	}
	return fmt.Sprint(*v)
}
func sanitizeErr(err error, secrets ...string) string {
	if err == nil {
		return "none"
	}
	message := strings.NewReplacer("\n", " ", "\r", " ").Replace(err.Error())
	for _, secret := range secrets {
		if secret != "" {
			message = strings.ReplaceAll(message, secret, "[REDACTED]")
		}
	}
	return message
}
func isReplayRejection(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.InvalidArgument, codes.OutOfRange, codes.FailedPrecondition, codes.Unimplemented:
	default:
		return false
	}
	// Invalid subscription filters can return the same status codes as an
	// unsupported replay cursor. Preserve the cursor unless the provider's
	// error actually identifies replay or from_slot as the cause.
	message := strings.ToLower(st.Message())
	return strings.Contains(message, "from_slot") || strings.Contains(message, "from slot") || strings.Contains(message, "replay")
}

func isTerminalSubscriptionError(err error) bool {
	switch status.Code(err) {
	case codes.InvalidArgument, codes.Unauthenticated, codes.PermissionDenied:
		return true
	default:
		return false
	}
}
