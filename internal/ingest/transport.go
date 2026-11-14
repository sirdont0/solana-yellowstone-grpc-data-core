package ingest

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var errForcedDrop = errors.New("injected stream disconnect")
var errIdle = errors.New("stream idle watchdog fired")
var errDurableWriterFailed = errors.New("durable WAL writer failed")

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
	return grpc.DialContext(dialCtx, target, grpc.WithBlock(), grpc.WithTransportCredentials(cred), grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 20 * time.Second, Timeout: 10 * time.Second, PermitWithoutStream: true}), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(c.MaxMessageBytes)))
}

func auth(ctx context.Context, c Config) context.Context {
	if c.Token == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, strings.ToLower(c.TokenHeader), c.Token)
}

func SubscribeRequest(c Config, from *uint64, withAccounts bool) *pb.SubscribeRequest {
	commitment := c.CommitmentEnum()
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
	Config Config
	Store  *Store
	Dial   Dialer
	Forced atomic.Bool
	OnLive func(Record)
	// OnProvisional runs immediately after Recv/decode, before WAL fsync.
	// It may observe duplicates and data lost in a crash; never treat it as verified.
	OnProvisional func(Record)
}

func NewRunner(c Config, s *Store) *Runner { return &Runner{Config: c, Store: s, Dial: GRPCDial} }

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
			if st != nil && st.LastSlot > 0 {
				v := st.LastSlot
				if v > r.Config.ReplayOverlap {
					v -= r.Config.ReplayOverlap
				} else {
					v = 0
				}
				from = &v
			}
			if st == nil && r.Config.FromSlot != nil {
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
		fmt.Fprintf(os.Stderr, "stream=%s generation=%d disconnect=%s\n", source, gen, sanitizeErr(err, r.Config.Token))
		if from != nil && isReplayRejection(err) {
			disableReplay = true
			if e := r.Store.Mark(source, gen, "replay_rejected", "provider rejected from_slot; next reconnect starts at live head"); e != nil {
				return e
			}
		} else {
			disableReplay = false
		}
		reconnects++
		if r.Config.MaxReconnects > 0 && reconnects >= r.Config.MaxReconnects {
			return fmt.Errorf("stream %s max reconnects reached: %w", source, err)
		}
		wait := time.Duration(1<<min(reconnects-1, 4)) * 250 * time.Millisecond
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
	}
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
	writeQueue, writeDone, writerStopped := r.startBatchedWriter(cancel)
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
		lastRecv.Store(time.Now().UnixNano())
		if u.GetPing() != nil {
			// Server initiated application ping: acknowledge on the same stream.
			if e = send(&pb.SubscribeRequest{Ping: &pb.SubscribeRequestPing{Id: 1}}); e != nil {
				return e
			}
		}
		rec, e := FromUpdate(r.Config, source, gen, u)
		if e != nil {
			return e
		}
		if r.OnProvisional != nil {
			r.OnProvisional(rec)
		}
		// Once Recv succeeded, cancellation must not silently discard the
		// observation. The writer drains this queue before runOnce returns.
		// A stopped writer is a fatal error, even during graceful shutdown.
		select {
		case writeQueue <- rec:
		case <-writerStopped:
			return errDurableWriterFailed
		}
		observed++
		if r.Config.ForceDropAfter > 0 && !r.Forced.Load() && observed >= r.Config.ForceDropAfter {
			return errForcedDrop
		}
	}
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
	message := strings.ReplaceAll(err.Error(), "\n", " ")
	for _, secret := range secrets {
		if secret != "" {
			message = strings.ReplaceAll(message, secret, "[REDACTED]")
		}
	}
	return message
}
func isReplayRejection(err error) bool {
	c := status.Code(err)
	return c == codes.InvalidArgument || c == codes.OutOfRange || c == codes.FailedPrecondition || c == codes.Unimplemented
}
