package ingest

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/mr-tron/base58"
	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
	"google.golang.org/protobuf/proto"
)

// Transaction contains a normalized read-only view of a received Yellowstone
// transaction. It is an observation, not evidence of complete bank coverage.
// Data slices must be treated as immutable by all handlers.
type Transaction struct {
	Source        string
	Generation    uint64
	Commitment    string
	FilterVersion string
	Slot          uint64
	BankID        uint64 // source-local; never use to compare providers
	Index         uint64
	Signature     string
	Digest        string
	Failed        bool
	Durable       bool
	Instructions  []Instruction
}

// Instruction identifies an outer or inner (CPI) instruction.
// Inner == -1 identifies an outer instruction. Positions are execution-local.
type Instruction struct {
	Outer       uint32
	Inner       int
	ProgramID   string
	Accounts    []string
	Data        []byte
	StackHeight *uint32
}

// DecodeRawTransaction parses the exact protobuf bytes stored in a WAL update.
// There is no Pump.fun or other application-specific decoding in this package.
func DecodeTransaction(raw []byte, source string, durable bool) (*Transaction, error) {
	var update pb.SubscribeUpdate
	if err := proto.Unmarshal(raw, &update); err != nil {
		return nil, fmt.Errorf("decode Yellowstone update: %w", err)
	}
	return normalizeTransaction(&update, source, durable, "")
}

var ErrRecordIntegrity = errors.New("record/protobuf integrity mismatch")

// DecodeTransactionRecord checks the untrusted envelope before applying decode
// policy. This includes metadata-unavailable records, not only decodable ones.
func DecodeTransactionRecord(rec Record, durable bool) (*Transaction, error) {
	var update pb.SubscribeUpdate
	if err := proto.Unmarshal(rec.Raw, &update); err != nil {
		return nil, fmt.Errorf("%w: invalid protobuf", ErrRecordIntegrity)
	}
	t := update.GetTransaction()
	if t == nil || t.GetTransaction() == nil {
		return nil, fmt.Errorf("%w: transaction missing", ErrRecordIntegrity)
	}
	info := t.GetTransaction()
	body, err := proto.MarshalOptions{Deterministic: true}.Marshal(info)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(body)
	digest := hex.EncodeToString(sum[:])
	if t.GetSlot() != rec.Slot || t.GetBankId() != rec.BankID || info.GetIndex() != rec.TxIndex || base58.Encode(info.GetSignature()) != rec.Signature || digest != rec.Digest {
		return nil, fmt.Errorf("%w: identity or digest", ErrRecordIntegrity)
	}
	tx, err := normalizeTransaction(&update, rec.Source, durable, digest)
	if err != nil {
		return nil, err
	}
	tx.Generation, tx.Commitment, tx.FilterVersion = rec.Generation, rec.Commitment, rec.FilterVersion
	return tx, nil
}

func normalizeTransaction(update *pb.SubscribeUpdate, source string, durable bool, digest string) (*Transaction, error) {
	infoUpdate := update.GetTransaction()
	if infoUpdate == nil || infoUpdate.GetTransaction() == nil {
		return nil, fmt.Errorf("not a Yellowstone transaction update")
	}
	info := infoUpdate.GetTransaction()
	message := info.GetTransaction().GetMessage()
	if message == nil {
		return nil, fmt.Errorf("transaction message missing")
	}
	if len(info.GetSignature()) != 64 {
		return nil, fmt.Errorf("invalid transaction signature size %d", len(info.GetSignature()))
	}
	meta := info.GetMeta()
	if meta == nil || meta.GetInnerInstructionsNone() {
		return nil, fmt.Errorf("transaction CPI metadata unavailable")
	}
	keys := make([]string, 0, len(message.GetAccountKeys())+32)
	addKey := func(b []byte) error {
		if len(b) != 32 {
			return fmt.Errorf("invalid account key size %d", len(b))
		}
		keys = append(keys, base58.Encode(b))
		return nil
	}
	for _, b := range message.GetAccountKeys() {
		if err := addKey(b); err != nil {
			return nil, err
		}
	}
	if meta != nil {
		for _, b := range meta.GetLoadedWritableAddresses() {
			if err := addKey(b); err != nil {
				return nil, err
			}
		}
		for _, b := range meta.GetLoadedReadonlyAddresses() {
			if err := addKey(b); err != nil {
				return nil, err
			}
		}
	}
	resolve := func(i uint32) (string, error) {
		if i >= uint32(len(keys)) {
			return "", fmt.Errorf("account index %d outside %d keys; unresolved ALT?", i, len(keys))
		}
		return keys[i], nil
	}
	appendInstruction := func(out *[]Instruction, outer uint32, inner int, pidx uint32, accountIdx []byte, data []byte, height *uint32) error {
		program, err := resolve(pidx)
		if err != nil {
			return err
		}
		accounts := make([]string, 0, len(accountIdx))
		for _, idx := range accountIdx {
			key, err := resolve(uint32(idx))
			if err != nil {
				return err
			}
			accounts = append(accounts, key)
		}
		*out = append(*out, Instruction{Outer: outer, Inner: inner, ProgramID: program, Accounts: accounts, Data: data, StackHeight: height})
		return nil
	}
	tx := &Transaction{Source: source, Slot: infoUpdate.GetSlot(), BankID: infoUpdate.GetBankId(), Index: info.GetIndex(), Signature: base58.Encode(info.GetSignature()), Durable: durable}
	if meta != nil && meta.GetErr() != nil {
		tx.Failed = true
	}
	innerByOuter := make(map[uint32][]*pb.InnerInstruction)
	if meta != nil {
		for _, group := range meta.GetInnerInstructions() {
			if group == nil || int(group.GetIndex()) >= len(message.GetInstructions()) {
				return nil, fmt.Errorf("invalid inner instruction group")
			}
			innerByOuter[group.GetIndex()] = append(innerByOuter[group.GetIndex()], group.GetInstructions()...)
		}
	}
	for i, outer := range message.GetInstructions() {
		if outer == nil {
			return nil, fmt.Errorf("nil outer instruction at %d", i)
		}
		if err := appendInstruction(&tx.Instructions, uint32(i), -1, outer.GetProgramIdIndex(), outer.GetAccounts(), outer.GetData(), nil); err != nil {
			return nil, err
		}
		for j, inner := range innerByOuter[uint32(i)] {
			if inner == nil {
				return nil, fmt.Errorf("nil inner instruction at outer=%d inner=%d", i, j)
			}
			if err := appendInstruction(&tx.Instructions, uint32(i), j, inner.GetProgramIdIndex(), inner.GetAccounts(), inner.GetData(), inner.StackHeight); err != nil {
				return nil, err
			}
		}
		delete(innerByOuter, uint32(i))
	}
	if len(innerByOuter) > 0 {
		return nil, fmt.Errorf("inner instructions refer to nonexistent outer instruction")
	}
	// Deterministic protobuf already canonicalizes absent/empty repeated fields.
	// Preserve *_none: unknown is NOT an authoritative empty list.
	if digest == "" {
		body, err := proto.MarshalOptions{Deterministic: true}.Marshal(info)
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(body)
		digest = hex.EncodeToString(sum[:])
	}
	tx.Digest = digest
	return tx, nil
}
