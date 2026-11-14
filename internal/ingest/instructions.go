package ingest

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/mr-tron/base58"
	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
	"google.golang.org/protobuf/proto"
)

// InstructionOccurrence is a stable *execution position*, not a decoded Pump
// trade. Event-CPI and log events may both refer to the same economic action;
// that semantic deduplication requires a validated decoder and fixture oracle.
type InstructionOccurrence struct {
	Source      string  `json:"source"`
	Slot        uint64  `json:"slot"`
	BankID      uint64  `json:"bank_id"`
	Signature   string  `json:"signature"`
	TxIndex     uint64  `json:"tx_index"`
	OuterIndex  uint32  `json:"outer_index"`
	InnerIndex  int     `json:"inner_index"` // -1 for outer instruction
	ProgramID   string  `json:"program_id"`
	DataSHA256  string  `json:"data_sha256"`
	StackHeight *uint32 `json:"stack_height,omitempty"`
	ID          string  `json:"id"`
}

func Occurrences(r Record) ([]InstructionOccurrence, error) {
	if r.Kind != "transaction" {
		return nil, nil
	}
	var u pb.SubscribeUpdate
	if e := proto.Unmarshal(r.Raw, &u); e != nil {
		return nil, e
	}
	t := u.GetTransaction()
	if t == nil || t.GetTransaction() == nil {
		return nil, fmt.Errorf("transaction update missing info")
	}
	info := t.GetTransaction()
	msg := info.GetTransaction().GetMessage()
	if msg == nil {
		return nil, fmt.Errorf("transaction message missing")
	}
	meta := info.GetMeta()
	if meta == nil || meta.GetInnerInstructionsNone() {
		return nil, fmt.Errorf("transaction CPI metadata unavailable")
	}
	keys := append([][]byte{}, msg.GetAccountKeys()...)
	if meta := info.GetMeta(); meta != nil {
		keys = append(keys, meta.GetLoadedWritableAddresses()...)
		keys = append(keys, meta.GetLoadedReadonlyAddresses()...)
	}
	resolve := func(i uint32) (string, error) {
		if int(i) >= len(keys) {
			return "", fmt.Errorf("program index %d outside %d keys (possible missing ALT resolution)", i, len(keys))
		}
		if len(keys[i]) != 32 {
			return "", fmt.Errorf("invalid pubkey byte count at %d", i)
		}
		return base58.Encode(keys[i]), nil
	}
	var out []InstructionOccurrence
	appendAt := func(outer uint32, inner int, index uint32, data []byte, height *uint32) error {
		program, e := resolve(index)
		if e != nil {
			return e
		}
		sum := sha256.Sum256(data)
		o := InstructionOccurrence{Source: r.Source, Slot: r.Slot, BankID: r.BankID, Signature: r.Signature, TxIndex: r.TxIndex, OuterIndex: outer, InnerIndex: inner, ProgramID: program, DataSHA256: hex.EncodeToString(sum[:]), StackHeight: height}
		o.ID = fmt.Sprintf("%s|%s|%s|%d|%d|%d|%s|%d|%d|%s", r.Source, r.Commitment, r.FilterVersion, r.Slot, r.BankID, r.TxIndex, r.Signature, outer, inner, program)
		out = append(out, o)
		return nil
	}
	innerByOuter := make(map[uint32][]*pb.InnerInstruction)
	for _, grp := range meta.GetInnerInstructions() {
		if grp == nil || int(grp.GetIndex()) >= len(msg.GetInstructions()) {
			return nil, fmt.Errorf("invalid inner instruction group")
		}
		innerByOuter[grp.GetIndex()] = append(innerByOuter[grp.GetIndex()], grp.GetInstructions()...)
	}
	for i, ix := range msg.GetInstructions() {
		if ix == nil {
			return nil, fmt.Errorf("nil outer instruction at %d", i)
		}
		if e := appendAt(uint32(i), -1, ix.GetProgramIdIndex(), ix.GetData(), nil); e != nil {
			return nil, e
		}
		for j, inner := range innerByOuter[uint32(i)] {
			if inner == nil {
				return nil, fmt.Errorf("nil inner instruction at outer=%d", i)
			}
			if e := appendAt(uint32(i), j, inner.GetProgramIdIndex(), inner.GetData(), inner.StackHeight); e != nil {
				return nil, e
			}
		}
		delete(innerByOuter, uint32(i))
	}
	return out, nil
}
