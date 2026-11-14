package ingest

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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
	tx, err := DecodeTransactionRecord(r, true)
	if err != nil {
		return nil, err
	}
	out := make([]InstructionOccurrence, 0, len(tx.Instructions))
	for _, ix := range tx.Instructions {
		sum := sha256.Sum256(ix.Data)
		o := InstructionOccurrence{Source: r.Source, Slot: tx.Slot, BankID: tx.BankID, Signature: tx.Signature, TxIndex: tx.Index, OuterIndex: ix.Outer, InnerIndex: ix.Inner, ProgramID: ix.ProgramID, DataSHA256: hex.EncodeToString(sum[:]), StackHeight: ix.StackHeight}
		o.ID = fmt.Sprintf("%s|%s|%s|%d|%d|%d|%s|%d|%d|%s", r.Source, r.Commitment, r.FilterVersion, r.Slot, r.BankID, r.TxIndex, r.Signature, ix.Outer, ix.Inner, ix.ProgramID)
		out = append(out, o)
	}
	return out, nil
}
