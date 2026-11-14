package core

import lab "github.com/sirdont0/solana-yellowstone-grpc-data-core/internal/ingest"

// Transaction is an immutable source observation, not complete bank coverage.
type Transaction = lab.Transaction

// Instruction identifies an execution occurrence; Inner == -1 means outer.
type Instruction = lab.Instruction

// DecodeRawTransaction normalizes Yellowstone bytes. Missing metadata or
// inner_instructions_none means CPI was not recorded, not that no CPI occurred.
// Known empty CPI (present metadata, false flag, empty list) is accepted.
func DecodeRawTransaction(raw []byte, source string, durable bool) (*Transaction, error) {
	return lab.DecodeTransaction(raw, source, durable)
}

// ErrRecordIntegrity cannot be bypassed by SkipAndFlag.
var ErrRecordIntegrity = lab.ErrRecordIntegrity

func decodeRecord(rec lab.Record, durable bool) (*Transaction, error) {
	return lab.DecodeTransactionRecord(rec, durable)
}
