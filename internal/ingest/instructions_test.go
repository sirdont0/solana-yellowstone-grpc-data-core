package ingest

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/mr-tron/base58"
	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
)

func occurrenceKey(t *testing.T, address string) []byte {
	t.Helper()
	b, err := base58.Decode(address)
	if err != nil || len(b) != 32 {
		t.Fatalf("invalid fixture pubkey %q: %v", address, err)
	}
	return b
}

func occurrenceTransaction(t *testing.T, msg *pb.Message, meta *pb.TransactionStatusMeta) Record {
	t.Helper()
	c := integrationConfig(t, "grpc://127.0.0.1:1")
	sig := make([]byte, 64)
	sig[0] = 7
	u := &pb.SubscribeUpdate{UpdateOneof: &pb.SubscribeUpdate_Transaction{Transaction: &pb.SubscribeUpdateTransaction{
		Slot: 1234, BankId: 42, Transaction: &pb.SubscribeUpdateTransactionInfo{
			Signature: sig, Index: 9,
			Transaction: &pb.Transaction{Message: msg}, Meta: meta,
		},
	}}}
	r, err := FromUpdate(c, "A", 3, u)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestOccurrencesOuterInnerCPIAndALTResolution(t *testing.T) {
	// Solana's executable indices use static keys, then loaded writable keys,
	// then loaded readonly keys. An ALT lookup itself does not resolve keys.
	static := [][]byte{occurrenceKey(t, PumpProgram), occurrenceKey(t, PumpSwapProgram)}
	writable := occurrenceKey(t, TokenProgram)
	readonly := make([]byte, 32)
	for i := range readonly {
		readonly[i] = byte(i + 1)
	}
	readonlyProgram := base58.Encode(readonly)
	height := uint32(3)
	msg := &pb.Message{
		Versioned: true, AccountKeys: static,
		AddressTableLookups: []*pb.MessageAddressTableLookup{{AccountKey: static[0], WritableIndexes: []byte{0}, ReadonlyIndexes: []byte{1}}},
		Instructions: []*pb.CompiledInstruction{
			{ProgramIdIndex: 0, Data: []byte("outer-0")},
			{ProgramIdIndex: 2, Data: []byte("outer-1-loaded-writable")},
			{ProgramIdIndex: 1, Data: []byte("outer-2")},
		},
	}
	meta := &pb.TransactionStatusMeta{
		LoadedWritableAddresses: [][]byte{writable}, LoadedReadonlyAddresses: [][]byte{readonly},
		InnerInstructions: []*pb.InnerInstructions{
			{Index: 0, Instructions: []*pb.InnerInstruction{
				{ProgramIdIndex: 1, Data: []byte("cpi-same")},
				{ProgramIdIndex: 1, Data: []byte("cpi-same"), StackHeight: &height},
				{ProgramIdIndex: 3, Data: []byte("readonly-cpi")},
			}},
			{Index: 2, Instructions: []*pb.InnerInstruction{
				{ProgramIdIndex: 2, Data: []byte("writable-cpi")},
			}},
		},
	}
	r := occurrenceTransaction(t, msg, meta)
	got, err := Occurrences(r)
	if err != nil {
		t.Fatal(err)
	}
	// Output follows execution order: an outer instruction immediately followed
	// by its CPI instructions, with inner indices scoped to that outer.
	want := []struct {
		outer   uint32
		inner   int
		program string
		data    string
		height  *uint32
	}{
		{0, -1, PumpProgram, "outer-0", nil},
		{0, 0, PumpSwapProgram, "cpi-same", nil},
		{0, 1, PumpSwapProgram, "cpi-same", &height},
		{0, 2, readonlyProgram, "readonly-cpi", nil},
		{1, -1, TokenProgram, "outer-1-loaded-writable", nil},
		{2, -1, PumpSwapProgram, "outer-2", nil},
		{2, 0, TokenProgram, "writable-cpi", nil},
	}
	if len(got) != len(want) {
		t.Fatalf("occurrences=%d, want %d: %+v", len(got), len(want), got)
	}
	seen := make(map[string]bool)
	for i, w := range want {
		o := got[i]
		sum := sha256.Sum256([]byte(w.data))
		id := fmt.Sprintf("A|%s|%s|1234|42|9|%s|%d|%d|%s", r.Commitment, r.FilterVersion, r.Signature, w.outer, w.inner, w.program)
		if o.Source != "A" || o.Slot != 1234 || o.BankID != 42 || o.TxIndex != 9 || o.Signature != r.Signature || o.OuterIndex != w.outer || o.InnerIndex != w.inner || o.ProgramID != w.program || o.DataSHA256 != hex.EncodeToString(sum[:]) || o.ID != id || !reflect.DeepEqual(o.StackHeight, w.height) {
			t.Errorf("occurrence[%d] mismatch: got=%+v want=%+v id=%s", i, o, w, id)
		}
		if seen[o.ID] {
			t.Errorf("two execution positions collapsed into same ID: %s", o.ID)
		}
		seen[o.ID] = true
	}
	// CPI 0 and CPI 1 invoke the same program with identical data; their
	// distinct instruction positions must survive economic-event dedupe later.
	if got[1].DataSHA256 != got[2].DataSHA256 || got[1].ID == got[2].ID {
		t.Fatalf("distinct CPI positions were not preserved: %+v %+v", got[1], got[2])
	}
	again, err := Occurrences(r)
	if err != nil || !reflect.DeepEqual(got, again) {
		t.Fatalf("same WAL transaction must produce stable occurrences: err=%v", err)
	}
}

func TestOccurrencesRejectMissingALTAndInvalidProgramIndex(t *testing.T) {
	static := occurrenceKey(t, PumpProgram)
	loaded := occurrenceKey(t, PumpSwapProgram)
	cases := []struct {
		name      string
		message   *pb.Message
		meta      *pb.TransactionStatusMeta
		wantError string
	}{
		{
			name: "missing ALT resolution for outer",
			message: &pb.Message{Versioned: true, AccountKeys: [][]byte{static},
				AddressTableLookups: []*pb.MessageAddressTableLookup{{AccountKey: static, WritableIndexes: []byte{0}}},
				Instructions:        []*pb.CompiledInstruction{{ProgramIdIndex: 1}}},
			meta:      &pb.TransactionStatusMeta{},
			wantError: "account index 1 outside 1 keys",
		},
		{
			name:    "out-of-range inner CPI index",
			message: &pb.Message{AccountKeys: [][]byte{static}, Instructions: []*pb.CompiledInstruction{{ProgramIdIndex: 0}}},
			meta: &pb.TransactionStatusMeta{LoadedWritableAddresses: [][]byte{loaded},
				InnerInstructions: []*pb.InnerInstructions{{Index: 0, Instructions: []*pb.InnerInstruction{{ProgramIdIndex: 2}}}}},
			wantError: "account index 2 outside 2 keys",
		},
		{
			name:      "invalid loaded address length",
			message:   &pb.Message{AccountKeys: [][]byte{static}, Instructions: []*pb.CompiledInstruction{{ProgramIdIndex: 1}}},
			meta:      &pb.TransactionStatusMeta{LoadedReadonlyAddresses: [][]byte{{1, 2, 3}}},
			wantError: "invalid account key size 3",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := occurrenceTransaction(t, tc.message, tc.meta)
			got, err := Occurrences(r)
			if err == nil || !strings.Contains(err.Error(), tc.wantError) || len(got) != 0 {
				t.Fatalf("got=%+v err=%v, expected %q and no partial output", got, err, tc.wantError)
			}
		})
	}
}

func TestOccurrencesRejectInvalidRecords(t *testing.T) {
	if got, err := Occurrences(Record{Kind: "block_meta", Raw: []byte("not protobuf")}); err != nil || len(got) != 0 {
		t.Fatalf("non-transactions should have no occurrences: got=%v err=%v", got, err)
	}
	if _, err := Occurrences(Record{Kind: "transaction", Raw: []byte{0xff}}); err == nil {
		t.Fatal("malformed protobuf accepted")
	}
	r := occurrenceTransaction(t, nil, nil)
	if _, err := Occurrences(r); err == nil || !strings.Contains(err.Error(), "transaction message missing") {
		t.Fatalf("missing transaction message: %v", err)
	}
}

func TestOccurrencesRejectsUnavailableOrMalformedCPI(t *testing.T) {
	program := occurrenceKey(t, PumpProgram)
	msg := &pb.Message{AccountKeys: [][]byte{program}, Instructions: []*pb.CompiledInstruction{{ProgramIdIndex: 0}}}
	for _, tc := range []struct {
		name string
		meta *pb.TransactionStatusMeta
	}{
		{"missing_meta", nil},
		{"not_recorded", &pb.TransactionStatusMeta{InnerInstructionsNone: true}},
		{"invalid_group_index", &pb.TransactionStatusMeta{InnerInstructions: []*pb.InnerInstructions{{Index: 99, Instructions: []*pb.InnerInstruction{{ProgramIdIndex: 0}}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := occurrenceTransaction(t, msg, tc.meta)
			if _, err := Occurrences(r); err == nil {
				t.Fatal("incomplete or malformed CPI accepted")
			}
		})
	}
}

func TestOccurrencesSplitCPIGroupsKeepDistinctPositions(t *testing.T) {
	program := occurrenceKey(t, PumpProgram)
	msg := &pb.Message{AccountKeys: [][]byte{program}, Instructions: []*pb.CompiledInstruction{{ProgramIdIndex: 0}}}
	meta := &pb.TransactionStatusMeta{InnerInstructions: []*pb.InnerInstructions{
		{Index: 0, Instructions: []*pb.InnerInstruction{{ProgramIdIndex: 0, Data: []byte("a")}}},
		{Index: 0, Instructions: []*pb.InnerInstruction{{ProgramIdIndex: 0, Data: []byte("b")}}},
	}}
	got, err := Occurrences(occurrenceTransaction(t, msg, meta))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[1].InnerIndex != 0 || got[2].InnerIndex != 1 || got[1].ID == got[2].ID {
		t.Fatalf("split groups collapsed CPI positions: %+v", got)
	}
}
