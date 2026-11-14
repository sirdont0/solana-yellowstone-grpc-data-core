package pumpfuncreate

import (
	"bytes"
	"encoding/binary"
	"testing"

	core "github.com/sirdont0/solana-yellowstone-grpc-data-core"
)

func TestCreateEventCPIKnownPrefixAndUnknownTail(t *testing.T) {
	var body bytes.Buffer
	str := func(s string) { _ = binary.Write(&body, binary.LittleEndian, uint32(len(s))); body.WriteString(s) }
	key := func(n byte) { body.Write(bytes.Repeat([]byte{n}, 32)) }
	u64 := func(n uint64) { _ = binary.Write(&body, binary.LittleEndian, n) }
	str("Test")
	str("TEST")
	str("https://example.test/metadata.json")
	key(1)
	key(2)
	key(3)
	key(4)
	u64(1750000000)
	u64(100)
	u64(200)
	u64(300)
	u64(400)
	key(5)
	body.WriteByte(1)
	body.WriteByte(0)
	key(6)
	u64(700)
	body.Write([]byte{9, 9})
	data := append(append([]byte{}, eventCPITag...), createEventTag...)
	data = append(data, body.Bytes()...)
	h := New()
	out, err := h.Decode(&core.Transaction{}, core.Instruction{ProgramID: ProgramID, Inner: 0, Data: data})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0].Kind != "pumpfun.create" {
		t.Fatalf("out=%+v", out)
	}
	ev := out[0].Payload.(Create)
	if ev.Name != "Test" || ev.Symbol != "TEST" || ev.Supply != 400 || ev.TrailingBytes != 2 || !ev.MayhemMode {
		t.Fatalf("wrong fields: %+v", ev)
	}
	out, err = h.Decode(&core.Transaction{}, core.Instruction{ProgramID: ProgramID, Inner: -1, Data: data})
	if err != nil || len(out) != 0 {
		t.Fatal("outer instruction incorrectly considered event CPI")
	}
	_, err = h.Decode(&core.Transaction{}, core.Instruction{ProgramID: ProgramID, Inner: 0, Data: data[:20]})
	if err == nil {
		t.Fatal("truncated known event accepted")
	}
}

func TestBorshReaderRejectsInvalidUTF8(t *testing.T) {
	r := &reader{data: []byte{1, 0, 0, 0, 0xff}}
	if _, err := r.string(); err == nil {
		t.Fatal("invalid UTF-8 Borsh string was accepted")
	}
}
