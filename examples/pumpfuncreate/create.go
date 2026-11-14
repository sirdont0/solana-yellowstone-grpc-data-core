// Package pumpfuncreate is a minimal example protocol handler. It is kept
// outside the engine and can be replaced independently by an application.
package pumpfuncreate

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/mr-tron/base58"
	core "github.com/sirdont0/solana-yellowstone-grpc-data-core"
)

const ProgramID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

var eventCPITag = []byte{0xe4, 0x45, 0xa5, 0x2e, 0x51, 0xcb, 0x9a, 0x1d}
var createEventTag = []byte{27, 114, 169, 77, 222, 235, 99, 118}

// Create describes the independently decoded, known prefix of Pump.fun
// CreateEvent. TrailingBytes records an unverified extension; no unknown field
// is silently interpreted.
type Create struct {
	Name                string `json:"name"`
	Symbol              string `json:"symbol"`
	URI                 string `json:"uri"`
	Mint                string `json:"mint"`
	BondingCurve        string `json:"bonding_curve"`
	Creator             string `json:"creator"`
	TokenProgram        string `json:"token_program"`
	QuoteMint           string `json:"quote_mint"`
	Timestamp           int64  `json:"timestamp"`
	VirtualTokenReserve uint64 `json:"virtual_token_reserve"`
	VirtualSOLReserve   uint64 `json:"virtual_sol_reserve"`
	RealTokenReserve    uint64 `json:"real_token_reserve"`
	Supply              uint64 `json:"supply"`
	VirtualQuoteReserve uint64 `json:"virtual_quote_reserve"`
	MayhemMode          bool   `json:"mayhem_mode"`
	TrailingBytes       int    `json:"trailing_bytes"`
}

type Handler struct{}

func New() *Handler                   { return &Handler{} }
func (*Handler) ProgramIDs() []string { return []string{ProgramID} }

func (*Handler) Decode(_ *core.Transaction, ix core.Instruction) ([]core.Emission, error) {
	if ix.ProgramID != ProgramID || ix.Inner < 0 {
		return nil, nil
	}
	if len(ix.Data) < 16 || !bytes.Equal(ix.Data[:8], eventCPITag) || !bytes.Equal(ix.Data[8:16], createEventTag) {
		return nil, nil
	}
	create, err := ParseCreateEvent(ix.Data[16:])
	if err != nil {
		return nil, err
	}
	return []core.Emission{{Kind: "pumpfun.create", Payload: create}}, nil
}

type reader struct {
	data []byte
	off  int
}

func (r *reader) take(n int) ([]byte, error) {
	if n < 0 || n > len(r.data)-r.off {
		return nil, errors.New("truncated Borsh CreateEvent")
	}
	b := r.data[r.off : r.off+n]
	r.off += n
	return b, nil
}
func (r *reader) u32() (uint32, error) {
	b, e := r.take(4)
	if e != nil {
		return 0, e
	}
	return binary.LittleEndian.Uint32(b), nil
}
func (r *reader) u64() (uint64, error) {
	b, e := r.take(8)
	if e != nil {
		return 0, e
	}
	return binary.LittleEndian.Uint64(b), nil
}
func (r *reader) string() (string, error) {
	n, e := r.u32()
	if e != nil {
		return "", e
	}
	if n > 8192 {
		return "", fmt.Errorf("Borsh string exceeds 8192-byte example limit: %d", n)
	}
	b, e := r.take(int(n))
	if e != nil {
		return "", e
	}
	if !utf8.Valid(b) {
		return "", errors.New("invalid UTF-8 in Borsh string")
	}
	return string(b), nil
}
func (r *reader) key() (string, error) {
	b, e := r.take(32)
	if e != nil {
		return "", e
	}
	return base58.Encode(b), nil
}
func (r *reader) boolean() (bool, error) {
	b, e := r.take(1)
	if e != nil {
		return false, e
	}
	if b[0] > 1 {
		return false, fmt.Errorf("invalid Borsh bool %d", b[0])
	}
	return b[0] == 1, nil
}

// ParseCreateEvent decodes the currently known CreateEvent prefix. Upstream
// IDL upgrades must be validated with fresh fixtures before changing fields.
func ParseCreateEvent(body []byte) (Create, error) {
	r := &reader{data: body}
	var c Create
	var err error
	if c.Name, err = r.string(); err != nil {
		return c, err
	}
	if c.Symbol, err = r.string(); err != nil {
		return c, err
	}
	if c.URI, err = r.string(); err != nil {
		return c, err
	}
	if c.Mint, err = r.key(); err != nil {
		return c, err
	}
	if c.BondingCurve, err = r.key(); err != nil {
		return c, err
	}
	if c.Creator, err = r.key(); err != nil {
		return c, err
	}
	if _, err = r.key(); err != nil {
		return c, err
	} // creator vault / reserved pubkey in pinned layout
	timestamp, err := r.u64()
	if err != nil {
		return c, err
	}
	c.Timestamp = int64(timestamp)
	if c.VirtualTokenReserve, err = r.u64(); err != nil {
		return c, err
	}
	if c.VirtualSOLReserve, err = r.u64(); err != nil {
		return c, err
	}
	if c.RealTokenReserve, err = r.u64(); err != nil {
		return c, err
	}
	if c.Supply, err = r.u64(); err != nil {
		return c, err
	}
	if c.TokenProgram, err = r.key(); err != nil {
		return c, err
	}
	if c.MayhemMode, err = r.boolean(); err != nil {
		return c, err
	}
	if _, err = r.boolean(); err != nil {
		return c, err
	} // reserved flag in pinned layout
	if c.QuoteMint, err = r.key(); err != nil {
		return c, err
	}
	if c.VirtualQuoteReserve, err = r.u64(); err != nil {
		return c, err
	}
	c.TrailingBytes = len(body) - r.off
	return c, nil
}
