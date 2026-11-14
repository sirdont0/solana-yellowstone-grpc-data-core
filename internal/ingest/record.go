package ingest

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/mr-tron/base58"
	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
	"google.golang.org/protobuf/proto"
)

type Record struct {
	Type string `json:"type"`
	// WALOwner is set only when a record is copied to the merged-order WAL.
	// Source remains untouched so source-empty lifecycle markers stay local.
	WALOwner      string    `json:"wal_owner,omitempty"`
	Time          time.Time `json:"time"`
	Source        string    `json:"source,omitempty"`
	Generation    uint64    `json:"generation,omitempty"`
	Kind          string    `json:"kind,omitempty"`
	Slot          uint64    `json:"slot,omitempty"`
	BankID        uint64    `json:"bank_id,omitempty"`
	Signature     string    `json:"signature,omitempty"`
	TxIndex       uint64    `json:"tx_index,omitempty"`
	Account       string    `json:"account,omitempty"`
	WriteVersion  uint64    `json:"write_version,omitempty"`
	ParentSlot    uint64    `json:"parent_slot,omitempty"`
	SlotStatus    string    `json:"slot_status,omitempty"`
	Blockhash     string    `json:"blockhash,omitempty"`
	Commitment    string    `json:"commitment,omitempty"`
	FilterVersion string    `json:"filter_version,omitempty"`
	Identity      string    `json:"identity,omitempty"`
	DedupeKey     string    `json:"dedupe_key,omitempty"`
	Digest        string    `json:"digest,omitempty"`
	Detail        string    `json:"detail,omitempty"`
	Raw           []byte    `json:"raw,omitempty"`
}

func Marker(c Config, source string, generation uint64, kind, detail string) Record {
	return Record{Type: "marker", Time: time.Now().UTC(), Source: source, Generation: generation, Kind: kind, Detail: detail, Commitment: c.Commitment, FilterVersion: c.FilterVersion, Identity: c.Identity()}
}

func FromUpdate(c Config, source string, generation uint64, u *pb.SubscribeUpdate) (Record, error) {
	b, e := proto.Marshal(u)
	if e != nil {
		return Record{}, e
	}
	r := Record{Type: "update", Time: time.Now().UTC(), Source: source, Generation: generation, Kind: "other", Commitment: c.Commitment, FilterVersion: c.FilterVersion, Identity: c.Identity(), Raw: b}
	var identityBody proto.Message
	switch {
	case u.GetTransaction() != nil:
		t := u.GetTransaction()
		r.Kind = "transaction"
		r.Slot = t.GetSlot()
		r.BankID = t.GetBankId()
		if t.GetTransaction() != nil {
			r.Signature = base58.Encode(t.GetTransaction().GetSignature())
			r.TxIndex = t.GetTransaction().GetIndex()
			identityBody = t.GetTransaction()
		}
	case u.GetTransactionStatus() != nil:
		t := u.GetTransactionStatus()
		r.Kind = "transaction_status"
		r.BankID = t.GetBankId()
		r.Slot = t.GetSlot()
		r.Signature = base58.Encode(t.GetSignature())
		r.TxIndex = t.GetIndex()
		identityBody = t
	case u.GetAccount() != nil:
		a := u.GetAccount()
		r.Kind = "account"
		r.Slot = a.GetSlot()
		r.BankID = a.GetBankId()
		if a.GetAccount() != nil {
			r.Account = base58.Encode(a.GetAccount().GetPubkey())
			r.WriteVersion = a.GetAccount().GetWriteVersion()
			identityBody = a.GetAccount()
		}
	case u.GetSlot() != nil:
		s := u.GetSlot()
		r.Kind = "slot"
		r.Slot = s.GetSlot()
		r.ParentSlot = s.GetParent()
		r.SlotStatus = s.GetStatus().String()
		r.BankID = s.GetBankId()
		identityBody = s
	case u.GetBlockMeta() != nil:
		m := u.GetBlockMeta()
		r.Kind = "block_meta"
		r.Slot = m.GetSlot()
		r.BankID = m.GetBankId()
		r.ParentSlot = m.GetParentSlot()
		r.Blockhash = m.GetBlockhash()
		identityBody = m
	case u.GetBlockFooter() != nil:
		f := u.GetBlockFooter()
		r.Kind = "block_footer"
		r.BankID = f.GetBankId()
		r.Slot = f.GetSlot()
		identityBody = f
	case u.GetPing() != nil:
		r.Kind = "ping"
	case u.GetPong() != nil:
		r.Kind = "pong"
	case u.GetEntry() != nil:
		r.Kind = "entry"
		r.Slot = u.GetEntry().GetSlot()
		identityBody = u.GetEntry()
	case u.GetBlock() != nil:
		r.Kind = "block"
		r.Slot = u.GetBlock().GetSlot()
		identityBody = u.GetBlock()
	}
	if identityBody != nil {
		v, e := proto.MarshalOptions{Deterministic: true}.Marshal(identityBody)
		if e != nil {
			return r, e
		}
		sum := sha256.Sum256(v)
		r.Digest = hex.EncodeToString(sum[:])
	}
	switch r.Kind {
	case "transaction", "transaction_status":
		// A reconnect can select a different validator with a different
		// source-local bank_id for the same finalized transaction.
		r.DedupeKey = fmt.Sprintf("%s|%s|%s|%s|%d|%d|%s", r.Source, r.Commitment, r.FilterVersion, r.Kind, r.Slot, r.TxIndex, r.Signature)
	case "account":
		// write_version is validator-local; never compare it across
		// reconnect generations as an occurrence identity.
		r.DedupeKey = fmt.Sprintf("%s|%s|%s|account|%d|%d|%d|%s|%d", r.Source, r.Commitment, r.FilterVersion, r.Generation, r.Slot, r.BankID, r.Account, r.WriteVersion)
	case "slot", "block_meta", "block_footer":
		r.DedupeKey = fmt.Sprintf("%s|%s|%s|%s|%d|%d|%s", r.Source, r.Commitment, r.FilterVersion, r.Kind, r.Slot, r.BankID, r.SlotStatus)
	}
	return r, nil
}
