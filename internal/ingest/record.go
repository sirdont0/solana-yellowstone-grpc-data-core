package ingest

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/mr-tron/base58"
	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
	"google.golang.org/protobuf/proto"
)

type Record struct {
	Type string `json:"type"`
	// Sequence is the 1-based append position within one WAL. Older v1 records
	// omit it; readers derive their position from the immutable frame order.
	Sequence uint64 `json:"sequence,omitempty"`
	// JournalID distinguishes a recreated journal from an earlier journal at
	// the same path. It is assigned by Store, not by the provider.
	JournalID string `json:"journal_id,omitempty"`
	// WALOwner is set only when a record is copied to the merged-order WAL.
	// Source remains untouched so source-empty lifecycle markers stay local.
	WALOwner        string    `json:"wal_owner,omitempty"`
	Time            time.Time `json:"time"`
	SourceCreatedAt time.Time `json:"source_created_at,omitempty"`
	Source          string    `json:"source,omitempty"`
	Generation      uint64    `json:"generation,omitempty"`
	Kind            string    `json:"kind,omitempty"`
	Slot            uint64    `json:"slot,omitempty"`
	BankID          uint64    `json:"bank_id,omitempty"`
	BankIDPresent   bool      `json:"bank_id_present,omitempty"`
	Failed          bool      `json:"failed,omitempty"`
	StatusPresent   bool      `json:"status_present,omitempty"`
	FeeLamports     uint64    `json:"fee_lamports,omitempty"`
	FeePresent      bool      `json:"fee_present,omitempty"`
	Signature       string    `json:"signature,omitempty"`
	TxIndex         uint64    `json:"tx_index,omitempty"`
	Account         string    `json:"account,omitempty"`
	WriteVersion    uint64    `json:"write_version,omitempty"`
	ParentSlot      uint64    `json:"parent_slot,omitempty"`
	SlotStatus      string    `json:"slot_status,omitempty"`
	Blockhash       string    `json:"blockhash,omitempty"`
	Commitment      string    `json:"commitment,omitempty"`
	FilterVersion   string    `json:"filter_version,omitempty"`
	Identity        string    `json:"identity,omitempty"`
	DedupeKey       string    `json:"dedupe_key,omitempty"`
	Digest          string    `json:"digest,omitempty"`
	Detail          string    `json:"detail,omitempty"`
	Raw             []byte    `json:"raw,omitempty"`
}

func Marker(c Config, source string, generation uint64, kind, detail string) Record {
	if len(detail) > 4096 {
		detail = detail[:4096] + "...[truncated]"
	}
	return Record{Type: "marker", Time: time.Now().UTC(), Source: source, Generation: generation, Kind: kind, Detail: detail, Commitment: c.Commitment, FilterVersion: c.FilterVersion, Identity: c.Identity()}
}

func rawPayloadFitsWAL(rawBytes int) bool {
	return rawBytes >= 0 && base64.StdEncoding.EncodedLen(rawBytes)+maxRecordMetadataBytes <= maxFrame
}

func FromUpdate(c Config, source string, generation uint64, u *pb.SubscribeUpdate) (Record, error) {
	return FromUpdateAt(c, source, generation, u, time.Now())
}

// FromUpdateAt preserves the moment Recv returned, even if queue admission or
// protobuf serialization takes time. Record.Time backs RawObservation.ReceivedAt.
func FromUpdateAt(c Config, source string, generation uint64, u *pb.SubscribeUpdate, receivedAt time.Time) (Record, error) {
	b, e := proto.Marshal(u)
	if e != nil {
		return Record{}, e
	}
	if !rawPayloadFitsWAL(len(b)) {
		return Record{}, fmt.Errorf("serialized Yellowstone update %d bytes cannot fit a v1 WAL frame (max frame %d bytes)", len(b), maxFrame)
	}
	r := Record{Type: "update", Time: receivedAt.UTC(), Source: source, Generation: generation, Kind: "other", Commitment: c.Commitment, FilterVersion: c.FilterVersion, Identity: c.Identity(), Raw: b}
	if created := u.GetCreatedAt(); created != nil && created.IsValid() {
		r.SourceCreatedAt = created.AsTime().UTC()
	}
	var identityBody proto.Message
	switch {
	case u.GetTransaction() != nil:
		t := u.GetTransaction()
		r.Kind = "transaction"
		r.Slot = t.GetSlot()
		r.BankID = t.GetBankId()
		r.BankIDPresent = t.GetBankId() != 0
		if t.GetTransaction() != nil {
			r.Signature = base58.Encode(t.GetTransaction().GetSignature())
			r.TxIndex = t.GetTransaction().GetIndex()
			if meta := t.GetTransaction().GetMeta(); meta != nil {
				r.StatusPresent = true
				r.Failed = meta.GetErr() != nil
				r.FeeLamports = meta.GetFee()
				r.FeePresent = true
			}
			identityBody = t.GetTransaction()
		}
	case u.GetTransactionStatus() != nil:
		t := u.GetTransactionStatus()
		r.Kind = "transaction_status"
		r.BankID = t.GetBankId()
		r.BankIDPresent = t.GetBankId() != 0
		r.Slot = t.GetSlot()
		r.Signature = base58.Encode(t.GetSignature())
		r.TxIndex = t.GetIndex()
		r.StatusPresent = true
		r.Failed = t.GetErr() != nil
		identityBody = t
	case u.GetAccount() != nil:
		a := u.GetAccount()
		r.Kind = "account"
		r.Slot = a.GetSlot()
		r.BankID = a.GetBankId()
		r.BankIDPresent = a.BankId != nil
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
		r.BankIDPresent = s.BankId != nil
		identityBody = s
	case u.GetBlockMeta() != nil:
		m := u.GetBlockMeta()
		r.Kind = "block_meta"
		r.Slot = m.GetSlot()
		r.BankID = m.GetBankId()
		r.BankIDPresent = m.GetBankId() != 0
		r.ParentSlot = m.GetParentSlot()
		r.Blockhash = m.GetBlockhash()
		identityBody = m
	case u.GetBlockFooter() != nil:
		f := u.GetBlockFooter()
		r.Kind = "block_footer"
		r.BankID = f.GetBankId()
		r.BankIDPresent = f.GetBankId() != 0
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
	// Non-finalized observations are intentionally source-local. Processed and
	// confirmed forks may contain different transactions at the same slot/index;
	// they must remain raw observations instead of becoming dedupe conflicts.
	if c.Commitment != "finalized" {
		return r, nil
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
