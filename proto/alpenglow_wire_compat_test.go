package yellowstonepb_test

import (
	"bytes"
	"testing"

	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// These builders deliberately use numeric wire tags rather than generated
// message types. A descriptor error must not make its own test fixture pass.
func wireVarint(dst []byte, field protowire.Number, value uint64) []byte {
	dst = protowire.AppendTag(dst, field, protowire.VarintType)
	return protowire.AppendVarint(dst, value)
}

func wireBytes(dst []byte, field protowire.Number, value []byte) []byte {
	dst = protowire.AppendTag(dst, field, protowire.BytesType)
	return protowire.AppendBytes(dst, value)
}

func TestAlpenglowTypedFooterWireCompatibility(t *testing.T) {
	// Yellowstone 14.0.1+ wire shape: footer certificates are fields 9/10/11.
	// A finalization cert may describe a slot other than its carrier footer.
	aggregate := wireVarint(nil, 1, 0) // COMPRESSED_BLS12_381_G2
	aggregate = wireBytes(aggregate, 2, []byte{0x11, 0x22})
	aggregate = wireBytes(aggregate, 3, []byte{0xaa, 0xbb})
	aggregate = wireBytes(aggregate, 4, []byte{0x01, 0x02})

	finalCert := wireVarint(nil, 1, 499)
	finalCert = wireBytes(finalCert, 2, aggregate)
	skipCert := wireVarint(nil, 1, 500)
	skipCert = wireBytes(skipCert, 2, aggregate)
	notarCert := wireVarint(nil, 1, 501)
	notarCert = wireBytes(notarCert, 2, aggregate)

	footer := wireVarint(nil, 1, 500)
	footer = wireVarint(footer, 2, 17)
	footer = wireBytes(footer, 3, []byte{0xcc})
	footer = wireBytes(footer, 9, finalCert)
	footer = wireBytes(footer, 10, skipCert)
	footer = wireBytes(footer, 11, notarCert)
	footer = wireVarint(footer, 99, 42)      // future unknown field
	updateWire := wireBytes(nil, 12, footer) // SubscribeUpdate.block_footer

	var got pb.SubscribeUpdate
	if err := proto.Unmarshal(updateWire, &got); err != nil {
		t.Fatal(err)
	}
	blockFooter := got.GetBlockFooter()
	if blockFooter == nil || blockFooter.GetSlot() != 500 || blockFooter.GetBankId() != 17 {
		t.Fatalf("wrong carrier footer: %+v", blockFooter)
	}
	final := blockFooter.GetBlockFinalCert()
	if final == nil || final.GetSlot() != 499 || final.GetFinalAggregate() == nil {
		t.Fatalf("typed finalization cert was not decoded: %+v", final)
	}
	if !bytes.Equal(final.GetFinalAggregate().GetBlockId(), []byte{0xaa, 0xbb}) {
		t.Fatal("finalization cert block ID was not decoded")
	}
	if blockFooter.GetSkipRewardCert() == nil || blockFooter.GetSkipRewardCert().GetSlot() != 500 {
		t.Fatal("skip reward certificate was not decoded")
	}
	if blockFooter.GetNotarRewardCert() == nil || blockFooter.GetNotarRewardCert().GetSlot() != 501 {
		t.Fatal("notarization reward certificate was not decoded")
	}
	if len(blockFooter.ProtoReflect().GetUnknown()) == 0 {
		t.Fatal("future footer field was dropped")
	}

	encoded, err := proto.Marshal(&got)
	if err != nil {
		t.Fatal(err)
	}
	var replay pb.SubscribeUpdate
	if err := proto.Unmarshal(encoded, &replay); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(&got, &replay) {
		t.Fatal("raw protobuf round-trip changed typed certificates or unknown fields")
	}
}

func TestLegacyFooterFieldsRemainUnknownAndNoCertIsRequired(t *testing.T) {
	// Older, pre-14.0.1 encodings used wincode bytes at 6/7/8.
	legacy := wireVarint(nil, 1, 400)
	legacy = wireBytes(legacy, 6, []byte{0xaa})
	legacy = wireBytes(legacy, 7, []byte{0xbb})
	legacy = wireBytes(legacy, 8, []byte{0xcc})
	var got pb.SubscribeUpdateBlockFooter
	if err := proto.Unmarshal(legacy, &got); err != nil {
		t.Fatal(err)
	}
	if got.GetBlockFinalCert() != nil || got.GetSkipRewardCert() != nil || got.GetNotarRewardCert() != nil {
		t.Fatal("legacy untyped certificate must not be interpreted as a typed certificate")
	}
	if len(got.ProtoReflect().GetUnknown()) == 0 {
		t.Fatal("legacy 6/7/8 bytes were not retained as unknown fields")
	}
	reencoded, err := proto.Marshal(&got)
	if err != nil {
		t.Fatal(err)
	}
	var restored pb.SubscribeUpdateBlockFooter
	if err := proto.Unmarshal(reencoded, &restored); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(restored.ProtoReflect().GetUnknown(), got.ProtoReflect().GetUnknown()) {
		t.Fatal("legacy unknown certificate fields did not survive replay")
	}

	// In Alpenglow, missing certificates are normal, not a coverage gap.
	var empty pb.SubscribeUpdateBlockFooter
	if err := proto.Unmarshal(wireVarint(nil, 1, 401), &empty); err != nil {
		t.Fatal(err)
	}
	if empty.GetSlot() != 401 || empty.GetBlockFinalCert() != nil {
		t.Fatal("valid certificate-free footer was rejected")
	}
}

func TestAlpenglowRawPreservesUnknownSignatureKindAndIncompleteCertificate(t *testing.T) {
	// Protobuf must not drop a source observation merely because the certificate
	// is not yet semantically verifiable. Raw preservation is distinct from
	// signature validation, which is intentionally outside this Core.
	unknownAggregate := wireVarint(nil, 1, 999)
	finalCert := wireVarint(nil, 1, 499)
	finalCert = wireBytes(finalCert, 2, unknownAggregate)
	footer := wireVarint(nil, 1, 500)
	footer = wireBytes(footer, 9, finalCert)
	footer = wireBytes(footer, 6, []byte{0xaa}) // legacy and typed coexist

	var got pb.SubscribeUpdateBlockFooter
	if err := proto.Unmarshal(footer, &got); err != nil {
		t.Fatal(err)
	}
	cert := got.GetBlockFinalCert()
	if cert == nil || cert.GetFinalAggregate() == nil || cert.GetFinalAggregate().GetSignatureKind() != pb.BlockFooterSignatureKind(999) {
		t.Fatal("unknown signature-kind enum was dropped or coerced")
	}
	if len(cert.GetFinalAggregate().GetSignature()) != 0 {
		t.Fatal("incomplete certificate unexpectedly acquired a signature")
	}
	if len(got.ProtoReflect().GetUnknown()) == 0 {
		t.Fatal("legacy field was lost when typed certificate coexisted")
	}
	encoded, err := proto.Marshal(&got)
	if err != nil {
		t.Fatal(err)
	}
	var replay pb.SubscribeUpdateBlockFooter
	if err := proto.Unmarshal(encoded, &replay); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(&got, &replay) {
		t.Fatal("incomplete/unknown certificate changed during raw protobuf replay")
	}
}

func TestAlpenglowDescriptorAndYellowstoneWirePath(t *testing.T) {
	d := (&pb.SubscribeUpdateBlockFooter{}).ProtoReflect().Descriptor()
	for _, n := range []protowire.Number{6, 7, 8} {
		if d.Fields().ByNumber(protoreflect.FieldNumber(n)) != nil || !d.ReservedRanges().Has(protoreflect.FieldNumber(n)) {
			t.Fatalf("legacy footer field %d must be reserved", n)
		}
	}
	for _, n := range []protowire.Number{9, 10, 11} {
		f := d.Fields().ByNumber(protoreflect.FieldNumber(n))
		if f == nil || f.Kind() != protoreflect.MessageKind {
			t.Fatalf("footer field %d must be a typed certificate message", n)
		}
	}
	if pb.Geyser_Subscribe_FullMethodName != "/geyser.Geyser/Subscribe" ||
		pb.Geyser_SubscribeReplayInfo_FullMethodName != "/geyser.Geyser/SubscribeReplayInfo" {
		t.Fatal("namespaced generated client must preserve Yellowstone gRPC service paths")
	}
}

func TestYellowstoneRequiredRawUpdateKindsWireCompatibility(t *testing.T) {
	// Outer field numbers are supplied independently of generated Go structs.
	// This verifies wire admission of every currently supported SubscribeUpdate
	// oneof kind. It does not claim provider-side support for every filter.
	for _, tc := range []struct {
		kind  string
		field protowire.Number
	}{
		{"account", 2},
		{"slot", 3},
		{"transaction", 4},
		{"transaction_status", 10},
		{"block", 5},
		{"ping", 6},
		{"pong", 9},
		{"block_meta", 7},
		{"entry", 8},
		{"block_footer", 12},
		{"entry_update_parent", 13},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			// Empty embedded message is wire-valid for every oneof variant;
			// field-level behavior is covered by the certificate fixtures above.
			encoded := wireBytes(nil, tc.field, nil)
			var got pb.SubscribeUpdate
			if err := proto.Unmarshal(encoded, &got); err != nil {
				t.Fatal(err)
			}
			refl := got.ProtoReflect()
			oneof := refl.Descriptor().Oneofs().ByName("update_oneof")
			field := refl.WhichOneof(oneof)
			if field == nil || field.Number() != protoreflect.FieldNumber(tc.field) {
				t.Fatalf("wire field %d did not decode to expected kind %s", tc.field, tc.kind)
			}
			roundTrip, err := proto.Marshal(&got)
			if err != nil {
				t.Fatal(err)
			}
			var replay pb.SubscribeUpdate
			if err := proto.Unmarshal(roundTrip, &replay); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(&got, &replay) {
				t.Fatal("raw update changed across protobuf wire round-trip")
			}
		})
	}
}
