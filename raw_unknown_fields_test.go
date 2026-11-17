package core

import (
	"bytes"
	"context"
	"testing"

	lab "github.com/sirdont0/solana-yellowstone-grpc-data-core/internal/ingest"
	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// Newer Yellowstone providers may add fields before this module updates its
// protobuf definitions. Raw WAL must preserve these fields for later decoders.
func TestRawWALPreservesUnknownProtobufFields(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(map[int]string{1: "v1", 2: "v2"}[version], func(t *testing.T) {
			cfg := configForTest(t)
			cfg.WALVersion = version
			if version == 2 {
				cfg.WALSegmentBytes = 128 << 20
			}
			e, err := NewRaw(cfg)
			if err != nil {
				t.Fatal(err)
			}
			update := &pb.SubscribeUpdate{UpdateOneof: &pb.SubscribeUpdate_Slot{Slot: &pb.SubscribeUpdateSlot{Slot: 444}}}
			outerUnknown := protowire.AppendBytes(protowire.AppendTag(nil, 19000, protowire.BytesType), []byte("future-top-level"))
			innerUnknown := protowire.AppendVarint(protowire.AppendTag(nil, 19001, protowire.VarintType), 123456)
			update.ProtoReflect().SetUnknown(outerUnknown)
			update.GetSlot().ProtoReflect().SetUnknown(innerUnknown)
			lcfg := toLabConfig(e.cfg, e.cfg.Sources[0])
			store, err := lab.OpenStore(lcfg)
			if err != nil {
				t.Fatal(err)
			}
			rec, err := lab.FromUpdate(lcfg, "a", 1, update)
			if err != nil {
				t.Fatal(err)
			}
			if accepted, err := store.Append(rec); err != nil || !accepted {
				t.Fatalf("append accepted=%v err=%v", accepted, err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			e.stores["a"] = store
			var found bool
			if err := e.ReadRaw(context.Background(), "a", RawCursor{}, func(ob RawObservation) error {
				if ob.Type != "update" {
					return nil
				}
				found = true
				var got pb.SubscribeUpdate
				if err := proto.Unmarshal(ob.Payload, &got); err != nil {
					return err
				}
				if !bytes.Equal(got.ProtoReflect().GetUnknown(), outerUnknown) || !bytes.Equal(got.GetSlot().ProtoReflect().GetUnknown(), innerUnknown) {
					t.Fatalf("unknown protobuf fields were lost on raw WAL replay")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if !found {
				t.Fatal("missing raw update")
			}
		})
	}
}
