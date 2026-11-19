package yellowstonepb_test

import (
	"crypto/sha256"
	"fmt"
	"testing"

	pb "github.com/sirdont0/solana-yellowstone-grpc-data-core/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// These hashes identify the reviewed local, namespaced Yellowstone 14.0.1
// descriptors. Schema changes must explicitly update this pin after review.
// Upstream immutable source/blob references are recorded in PROTO_NOTICE.md.
func TestReviewedYellowstoneSchemaDescriptorPin(t *testing.T) {
	for _, tc := range []struct {
		name string
		file protoreflect.FileDescriptor
		want string
	}{
		{
			name: "geyser",
			file: pb.File_solanacore_geyser_proto,
			want: "20146a8e752395fa29404cb7577b1455626aa6f99ddab8d663f56fc380e43f4d",
		},
		{
			name: "storage",
			file: pb.File_solanacore_storage_proto,
			want: "fb8563bc72daf6d1996ae5b00e5315dece42619b407ab1160f21aa364d110ec5",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(protodesc.ToFileDescriptorProto(tc.file))
			if err != nil {
				t.Fatal(err)
			}
			if got := fmt.Sprintf("%x", sha256.Sum256(encoded)); got != tc.want {
				t.Fatalf("Yellowstone descriptor changed without a pin update: got %s want %s", got, tc.want)
			}
		})
	}
}
