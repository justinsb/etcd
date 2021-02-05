package etcdserverpb

import (
	"crypto/sha256"
	"testing"

	oldproto "github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/encoding/prototext"
	proto "google.golang.org/protobuf/proto"
)

func checkNewProto(t *testing.T, in proto.Message, out proto.Message) {
	msg, err := proto.Marshal(in)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	hasher := sha256.New()
	hasher.Write(msg)
	hash := hasher.Sum(nil)

	t.Logf("%x\n", hash)

	if err := proto.Unmarshal(msg, out); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	s := prototext.MarshalOptions{Multiline: false}.Format(in)
	t.Logf("%s\n", s)
}

func checkOldProto(t *testing.T, in oldproto.Message, out oldproto.Message) {
	msg, err := oldproto.Marshal(in)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	hasher := sha256.New()
	hasher.Write(msg)
	hash := hasher.Sum(nil)

	t.Logf("%x\n", hash)

	if err := oldproto.Unmarshal(msg, out); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	s := oldproto.CompactTextString(in)
	t.Logf("%s\n", s)
}

func TestMarshal(t *testing.T) {
	in := &RangeRequest{Limit: 123, Key: []byte("hello"), SortOrder: RangeRequest_ASCEND}
	out := &RangeRequest{}

	checkNewProto(t, in, out)
}
