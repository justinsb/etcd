package etcdserverpb

import (
	proto "github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var _ proto.Message = &RangeRequest{}

func (m *RangeRequest) ProtoReflect() protoreflect.Message {
	return &protoreflectAdapter{msg: m}
}
