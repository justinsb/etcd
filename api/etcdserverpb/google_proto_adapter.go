package etcdserverpb

import (
	"bytes"
	"compress/gzip"
	fmt "fmt"
	"io/ioutil"
	"reflect"
	"sync"

	gogoproto "github.com/gogo/protobuf/proto"
	"google.golang.org/protobuf/encoding/protowire"
	proto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/runtime/protoiface"
	"google.golang.org/protobuf/types/descriptorpb"
	"k8s.io/klog"
)

type gogoMessage interface {
	Descriptor() (gz []byte, path []int)
	Size() int
	Reset()
	Unmarshal([]byte) error
	MarshalToSizedBuffer([]byte) (int, error)
}

type protoreflectAdapter struct {
	msg       gogoMessage
	reflector *protoMessageReflector
}

var typeCache protoreflectAdapterCache

type protoreflectAdapterCache struct {
	mutex sync.Mutex
	cache map[string]*protoMessageReflector
}

func (c *protoreflectAdapterCache) getProtoMessageReflector(msg gogoMessage) *protoMessageReflector {
	vt := reflect.TypeOf(msg)
	vt = vt.Elem()

	key := vt.Name()

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.cache == nil {
		c.cache = make(map[string]*protoMessageReflector)
	}
	r, found := c.cache[key]
	if found {
		return r
	}

	r, err := newProtoMessageReflector(vt, msg)
	if err != nil {
		panic(fmt.Sprintf("failed to build type information: %v", err))
	}
	c.cache[key] = r

	return r
}

type protoMessageReflector struct {
	msgDesc        protoreflect.MessageDescriptor
	gogoProps      *gogoproto.StructProperties
	fieldsByNumber map[protowire.Number]*protoField
}

type protoField struct {
	reflectFieldIndex int
	fd                protoreflect.FieldDescriptor
	has               func(msg reflect.Value, field *protoField) bool
	get               func(msg reflect.Value, field *protoField) protoreflect.Value
}

func (r *protoMessageReflector) Range(msg gogoMessage, f func(protoreflect.FieldDescriptor, protoreflect.Value) bool) {
	v := reflect.ValueOf(msg)
	v = v.Elem()

	for _, field := range r.fieldsByNumber {
		val := field.get(v, field)
		if !f(field.fd, val) {
			break
		}
	}
}

func (r *protoMessageReflector) Has(msg gogoMessage, fd protoreflect.FieldDescriptor) bool {
	v := reflect.ValueOf(msg)
	v = v.Elem()

	fieldProtoNumber := fd.Number()
	field := r.fieldsByNumber[fieldProtoNumber]
	if field == nil {
		panic(fmt.Sprintf("field not found with matching tag for %v", fd))
	}

	return field.has(v, field)
}

func (r *protoMessageReflector) Get(msg gogoMessage, fd protoreflect.FieldDescriptor) protoreflect.Value {
	v := reflect.ValueOf(msg)
	v = v.Elem()

	fieldProtoNumber := fd.Number()
	field := r.fieldsByNumber[fieldProtoNumber]
	if field == nil {
		panic(fmt.Sprintf("field not found with matching tag for %v", fd))
	}

	return field.get(v, field)
}

func (a *protoreflectAdapter) Reset() {
	a.msg.Reset()
}

func hasNotNilPointer(msg reflect.Value, field *protoField) bool {
	// TODO: Skip and just return true if not a pointer?  Check for zero values?
	fieldValue := msg.Field(field.reflectFieldIndex)
	return fieldValue.Kind() != reflect.Ptr || !fieldValue.IsNil()
}

func getEnumValue(msg reflect.Value, field *protoField) protoreflect.Value {
	fieldValue := msg.Field(field.reflectFieldIndex)
	return protoreflect.ValueOfEnum(protoreflect.EnumNumber(fieldValue.Int()))
}

func getInt64Value(msg reflect.Value, field *protoField) protoreflect.Value {
	fieldValue := msg.Field(field.reflectFieldIndex)
	return protoreflect.ValueOfInt64(fieldValue.Int())
}
func getBoolValue(msg reflect.Value, field *protoField) protoreflect.Value {
	fieldValue := msg.Field(field.reflectFieldIndex)
	return protoreflect.ValueOfBool(fieldValue.Bool())
}

func getBytesValue(msg reflect.Value, field *protoField) protoreflect.Value {
	fieldValue := msg.Field(field.reflectFieldIndex)
	return protoreflect.ValueOfBytes(fieldValue.Bytes())
}

// var val protoreflect.Value
// 	if fd.IsList() {
// 		val = protoreflect.ValueOfList(&listAdapter{fieldValue})
// 	} else {
// 		switch fd.Kind() {
// 		case protoreflect.EnumKind:
// 			return protoreflect.ValueOfEnum(protoreflect.EnumNumber(fieldValue.Int()))
// 		case protoreflect.Uint64Kind:
// 			val = protoreflect.ValueOfUint64(fieldValue.Uint())
// 		case protoreflect.Int64Kind:
// 			val = protoreflect.ValueOfInt64(fieldValue.Int())
// 		case protoreflect.BoolKind:
// 			val = protoreflect.ValueOfBool(fieldValue.Bool())
// 		case protoreflect.BytesKind:
// 			val = protoreflect.ValueOfBytes(fieldValue.Bytes())
// 		case protoreflect.MessageKind:
// 			if fieldValue.Kind() != reflect.Ptr {
// 				fieldValue = fieldValue.Addr()
// 			}
// 			i := fieldValue.Interface()
// 			gogo, ok := i.(gogoMessage)
// 			if !ok {
// 				panic(fmt.Sprintf("message type %T does not implement gogoMessage", i))
// 			}
// 			val = protoreflect.ValueOfMessage(&protoreflectAdapter{msg: gogo})
// 		default:
// 			panic(fmt.Sprintf("unhandled protoField kind %q", fd.Kind()))
// 		}
// 	}

// }

func newProtoMessageReflector(vt reflect.Type, msg gogoMessage) (*protoMessageReflector, error) {
	fd, md := /*gogodescriptor.*/ forMessage(msg.Descriptor())

	var resolver protodesc.Resolver
	rfd, err := protodesc.FileOptions{AllowUnresolvable: true}.New(fd, resolver)
	if err != nil {
		return nil, fmt.Errorf("cannot parse FileDescriptor: %v", err)
	}
	messageName := protoreflect.Name(md.GetName())
	msgDesc := rfd.Messages().ByName(messageName)
	if msgDesc == nil {
		return nil, fmt.Errorf("cannot get message %q: %v", messageName, err)
	}

	r := &protoMessageReflector{
		msgDesc:        msgDesc,
		fieldsByNumber: make(map[protowire.Number]*protoField),
	}

	klog.Infof("reflect on %T", msg)

	r.gogoProps = gogoproto.GetProperties(vt)

	for _, prop := range r.gogoProps.Prop {
		if prop.Tag == 0 {
			continue
		}

		structField, found := vt.FieldByName(prop.Name)
		if !found {
			panic(fmt.Sprintf("cannot find struct field %q: %+v", prop.Name, prop))
		}

		fd := msgDesc.Fields().ByName(protoreflect.Name(prop.OrigName))
		if fd == nil {
			panic(fmt.Sprintf("cannot find proto field %q: %+v", prop.OrigName, prop))
		}

		f := &protoField{
			reflectFieldIndex: structField.Index[0],
			fd:                fd,
		}

		if fd.IsList() {
			panic(fmt.Sprintf("unhandled protoField list kind %v", fd))
			// val = protoreflect.ValueOfList(&listAdapter{fieldValue})
		} else {
			switch fd.Kind() {
			case protoreflect.EnumKind:
				f.has = hasNotNilPointer
				f.get = getEnumValue

			case protoreflect.BoolKind:
				f.has = hasNotNilPointer
				f.get = getBoolValue

			case protoreflect.Int64Kind:
				f.has = hasNotNilPointer
				f.get = getInt64Value

			case protoreflect.BytesKind:
				f.has = hasNotNilPointer
				f.get = getBytesValue

			// case protoreflect.MessageKind:
			// 	if fieldValue.Kind() != reflect.Ptr {
			// 		fieldValue = fieldValue.Addr()
			// 	}
			// 	i := fieldValue.Interface()
			// 	gogo, ok := i.(gogoMessage)
			// 	if !ok {
			// 		panic(fmt.Sprintf("message type %T does not implement gogoMessage", i))
			// 	}
			// 	val = protoreflect.ValueOfMessage(&protoreflectAdapter{msg: gogo})
			default:
				panic(fmt.Sprintf("unhandled protoField kind %v", fd))
			}
		}

		r.fieldsByNumber[protowire.Number(prop.Tag)] = f
	}

	return r, nil
}

func (r *protoMessageReflector) Descriptor() protoreflect.MessageDescriptor {
	return r.msgDesc
}

// extractFile extracts a FileDescriptorProto from a gzip'd buffer.
func extractFile(gz []byte) (*descriptorpb.FileDescriptorProto, error) {
	r, err := gzip.NewReader(bytes.NewReader(gz))
	if err != nil {
		return nil, fmt.Errorf("failed to open gzip reader: %v", err)
	}
	defer r.Close()

	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to uncompress descriptor: %v", err)
	}

	fd := new(descriptorpb.FileDescriptorProto)
	if err := proto.Unmarshal(b, fd); err != nil {
		return nil, fmt.Errorf("malformed FileDescriptorProto: %v", err)
	}

	return fd, nil
}

// forMessage returns a FileDescriptorProto and a DescriptorProto from within it
// describing the given message.
func forMessage(gz []byte, path []int) (fd *descriptorpb.FileDescriptorProto, md *descriptorpb.DescriptorProto) {
	// gz, path := msg.Descriptor()
	fd, err := extractFile(gz)
	if err != nil {
		panic(fmt.Sprintf("invalid FileDescriptorProto: %v", err))
	}

	md = fd.MessageType[path[0]]
	for _, i := range path[1:] {
		md = md.NestedType[i]
	}
	return fd, md
}

var _ protoreflect.Message = &protoreflectAdapter{}

// Clear clears the field such that a subsequent Has call reports false.
//
// Clearing an extension field clears both the extension type and value
// associated with the given field number.
//
// Clear is a mutating operation and unsafe for concurrent use.
func (a *protoreflectAdapter) Clear(fd protoreflect.FieldDescriptor) {
	klog.Infof("Clear(%+v)", fd)
	panic("not implemented")
}

func (a *protoreflectAdapter) getReflector() *protoMessageReflector {
	reflector := a.reflector
	if a.reflector == nil {
		reflector = typeCache.getProtoMessageReflector(a.msg)
		a.reflector = reflector
	}
	return reflector
}

// Descriptor returns message descriptor, which contains only the protobuf
// type information for the message.
func (a *protoreflectAdapter) Descriptor() protoreflect.MessageDescriptor {
	return a.getReflector().Descriptor()
}

// Type returns the message type, which encapsulates both Go and protobuf
// type information. If the Go type information is not needed,
// it is recommended that the message descriptor be used instead.
func (a *protoreflectAdapter) Type() protoreflect.MessageType {
	return &adapterMessageType{a}
}

// New returns a newly allocated and mutable empty message.
func (a *protoreflectAdapter) New() protoreflect.Message {
	panic("not implemented")
}

// ProtoMethods returns optional fast-path implementions of various operations.
// This method may return nil.
//
// The returned methods type is identical to
// "google.golang.org/protobuf/runtime/protoiface".Methods.
// Consult the protoiface package documentation for details.
func (a *protoreflectAdapter) ProtoMethods() *protoiface.Methods {
	return &protoiface.Methods{
		Marshal:          a.marshal,
		Unmarshal:        a.unmarshal,
		CheckInitialized: a.checkInitialized,
	}
}

func (a *protoreflectAdapter) marshal(input protoiface.MarshalInput) (protoiface.MarshalOutput, error) {
	if input.Flags != 0 {
		return protoiface.MarshalOutput{}, fmt.Errorf("flags not supported: %+v", input)
	}

	buffer := input.Buf
	size := a.msg.Size()
	if len(buffer) > size {
		buffer = buffer[:size]
	}
	if len(buffer) < size {
		buffer = make([]byte, size)
	}
	_, err := a.msg.MarshalToSizedBuffer(buffer)
	if err != nil {
		return protoiface.MarshalOutput{}, err
	}
	return protoiface.MarshalOutput{
		Buf: buffer,
	}, nil
}

func (a *protoreflectAdapter) unmarshal(input protoiface.UnmarshalInput) (protoiface.UnmarshalOutput, error) {
	if input.Flags != 0 {
		return protoiface.UnmarshalOutput{}, fmt.Errorf("flags not supported: %+v", input)
	}

	err := a.msg.Unmarshal(input.Buf)
	if err != nil {
		return protoiface.UnmarshalOutput{}, err
	}
	return protoiface.UnmarshalOutput{}, nil
}

func (a *protoreflectAdapter) checkInitialized(input protoiface.CheckInitializedInput) (protoiface.CheckInitializedOutput, error) {
	klog.Infof("TODO: checkInitialized should check that fields are initialized")
	return protoiface.CheckInitializedOutput{}, nil
}

// Get retrieves the value for a field.
//
// For unpopulated scalars, it returns the default value, where
// the default value of a bytes scalar is guaranteed to be a copy.
// For unpopulated composite types, it returns an empty, read-only view
// of the value; to obtain a mutable reference, use Mutable.
func (a *protoreflectAdapter) Get(fd protoreflect.FieldDescriptor) protoreflect.Value {
	return a.getReflector().Get(a.msg, fd)
}

// GetUnknown retrieves the entire list of unknown fields.
// The caller may only mutate the contents of the RawFields
// if the mutated bytes are stored back into the message with SetUnknown.
func (a *protoreflectAdapter) GetUnknown() protoreflect.RawFields {
	return nil
	// panic("not implemented")
}

// SetUnknown stores an entire list of unknown fields.
// The raw fields must be syntactically valid according to the wire format.
// An implementation may panic if this is not the case.
// Once stored, the caller must not mutate the content of the RawFields.
// An empty RawFields may be passed to clear the fields.
//
// SetUnknown is a mutating operation and unsafe for concurrent use.
func (a *protoreflectAdapter) SetUnknown(protoreflect.RawFields) {
	panic("not implemented")
}

// Interface unwraps the message reflection interface and
// returns the underlying ProtoMessage interface.
func (a *protoreflectAdapter) Interface() protoreflect.ProtoMessage {
	return a
}

func (a *protoreflectAdapter) ProtoReflect() protoreflect.Message {
	return a
}

// Range iterates over every populated field in an undefined order,
// calling f for each field descriptor and value encountered.
// Range returns immediately if f returns false.
// While iterating, mutating operations may only be performed
// on the current field descriptor.
func (a *protoreflectAdapter) Range(f func(protoreflect.FieldDescriptor, protoreflect.Value) bool) {
	a.getReflector().Range(a.msg, f)
}

// Has reports whether a field is populated.
//
// Some fields have the property of nullability where it is possible to
// distinguish between the default value of a field and whether the field
// was explicitly populated with the default value. Singular message fields,
// member fields of a oneof, and proto2 scalar fields are nullable. Such
// fields are populated only if explicitly set.
//
// In other cases (aside from the nullable cases above),
// a proto3 scalar field is populated if it contains a non-zero value, and
// a repeated field is populated if it is non-empty.
func (a *protoreflectAdapter) Has(fd protoreflect.FieldDescriptor) bool {
	return a.getReflector().Has(a.msg, fd)
}

// IsValid reports whether the message is valid.
//
// An invalid message is an empty, read-only value.
//
// An invalid message often corresponds to a nil pointer of the concrete
// message type, but the details are implementation dependent.
// Validity is not part of the protobuf data model, and may not
// be preserved in marshaling or other operations.
func (a *protoreflectAdapter) IsValid() bool {
	klog.Infof("TODO: IsValid")
	return true
}

// Set stores the value for a field.
//
// For a field belonging to a oneof, it implicitly clears any other field
// that may be currently set within the same oneof.
// For extension fields, it implicitly stores the provided ExtensionType.
// When setting a composite type, it is unspecified whether the stored value
// aliases the source's memory in any way. If the composite value is an
// empty, read-only value, then it panics.
//
// Set is a mutating operation and unsafe for concurrent use.
func (a *protoreflectAdapter) Set(protoreflect.FieldDescriptor, protoreflect.Value) {
	panic("not implemented")
}

// Mutable returns a mutable reference to a composite type.
//
// If the field is unpopulated, it may allocate a composite value.
// For a field belonging to a oneof, it implicitly clears any other field
// that may be currently set within the same oneof.
// For extension fields, it implicitly stores the provided ExtensionType
// if not already stored.
// It panics if the field does not contain a composite type.
//
// Mutable is a mutating operation and unsafe for concurrent use.
func (a *protoreflectAdapter) Mutable(protoreflect.FieldDescriptor) protoreflect.Value {
	panic("not implemented")
	return protoreflect.Value{}
}

// NewField returns a new value that is assignable to the field
// for the given descriptor. For scalars, this returns the default value.
// For lists, maps, and messages, this returns a new, empty, mutable value.
func (a *protoreflectAdapter) NewField(protoreflect.FieldDescriptor) protoreflect.Value {
	panic("not implemented")
	return protoreflect.Value{}
}

// WhichOneof reports which field within the oneof is populated,
// returning nil if none are populated.
// It panics if the oneof descriptor does not belong to this message.
func (a *protoreflectAdapter) WhichOneof(protoreflect.OneofDescriptor) protoreflect.FieldDescriptor {
	panic("not implemented")
	return nil
}

type listAdapter struct {
	slice reflect.Value
}

var _ protoreflect.List = &listAdapter{}

// Len reports the number of entries in the List.
// Get, Set, and Truncate panic with out of bound indexes.
func (a *listAdapter) Len() int {
	return a.slice.Len()
}

// Get retrieves the value at the given index.
// It never returns an invalid value.
func (a *listAdapter) Get(int) protoreflect.Value {
	panic("not implemented")
}

// Set stores a value for the given index.
// When setting a composite type, it is unspecified whether the set
// value aliases the source's memory in any way.
//
// Set is a mutating operation and unsafe for concurrent use.
func (a *listAdapter) Set(int, protoreflect.Value) {
	panic("not implemented")

}

// Append appends the provided value to the end of the list.
// When appending a composite type, it is unspecified whether the appended
// value aliases the source's memory in any way.
//
// Append is a mutating operation and unsafe for concurrent use.
func (a *listAdapter) Append(protoreflect.Value) {
	panic("not implemented")

}

// AppendMutable appends a new, empty, mutable message value to the end
// of the list and returns it.
// It panics if the list does not contain a message type.
func (a *listAdapter) AppendMutable() protoreflect.Value {
	panic("not implemented")
}

// Truncate truncates the list to a smaller length.
//
// Truncate is a mutating operation and unsafe for concurrent use.
func (a *listAdapter) Truncate(int) {
	panic("not implemented")
}

// NewElement returns a new value for a list element.
// For enums, this returns the first enum value.
// For other scalars, this returns the zero value.
// For messages, this returns a new, empty, mutable value.
func (a *listAdapter) NewElement() protoreflect.Value {
	panic("not implemented")
}

// IsValid reports whether the list is valid.
//
// An invalid list is an empty, read-only value.
//
// Validity is not part of the protobuf data model, and may not
// be preserved in marshaling or other operations.
func (a *listAdapter) IsValid() bool {
	panic("not implemented")
}

type adapterMessageType struct {
	a *protoreflectAdapter
}

var _ protoreflect.MessageType = &adapterMessageType{}

// New returns a newly allocated empty message.
func (a *adapterMessageType) New() protoreflect.Message {
	panic("not implemented")
}

// Zero returns an empty, read-only message.
func (a *adapterMessageType) Zero() protoreflect.Message {
	panic("not implemented")
}

// Descriptor returns the message descriptor.
//
// Invariant: t.Descriptor() == t.New().Descriptor()
func (a *adapterMessageType) Descriptor() protoreflect.MessageDescriptor {
	return a.a.Descriptor()
}
