// A proto for capturing a trace.

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v3.21.8
// source: proto/trace.proto

package tracepb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// The KeyValue message defines a key and value pair.
// The key is always a string. The value for the field is determined by the ValueType.
// If the ValueType is STRING the field v_str should be set, for BOOL v_bool is used etc..
// Only the type of valueType is used. Rest of the fields are ignored even if they are set.
// So, of v_type is ValueType.STRING, only the value v_str is used. Rest of the fields are ignored.
// We chose not to use OneOf field, since it's JSON encoding is not as straight forward.
type ValueType int32

const (
	ValueType_STRING  ValueType = 0
	ValueType_BOOL    ValueType = 1
	ValueType_INT64   ValueType = 2
	ValueType_FLOAT64 ValueType = 3
	ValueType_BINARY  ValueType = 4
)

// Enum value maps for ValueType.
var (
	ValueType_name = map[int32]string{
		0: "STRING",
		1: "BOOL",
		2: "INT64",
		3: "FLOAT64",
		4: "BINARY",
	}
	ValueType_value = map[string]int32{
		"STRING":  0,
		"BOOL":    1,
		"INT64":   2,
		"FLOAT64": 3,
		"BINARY":  4,
	}
)

func (x ValueType) Enum() *ValueType {
	p := new(ValueType)
	*p = x
	return p
}

func (x ValueType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (ValueType) Descriptor() protoreflect.EnumDescriptor {
	return file_proto_trace_proto_enumTypes[0].Descriptor()
}

func (ValueType) Type() protoreflect.EnumType {
	return &file_proto_trace_proto_enumTypes[0]
}

func (x ValueType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use ValueType.Descriptor instead.
func (ValueType) EnumDescriptor() ([]byte, []int) {
	return file_proto_trace_proto_rawDescGZIP(), []int{0}
}

type KeyValue struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key      string    `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	VType    ValueType `protobuf:"varint,2,opt,name=v_type,json=vType,proto3,enum=trace.ValueType" json:"v_type,omitempty"`
	VStr     string    `protobuf:"bytes,3,opt,name=v_str,json=vStr,proto3" json:"v_str,omitempty"`
	VBool    bool      `protobuf:"varint,4,opt,name=v_bool,json=vBool,proto3" json:"v_bool,omitempty"`
	VInt64   int64     `protobuf:"varint,5,opt,name=v_int64,json=vInt64,proto3" json:"v_int64,omitempty"`
	VFloat64 float64   `protobuf:"fixed64,6,opt,name=v_float64,json=vFloat64,proto3" json:"v_float64,omitempty"`
	VBinary  []byte    `protobuf:"bytes,7,opt,name=v_binary,json=vBinary,proto3" json:"v_binary,omitempty"`
}

func (x *KeyValue) Reset() {
	*x = KeyValue{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_trace_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *KeyValue) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*KeyValue) ProtoMessage() {}

func (x *KeyValue) ProtoReflect() protoreflect.Message {
	mi := &file_proto_trace_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use KeyValue.ProtoReflect.Descriptor instead.
func (*KeyValue) Descriptor() ([]byte, []int) {
	return file_proto_trace_proto_rawDescGZIP(), []int{0}
}

func (x *KeyValue) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *KeyValue) GetVType() ValueType {
	if x != nil {
		return x.VType
	}
	return ValueType_STRING
}

func (x *KeyValue) GetVStr() string {
	if x != nil {
		return x.VStr
	}
	return ""
}

func (x *KeyValue) GetVBool() bool {
	if x != nil {
		return x.VBool
	}
	return false
}

func (x *KeyValue) GetVInt64() int64 {
	if x != nil {
		return x.VInt64
	}
	return 0
}

func (x *KeyValue) GetVFloat64() float64 {
	if x != nil {
		return x.VFloat64
	}
	return 0
}

func (x *KeyValue) GetVBinary() []byte {
	if x != nil {
		return x.VBinary
	}
	return nil
}

// A span defines a single event in a trace.
// This span format is inspired by the zipkin span design at:
// https://github.com/openzipkin/zipkin-api/blob/master/zipkin.proto
type Span struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// A field that uniquely identifies this event.
	// This field usually contains a randomly generated UUID.
	// This field is required and encoded as 8 or 16 bytes, in big endian byte order.
	Id []byte `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	// This field contains the parent id of this span.  It is useful to establish
	// a parent-child relationships between spans.
	// If empty, this span will be considered a root span.
	ParentId []byte `protobuf:"bytes,2,opt,name=parent_id,json=parentId,proto3" json:"parent_id,omitempty"`
	// A trace is a directed acyclic graph of spans. All spans with the same trace_id belong to the
	// same transaction. This field is required.
	TraceId []byte `protobuf:"bytes,3,opt,name=trace_id,json=traceId,proto3" json:"trace_id,omitempty"`
	// A name for the event.
	Name string `protobuf:"bytes,4,opt,name=name,proto3" json:"name,omitempty"`
	// The timestamp field stores the epoch microseconds at which this event happened.
	// For example: a value of 1551849569000000 represents March 6, 2019 5:19:29 UTC.
	// We use fixed64 since it is more wire efficient for than int64 for larger numbers.
	// This field is required.
	Timestamp uint64 `protobuf:"fixed64,5,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	// This field stored the duration in microseconds for the event in the critical path.
	// For example 150 milliseconds is 150000 microseconds.
	// This field is required.
	Duration uint64 `protobuf:"varint,6,opt,name=duration,proto3" json:"duration,omitempty"`
	// A list of key value pairs.
	Tags []*KeyValue `protobuf:"bytes,7,rep,name=tags,proto3" json:"tags,omitempty"`
}

func (x *Span) Reset() {
	*x = Span{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_trace_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Span) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Span) ProtoMessage() {}

func (x *Span) ProtoReflect() protoreflect.Message {
	mi := &file_proto_trace_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Span.ProtoReflect.Descriptor instead.
func (*Span) Descriptor() ([]byte, []int) {
	return file_proto_trace_proto_rawDescGZIP(), []int{1}
}

func (x *Span) GetId() []byte {
	if x != nil {
		return x.Id
	}
	return nil
}

func (x *Span) GetParentId() []byte {
	if x != nil {
		return x.ParentId
	}
	return nil
}

func (x *Span) GetTraceId() []byte {
	if x != nil {
		return x.TraceId
	}
	return nil
}

func (x *Span) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Span) GetTimestamp() uint64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

func (x *Span) GetDuration() uint64 {
	if x != nil {
		return x.Duration
	}
	return 0
}

func (x *Span) GetTags() []*KeyValue {
	if x != nil {
		return x.Tags
	}
	return nil
}

// List of spans is a message to send multiple spans in a single call.
// Any tags specified in this call will be added to all the spans sent in this message.
// If the list of spans is empty, this message will be ignored even if tags field is set.
type ListOfSpans struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// A list of spans.
	Spans []*Span `protobuf:"bytes,1,rep,name=spans,proto3" json:"spans,omitempty"`
	// A list of tags common to all the spans in this request.
	// All these tags will be added for all the spans sent in this request.
	// Sending common tags this way is more network efficient.
	Tags []*KeyValue `protobuf:"bytes,2,rep,name=tags,proto3" json:"tags,omitempty"`
}

func (x *ListOfSpans) Reset() {
	*x = ListOfSpans{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_trace_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListOfSpans) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListOfSpans) ProtoMessage() {}

func (x *ListOfSpans) ProtoReflect() protoreflect.Message {
	mi := &file_proto_trace_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListOfSpans.ProtoReflect.Descriptor instead.
func (*ListOfSpans) Descriptor() ([]byte, []int) {
	return file_proto_trace_proto_rawDescGZIP(), []int{2}
}

func (x *ListOfSpans) GetSpans() []*Span {
	if x != nil {
		return x.Spans
	}
	return nil
}

func (x *ListOfSpans) GetTags() []*KeyValue {
	if x != nil {
		return x.Tags
	}
	return nil
}

var File_proto_trace_proto protoreflect.FileDescriptor

var file_proto_trace_proto_rawDesc = []byte{
	0x0a, 0x11, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x74, 0x72, 0x61, 0x63, 0x65, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x12, 0x05, 0x74, 0x72, 0x61, 0x63, 0x65, 0x1a, 0x1b, 0x67, 0x6f, 0x6f, 0x67,
	0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x65, 0x6d, 0x70, 0x74,
	0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xc2, 0x01, 0x0a, 0x08, 0x4b, 0x65, 0x79, 0x56,
	0x61, 0x6c, 0x75, 0x65, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x27, 0x0a, 0x06, 0x76, 0x5f, 0x74, 0x79, 0x70, 0x65,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x10, 0x2e, 0x74, 0x72, 0x61, 0x63, 0x65, 0x2e, 0x56,
	0x61, 0x6c, 0x75, 0x65, 0x54, 0x79, 0x70, 0x65, 0x52, 0x05, 0x76, 0x54, 0x79, 0x70, 0x65, 0x12,
	0x13, 0x0a, 0x05, 0x76, 0x5f, 0x73, 0x74, 0x72, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04,
	0x76, 0x53, 0x74, 0x72, 0x12, 0x15, 0x0a, 0x06, 0x76, 0x5f, 0x62, 0x6f, 0x6f, 0x6c, 0x18, 0x04,
	0x20, 0x01, 0x28, 0x08, 0x52, 0x05, 0x76, 0x42, 0x6f, 0x6f, 0x6c, 0x12, 0x17, 0x0a, 0x07, 0x76,
	0x5f, 0x69, 0x6e, 0x74, 0x36, 0x34, 0x18, 0x05, 0x20, 0x01, 0x28, 0x03, 0x52, 0x06, 0x76, 0x49,
	0x6e, 0x74, 0x36, 0x34, 0x12, 0x1b, 0x0a, 0x09, 0x76, 0x5f, 0x66, 0x6c, 0x6f, 0x61, 0x74, 0x36,
	0x34, 0x18, 0x06, 0x20, 0x01, 0x28, 0x01, 0x52, 0x08, 0x76, 0x46, 0x6c, 0x6f, 0x61, 0x74, 0x36,
	0x34, 0x12, 0x19, 0x0a, 0x08, 0x76, 0x5f, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x18, 0x07, 0x20,
	0x01, 0x28, 0x0c, 0x52, 0x07, 0x76, 0x42, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x22, 0xc1, 0x01, 0x0a,
	0x04, 0x53, 0x70, 0x61, 0x6e, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x0c, 0x52, 0x02, 0x69, 0x64, 0x12, 0x1b, 0x0a, 0x09, 0x70, 0x61, 0x72, 0x65, 0x6e, 0x74, 0x5f,
	0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x08, 0x70, 0x61, 0x72, 0x65, 0x6e, 0x74,
	0x49, 0x64, 0x12, 0x19, 0x0a, 0x08, 0x74, 0x72, 0x61, 0x63, 0x65, 0x5f, 0x69, 0x64, 0x18, 0x03,
	0x20, 0x01, 0x28, 0x0c, 0x52, 0x07, 0x74, 0x72, 0x61, 0x63, 0x65, 0x49, 0x64, 0x12, 0x12, 0x0a,
	0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d,
	0x65, 0x12, 0x1c, 0x0a, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x05,
	0x20, 0x01, 0x28, 0x06, 0x52, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12,
	0x1a, 0x0a, 0x08, 0x64, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x06, 0x20, 0x01, 0x28,
	0x04, 0x52, 0x08, 0x64, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x23, 0x0a, 0x04, 0x74,
	0x61, 0x67, 0x73, 0x18, 0x07, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0f, 0x2e, 0x74, 0x72, 0x61, 0x63,
	0x65, 0x2e, 0x4b, 0x65, 0x79, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x52, 0x04, 0x74, 0x61, 0x67, 0x73,
	0x22, 0x55, 0x0a, 0x0b, 0x4c, 0x69, 0x73, 0x74, 0x4f, 0x66, 0x53, 0x70, 0x61, 0x6e, 0x73, 0x12,
	0x21, 0x0a, 0x05, 0x73, 0x70, 0x61, 0x6e, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0b,
	0x2e, 0x74, 0x72, 0x61, 0x63, 0x65, 0x2e, 0x53, 0x70, 0x61, 0x6e, 0x52, 0x05, 0x73, 0x70, 0x61,
	0x6e, 0x73, 0x12, 0x23, 0x0a, 0x04, 0x74, 0x61, 0x67, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b,
	0x32, 0x0f, 0x2e, 0x74, 0x72, 0x61, 0x63, 0x65, 0x2e, 0x4b, 0x65, 0x79, 0x56, 0x61, 0x6c, 0x75,
	0x65, 0x52, 0x04, 0x74, 0x61, 0x67, 0x73, 0x2a, 0x45, 0x0a, 0x09, 0x56, 0x61, 0x6c, 0x75, 0x65,
	0x54, 0x79, 0x70, 0x65, 0x12, 0x0a, 0x0a, 0x06, 0x53, 0x54, 0x52, 0x49, 0x4e, 0x47, 0x10, 0x00,
	0x12, 0x08, 0x0a, 0x04, 0x42, 0x4f, 0x4f, 0x4c, 0x10, 0x01, 0x12, 0x09, 0x0a, 0x05, 0x49, 0x4e,
	0x54, 0x36, 0x34, 0x10, 0x02, 0x12, 0x0b, 0x0a, 0x07, 0x46, 0x4c, 0x4f, 0x41, 0x54, 0x36, 0x34,
	0x10, 0x03, 0x12, 0x0a, 0x0a, 0x06, 0x42, 0x49, 0x4e, 0x41, 0x52, 0x59, 0x10, 0x04, 0x32, 0x47,
	0x0a, 0x0c, 0x54, 0x72, 0x61, 0x63, 0x65, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x37,
	0x0a, 0x09, 0x53, 0x65, 0x6e, 0x64, 0x54, 0x72, 0x61, 0x63, 0x65, 0x12, 0x12, 0x2e, 0x74, 0x72,
	0x61, 0x63, 0x65, 0x2e, 0x4c, 0x69, 0x73, 0x74, 0x4f, 0x66, 0x53, 0x70, 0x61, 0x6e, 0x73, 0x1a,
	0x16, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x42, 0x43, 0x0a, 0x1e, 0x63, 0x6f, 0x6d, 0x2e, 0x73,
	0x6c, 0x61, 0x63, 0x6b, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x6d, 0x75, 0x72,
	0x72, 0x6f, 0x6e, 0x2e, 0x74, 0x72, 0x61, 0x63, 0x65, 0x5a, 0x21, 0x63, 0x6f, 0x6d, 0x2e, 0x73,
	0x6c, 0x61, 0x63, 0x6b, 0x2e, 0x6b, 0x61, 0x6c, 0x64, 0x62, 0x2f, 0x67, 0x65, 0x6e, 0x2f, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x74, 0x72, 0x61, 0x63, 0x65, 0x70, 0x62, 0x62, 0x06, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_proto_trace_proto_rawDescOnce sync.Once
	file_proto_trace_proto_rawDescData = file_proto_trace_proto_rawDesc
)

func file_proto_trace_proto_rawDescGZIP() []byte {
	file_proto_trace_proto_rawDescOnce.Do(func() {
		file_proto_trace_proto_rawDescData = protoimpl.X.CompressGZIP(file_proto_trace_proto_rawDescData)
	})
	return file_proto_trace_proto_rawDescData
}

var file_proto_trace_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_proto_trace_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_proto_trace_proto_goTypes = []interface{}{
	(ValueType)(0),        // 0: trace.ValueType
	(*KeyValue)(nil),      // 1: trace.KeyValue
	(*Span)(nil),          // 2: trace.Span
	(*ListOfSpans)(nil),   // 3: trace.ListOfSpans
	(*emptypb.Empty)(nil), // 4: google.protobuf.Empty
}
var file_proto_trace_proto_depIdxs = []int32{
	0, // 0: trace.KeyValue.v_type:type_name -> trace.ValueType
	1, // 1: trace.Span.tags:type_name -> trace.KeyValue
	2, // 2: trace.ListOfSpans.spans:type_name -> trace.Span
	1, // 3: trace.ListOfSpans.tags:type_name -> trace.KeyValue
	3, // 4: trace.TraceService.SendTrace:input_type -> trace.ListOfSpans
	4, // 5: trace.TraceService.SendTrace:output_type -> google.protobuf.Empty
	5, // [5:6] is the sub-list for method output_type
	4, // [4:5] is the sub-list for method input_type
	4, // [4:4] is the sub-list for extension type_name
	4, // [4:4] is the sub-list for extension extendee
	0, // [0:4] is the sub-list for field type_name
}

func init() { file_proto_trace_proto_init() }
func file_proto_trace_proto_init() {
	if File_proto_trace_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_proto_trace_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*KeyValue); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_proto_trace_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Span); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_proto_trace_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ListOfSpans); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_proto_trace_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_proto_trace_proto_goTypes,
		DependencyIndexes: file_proto_trace_proto_depIdxs,
		EnumInfos:         file_proto_trace_proto_enumTypes,
		MessageInfos:      file_proto_trace_proto_msgTypes,
	}.Build()
	File_proto_trace_proto = out.File
	file_proto_trace_proto_rawDesc = nil
	file_proto_trace_proto_goTypes = nil
	file_proto_trace_proto_depIdxs = nil
}