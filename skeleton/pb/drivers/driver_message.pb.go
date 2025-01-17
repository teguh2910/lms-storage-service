// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v3.21.12
// source: drivers/driver_message.proto

package drivers

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Driver struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id            string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Name          string `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	Phone         string `protobuf:"bytes,3,opt,name=phone,proto3" json:"phone,omitempty"`
	LicenceNumber string `protobuf:"bytes,4,opt,name=licence_number,json=licenceNumber,proto3" json:"licence_number,omitempty"`
	CompanyId     string `protobuf:"bytes,5,opt,name=company_id,json=companyId,proto3" json:"company_id,omitempty"`
	CompanyName   string `protobuf:"bytes,6,opt,name=company_name,json=companyName,proto3" json:"company_name,omitempty"`
	IsDelete      bool   `protobuf:"varint,7,opt,name=is_delete,json=isDelete,proto3" json:"is_delete,omitempty"`
	Created       string `protobuf:"bytes,8,opt,name=created,proto3" json:"created,omitempty"`
	CreatedBy     string `protobuf:"bytes,9,opt,name=created_by,json=createdBy,proto3" json:"created_by,omitempty"`
	Updated       string `protobuf:"bytes,10,opt,name=updated,proto3" json:"updated,omitempty"`
	UpdatedBy     string `protobuf:"bytes,11,opt,name=updated_by,json=updatedBy,proto3" json:"updated_by,omitempty"`
}

func (x *Driver) Reset() {
	*x = Driver{}
	if protoimpl.UnsafeEnabled {
		mi := &file_drivers_driver_message_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Driver) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Driver) ProtoMessage() {}

func (x *Driver) ProtoReflect() protoreflect.Message {
	mi := &file_drivers_driver_message_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Driver.ProtoReflect.Descriptor instead.
func (*Driver) Descriptor() ([]byte, []int) {
	return file_drivers_driver_message_proto_rawDescGZIP(), []int{0}
}

func (x *Driver) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *Driver) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Driver) GetPhone() string {
	if x != nil {
		return x.Phone
	}
	return ""
}

func (x *Driver) GetLicenceNumber() string {
	if x != nil {
		return x.LicenceNumber
	}
	return ""
}

func (x *Driver) GetCompanyId() string {
	if x != nil {
		return x.CompanyId
	}
	return ""
}

func (x *Driver) GetCompanyName() string {
	if x != nil {
		return x.CompanyName
	}
	return ""
}

func (x *Driver) GetIsDelete() bool {
	if x != nil {
		return x.IsDelete
	}
	return false
}

func (x *Driver) GetCreated() string {
	if x != nil {
		return x.Created
	}
	return ""
}

func (x *Driver) GetCreatedBy() string {
	if x != nil {
		return x.CreatedBy
	}
	return ""
}

func (x *Driver) GetUpdated() string {
	if x != nil {
		return x.Updated
	}
	return ""
}

func (x *Driver) GetUpdatedBy() string {
	if x != nil {
		return x.UpdatedBy
	}
	return ""
}

type Drivers struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Driver []*Driver `protobuf:"bytes,1,rep,name=driver,proto3" json:"driver,omitempty"`
}

func (x *Drivers) Reset() {
	*x = Drivers{}
	if protoimpl.UnsafeEnabled {
		mi := &file_drivers_driver_message_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Drivers) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Drivers) ProtoMessage() {}

func (x *Drivers) ProtoReflect() protoreflect.Message {
	mi := &file_drivers_driver_message_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Drivers.ProtoReflect.Descriptor instead.
func (*Drivers) Descriptor() ([]byte, []int) {
	return file_drivers_driver_message_proto_rawDescGZIP(), []int{1}
}

func (x *Drivers) GetDriver() []*Driver {
	if x != nil {
		return x.Driver
	}
	return nil
}

var File_drivers_driver_message_proto protoreflect.FileDescriptor

var file_drivers_driver_message_proto_rawDesc = []byte{
	0x0a, 0x1c, 0x64, 0x72, 0x69, 0x76, 0x65, 0x72, 0x73, 0x2f, 0x64, 0x72, 0x69, 0x76, 0x65, 0x72,
	0x5f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x08,
	0x73, 0x6b, 0x65, 0x6c, 0x65, 0x74, 0x6f, 0x6e, 0x22, 0xba, 0x02, 0x0a, 0x06, 0x44, 0x72, 0x69,
	0x76, 0x65, 0x72, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x02, 0x69, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x14, 0x0a, 0x05, 0x70, 0x68, 0x6f, 0x6e, 0x65,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x70, 0x68, 0x6f, 0x6e, 0x65, 0x12, 0x25, 0x0a,
	0x0e, 0x6c, 0x69, 0x63, 0x65, 0x6e, 0x63, 0x65, 0x5f, 0x6e, 0x75, 0x6d, 0x62, 0x65, 0x72, 0x18,
	0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0d, 0x6c, 0x69, 0x63, 0x65, 0x6e, 0x63, 0x65, 0x4e, 0x75,
	0x6d, 0x62, 0x65, 0x72, 0x12, 0x1d, 0x0a, 0x0a, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x5f,
	0x69, 0x64, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e,
	0x79, 0x49, 0x64, 0x12, 0x21, 0x0a, 0x0c, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x5f, 0x6e,
	0x61, 0x6d, 0x65, 0x18, 0x06, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x63, 0x6f, 0x6d, 0x70, 0x61,
	0x6e, 0x79, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x1b, 0x0a, 0x09, 0x69, 0x73, 0x5f, 0x64, 0x65, 0x6c,
	0x65, 0x74, 0x65, 0x18, 0x07, 0x20, 0x01, 0x28, 0x08, 0x52, 0x08, 0x69, 0x73, 0x44, 0x65, 0x6c,
	0x65, 0x74, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x64, 0x18, 0x08,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x64, 0x12, 0x1d, 0x0a,
	0x0a, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x64, 0x5f, 0x62, 0x79, 0x18, 0x09, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x09, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x64, 0x42, 0x79, 0x12, 0x18, 0x0a, 0x07,
	0x75, 0x70, 0x64, 0x61, 0x74, 0x65, 0x64, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x75,
	0x70, 0x64, 0x61, 0x74, 0x65, 0x64, 0x12, 0x1d, 0x0a, 0x0a, 0x75, 0x70, 0x64, 0x61, 0x74, 0x65,
	0x64, 0x5f, 0x62, 0x79, 0x18, 0x0b, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x75, 0x70, 0x64, 0x61,
	0x74, 0x65, 0x64, 0x42, 0x79, 0x22, 0x33, 0x0a, 0x07, 0x44, 0x72, 0x69, 0x76, 0x65, 0x72, 0x73,
	0x12, 0x28, 0x0a, 0x06, 0x64, 0x72, 0x69, 0x76, 0x65, 0x72, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b,
	0x32, 0x10, 0x2e, 0x73, 0x6b, 0x65, 0x6c, 0x65, 0x74, 0x6f, 0x6e, 0x2e, 0x44, 0x72, 0x69, 0x76,
	0x65, 0x72, 0x52, 0x06, 0x64, 0x72, 0x69, 0x76, 0x65, 0x72, 0x42, 0x14, 0x5a, 0x12, 0x70, 0x62,
	0x2f, 0x64, 0x72, 0x69, 0x76, 0x65, 0x72, 0x73, 0x3b, 0x64, 0x72, 0x69, 0x76, 0x65, 0x72, 0x73,
	0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_drivers_driver_message_proto_rawDescOnce sync.Once
	file_drivers_driver_message_proto_rawDescData = file_drivers_driver_message_proto_rawDesc
)

func file_drivers_driver_message_proto_rawDescGZIP() []byte {
	file_drivers_driver_message_proto_rawDescOnce.Do(func() {
		file_drivers_driver_message_proto_rawDescData = protoimpl.X.CompressGZIP(file_drivers_driver_message_proto_rawDescData)
	})
	return file_drivers_driver_message_proto_rawDescData
}

var file_drivers_driver_message_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_drivers_driver_message_proto_goTypes = []interface{}{
	(*Driver)(nil),  // 0: skeleton.Driver
	(*Drivers)(nil), // 1: skeleton.Drivers
}
var file_drivers_driver_message_proto_depIdxs = []int32{
	0, // 0: skeleton.Drivers.driver:type_name -> skeleton.Driver
	1, // [1:1] is the sub-list for method output_type
	1, // [1:1] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_drivers_driver_message_proto_init() }
func file_drivers_driver_message_proto_init() {
	if File_drivers_driver_message_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_drivers_driver_message_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Driver); i {
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
		file_drivers_driver_message_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Drivers); i {
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
			RawDescriptor: file_drivers_driver_message_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_drivers_driver_message_proto_goTypes,
		DependencyIndexes: file_drivers_driver_message_proto_depIdxs,
		MessageInfos:      file_drivers_driver_message_proto_msgTypes,
	}.Build()
	File_drivers_driver_message_proto = out.File
	file_drivers_driver_message_proto_rawDesc = nil
	file_drivers_driver_message_proto_goTypes = nil
	file_drivers_driver_message_proto_depIdxs = nil
}
