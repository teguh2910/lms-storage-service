// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v3.21.12
// source: drivers/driver_service.proto

package drivers

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	generic "skeleton/pb/generic"
	reflect "reflect"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

var File_drivers_driver_service_proto protoreflect.FileDescriptor

var file_drivers_driver_service_proto_rawDesc = []byte{
	0x0a, 0x1c, 0x64, 0x72, 0x69, 0x76, 0x65, 0x72, 0x73, 0x2f, 0x64, 0x72, 0x69, 0x76, 0x65, 0x72,
	0x5f, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x08,
	0x73, 0x6b, 0x65, 0x6c, 0x65, 0x74, 0x6f, 0x6e, 0x1a, 0x1c, 0x64, 0x72, 0x69, 0x76, 0x65, 0x72,
	0x73, 0x2f, 0x64, 0x72, 0x69, 0x76, 0x65, 0x72, 0x5f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1a, 0x64, 0x72, 0x69, 0x76, 0x65, 0x72, 0x73, 0x2f,
	0x64, 0x72, 0x69, 0x76, 0x65, 0x72, 0x5f, 0x69, 0x6e, 0x70, 0x75, 0x74, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x1a, 0x1d, 0x67, 0x65, 0x6e, 0x65, 0x72, 0x69, 0x63, 0x2f, 0x67, 0x65, 0x6e, 0x65,
	0x72, 0x69, 0x63, 0x5f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x32, 0xd9, 0x01, 0x0a, 0x0e, 0x44, 0x72, 0x69, 0x76, 0x65, 0x72, 0x73, 0x53, 0x65, 0x72,
	0x76, 0x69, 0x63, 0x65, 0x12, 0x36, 0x0a, 0x04, 0x4c, 0x69, 0x73, 0x74, 0x12, 0x19, 0x2e, 0x73,
	0x6b, 0x65, 0x6c, 0x65, 0x74, 0x6f, 0x6e, 0x2e, 0x44, 0x72, 0x69, 0x76, 0x65, 0x72, 0x4c, 0x69,
	0x73, 0x74, 0x49, 0x6e, 0x70, 0x75, 0x74, 0x1a, 0x11, 0x2e, 0x73, 0x6b, 0x65, 0x6c, 0x65, 0x74,
	0x6f, 0x6e, 0x2e, 0x44, 0x72, 0x69, 0x76, 0x65, 0x72, 0x73, 0x22, 0x00, 0x12, 0x2e, 0x0a, 0x06,
	0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x12, 0x10, 0x2e, 0x73, 0x6b, 0x65, 0x6c, 0x65, 0x74, 0x6f,
	0x6e, 0x2e, 0x44, 0x72, 0x69, 0x76, 0x65, 0x72, 0x1a, 0x10, 0x2e, 0x73, 0x6b, 0x65, 0x6c, 0x65,
	0x74, 0x6f, 0x6e, 0x2e, 0x44, 0x72, 0x69, 0x76, 0x65, 0x72, 0x22, 0x00, 0x12, 0x2e, 0x0a, 0x06,
	0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x12, 0x10, 0x2e, 0x73, 0x6b, 0x65, 0x6c, 0x65, 0x74, 0x6f,
	0x6e, 0x2e, 0x44, 0x72, 0x69, 0x76, 0x65, 0x72, 0x1a, 0x10, 0x2e, 0x73, 0x6b, 0x65, 0x6c, 0x65,
	0x74, 0x6f, 0x6e, 0x2e, 0x44, 0x72, 0x69, 0x76, 0x65, 0x72, 0x22, 0x00, 0x12, 0x2f, 0x0a, 0x06,
	0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x12, 0x0c, 0x2e, 0x73, 0x6b, 0x65, 0x6c, 0x65, 0x74, 0x6f,
	0x6e, 0x2e, 0x49, 0x64, 0x1a, 0x15, 0x2e, 0x73, 0x6b, 0x65, 0x6c, 0x65, 0x74, 0x6f, 0x6e, 0x2e,
	0x42, 0x6f, 0x6f, 0x6c, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x22, 0x00, 0x42, 0x14, 0x5a,
	0x12, 0x70, 0x62, 0x2f, 0x64, 0x72, 0x69, 0x76, 0x65, 0x72, 0x73, 0x3b, 0x64, 0x72, 0x69, 0x76,
	0x65, 0x72, 0x73, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var file_drivers_driver_service_proto_goTypes = []interface{}{
	(*DriverListInput)(nil),     // 0: skeleton.DriverListInput
	(*Driver)(nil),              // 1: skeleton.Driver
	(*generic.Id)(nil),          // 2: skeleton.Id
	(*Drivers)(nil),             // 3: skeleton.Drivers
	(*generic.BoolMessage)(nil), // 4: skeleton.BoolMessage
}
var file_drivers_driver_service_proto_depIdxs = []int32{
	0, // 0: skeleton.DriversService.List:input_type -> skeleton.DriverListInput
	1, // 1: skeleton.DriversService.Create:input_type -> skeleton.Driver
	1, // 2: skeleton.DriversService.Update:input_type -> skeleton.Driver
	2, // 3: skeleton.DriversService.Delete:input_type -> skeleton.Id
	3, // 4: skeleton.DriversService.List:output_type -> skeleton.Drivers
	1, // 5: skeleton.DriversService.Create:output_type -> skeleton.Driver
	1, // 6: skeleton.DriversService.Update:output_type -> skeleton.Driver
	4, // 7: skeleton.DriversService.Delete:output_type -> skeleton.BoolMessage
	4, // [4:8] is the sub-list for method output_type
	0, // [0:4] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_drivers_driver_service_proto_init() }
func file_drivers_driver_service_proto_init() {
	if File_drivers_driver_service_proto != nil {
		return
	}
	file_drivers_driver_message_proto_init()
	file_drivers_driver_input_proto_init()
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_drivers_driver_service_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   0,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_drivers_driver_service_proto_goTypes,
		DependencyIndexes: file_drivers_driver_service_proto_depIdxs,
	}.Build()
	File_drivers_driver_service_proto = out.File
	file_drivers_driver_service_proto_rawDesc = nil
	file_drivers_driver_service_proto_goTypes = nil
	file_drivers_driver_service_proto_depIdxs = nil
}