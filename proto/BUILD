load("@com_google_protobuf//bazel:proto_library.bzl", "proto_library")
load("@com_tsdb2_platform//proto:proto.bzl", "tsdb2_cc_proto_library")

package(default_visibility = ["//visibility:public"])

local_visibility = ["//:__subpackages__"]

proto_library(
    name = "tsql_proto",
    srcs = ["tsql.proto"],
)

tsdb2_cc_proto_library(
    name = "tsql_cc_proto",
    proto = ":tsql_proto",
    visibility = local_visibility,
)
