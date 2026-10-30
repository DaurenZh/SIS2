// Package rpcspec keeps REST routes and message factories in one place so the
// comparison clients and adapters cannot silently disagree about an operation.
package rpcspec

import (
	"fmt"
	"net/url"
	"strings"

	pb "github.com/DaurenZh/SIS2/internal/imagev1"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type Operation struct {
	Name, Verb, Path string
	Request          func() proto.Message
	Response         func() proto.Message
}

var Operations = []Operation{
	{"CreateUser", "POST", "/v1/users", func() proto.Message { return &pb.CreateUserRequest{} }, func() proto.Message { return &pb.CreateUserResponse{} }},
	{"CreateImage", "POST", "/v1/images", func() proto.Message { return &pb.CreateImageRequest{} }, func() proto.Message { return &pb.CreateImageResponse{} }},
	{"CompleteUpload", "POST", "/v1/images/{image_id}/complete", func() proto.Message { return &pb.CompleteUploadRequest{} }, func() proto.Message { return &pb.GetImageResponse{} }},
	{"GetImage", "GET", "/v1/images/{image_id}", func() proto.Message { return &pb.GetImageRequest{} }, func() proto.Message { return &pb.GetImageResponse{} }},
	{"UpdateImage", "PATCH", "/v1/images/{image_id}", func() proto.Message { return &pb.UpdateImageRequest{} }, func() proto.Message { return &pb.GetImageResponse{} }},
	{"DeleteImage", "DELETE", "/v1/images/{image_id}", func() proto.Message { return &pb.DeleteImageRequest{} }, func() proto.Message { return &pb.GetImageResponse{} }},
	{"ListUserImages", "GET", "/v1/users/{owner_id}/images", func() proto.Message { return &pb.ListUserImagesRequest{} }, func() proto.Message { return &pb.ListUserImagesResponse{} }},
	{"CreateComment", "POST", "/v1/images/{image_id}/comments", func() proto.Message { return &pb.CreateCommentRequest{} }, func() proto.Message { return &pb.CreateCommentResponse{} }},
	{"LikeImage", "POST", "/v1/images/{image_id}/likes", func() proto.Message { return &pb.LikeImageRequest{} }, func() proto.Message { return &pb.LikeImageResponse{} }},
	{"CreateShareLink", "POST", "/v1/images/{image_id}/share-links", func() proto.Message { return &pb.CreateShareLinkRequest{} }, func() proto.Message { return &pb.CreateShareLinkResponse{} }},
}

func Find(name string) (Operation, error) {
	for _, op := range Operations {
		if op.Name == name {
			return op, nil
		}
	}
	return Operation{}, fmt.Errorf("unknown operation %q", name)
}
func String(m proto.Message, name string) string {
	f := m.ProtoReflect().Descriptor().Fields().ByName(protoreflect.Name(name))
	if f == nil {
		return ""
	}
	return m.ProtoReflect().Get(f).String()
}
func SetString(m proto.Message, name, value string) {
	f := m.ProtoReflect().Descriptor().Fields().ByName(protoreflect.Name(name))
	if f != nil {
		m.ProtoReflect().Set(f, protoreflect.ValueOfString(value))
	}
}
func (op Operation) URL(req proto.Message) string {
	path := op.Path
	for _, name := range []string{"image_id", "owner_id"} {
		path = strings.ReplaceAll(path, "{"+name+"}", url.PathEscape(String(req, name)))
	}
	if r, ok := req.(*pb.ListUserImagesRequest); ok {
		q := url.Values{}
		q.Set("limit", fmt.Sprint(r.Limit))
		q.Set("cursor", r.Cursor)
		path += "?" + q.Encode()
	}
	return path
}
