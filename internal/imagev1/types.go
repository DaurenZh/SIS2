package imagev1

import (
	"context"
	"encoding/json"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

type CreateImageRequest struct {
	IdempotencyKey  string `json:"idempotency_key"`
	OwnerID         string `json:"owner_id"`
	Caption         string `json:"caption"`
	ResponseDelayMS int32  `json:"response_delay_ms,omitempty"`
}

type CreateImageResponse struct {
	Image *Image `json:"image"`
}

type GetImageRequest struct {
	ImageID string `json:"image_id"`
}

type GetImageResponse struct {
	Image *Image `json:"image"`
}

type Image struct {
	ImageID string `json:"image_id"`
	OwnerID string `json:"owner_id"`
	Caption string `json:"caption"`
	Status  string `json:"status"`
}

type jsonCodec struct{}

func (jsonCodec) Name() string                           { return "sis-json" }
func (jsonCodec) Marshal(value any) ([]byte, error)      { return json.Marshal(value) }
func (jsonCodec) Unmarshal(data []byte, value any) error { return json.Unmarshal(data, value) }

func init() { encoding.RegisterCodec(jsonCodec{}) }

type ImageServiceClient interface {
	CreateImage(ctx context.Context, in *CreateImageRequest, opts ...grpc.CallOption) (*CreateImageResponse, error)
	GetImage(ctx context.Context, in *GetImageRequest, opts ...grpc.CallOption) (*GetImageResponse, error)
}

type imageServiceClient struct{ cc grpc.ClientConnInterface }

func NewImageServiceClient(cc grpc.ClientConnInterface) ImageServiceClient {
	return &imageServiceClient{cc}
}

func (c *imageServiceClient) CreateImage(ctx context.Context, in *CreateImageRequest, opts ...grpc.CallOption) (*CreateImageResponse, error) {
	out := new(CreateImageResponse)
	err := c.cc.Invoke(ctx, "/sis.image.v1.ImageService/CreateImage", in, out, opts...)
	return out, err
}

func (c *imageServiceClient) GetImage(ctx context.Context, in *GetImageRequest, opts ...grpc.CallOption) (*GetImageResponse, error) {
	out := new(GetImageResponse)
	err := c.cc.Invoke(ctx, "/sis.image.v1.ImageService/GetImage", in, out, opts...)
	return out, err
}

type ImageServiceServer interface {
	CreateImage(context.Context, *CreateImageRequest) (*CreateImageResponse, error)
	GetImage(context.Context, *GetImageRequest) (*GetImageResponse, error)
}

func RegisterImageServiceServer(s grpc.ServiceRegistrar, srv ImageServiceServer) {
	s.RegisterService(&ImageService_ServiceDesc, srv)
}

func _ImageService_CreateImage_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(CreateImageRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ImageServiceServer).CreateImage(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/sis.image.v1.ImageService/CreateImage"}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(ImageServiceServer).CreateImage(ctx, req.(*CreateImageRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ImageService_GetImage_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(GetImageRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ImageServiceServer).GetImage(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/sis.image.v1.ImageService/GetImage"}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(ImageServiceServer).GetImage(ctx, req.(*GetImageRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var ImageService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "sis.image.v1.ImageService",
	HandlerType: (*ImageServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "CreateImage", Handler: _ImageService_CreateImage_Handler},
		{MethodName: "GetImage", Handler: _ImageService_GetImage_Handler},
	},
}
