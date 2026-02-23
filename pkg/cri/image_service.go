package cri

import (
	"context"

	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type imageService struct {
	runtimeapi.UnimplementedImageServiceServer
	backend Backend
}

func (s *imageService) ListImages(ctx context.Context, req *runtimeapi.ListImagesRequest) (*runtimeapi.ListImagesResponse, error) {
	images, err := s.backend.ListImages(ctx, req.GetFilter())
	if err != nil {
		return nil, err
	}
	return &runtimeapi.ListImagesResponse{Images: images}, nil
}

func (s *imageService) ImageStatus(ctx context.Context, req *runtimeapi.ImageStatusRequest) (*runtimeapi.ImageStatusResponse, error) {
	return s.backend.ImageStatus(ctx, req.GetImage(), req.GetVerbose())
}

func (s *imageService) PullImage(ctx context.Context, req *runtimeapi.PullImageRequest) (*runtimeapi.PullImageResponse, error) {
	ref, err := s.backend.PullImage(ctx, req.GetImage(), req.GetAuth(), req.GetSandboxConfig())
	if err != nil {
		return nil, err
	}
	return &runtimeapi.PullImageResponse{ImageRef: ref}, nil
}

func (s *imageService) RemoveImage(ctx context.Context, req *runtimeapi.RemoveImageRequest) (*runtimeapi.RemoveImageResponse, error) {
	if err := s.backend.RemoveImage(ctx, req.GetImage()); err != nil {
		return nil, err
	}
	return &runtimeapi.RemoveImageResponse{}, nil
}

func (s *imageService) ImageFsInfo(ctx context.Context, req *runtimeapi.ImageFsInfoRequest) (*runtimeapi.ImageFsInfoResponse, error) {
	return s.backend.ImageFsInfo(ctx)
}
