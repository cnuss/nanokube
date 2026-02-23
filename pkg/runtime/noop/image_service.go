package noop

import (
	"context"

	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type ImageService struct{}

func NewImageService() internalapi.ImageManagerService {
	return &ImageService{}
}

func (s *ImageService) ListImages(context.Context, *runtimeapi.ImageFilter) ([]*runtimeapi.Image, error) {
	return nil, nil
}
func (s *ImageService) ImageStatus(context.Context, *runtimeapi.ImageSpec, bool) (*runtimeapi.ImageStatusResponse, error) {
	return &runtimeapi.ImageStatusResponse{}, nil
}
func (s *ImageService) PullImage(context.Context, *runtimeapi.ImageSpec, *runtimeapi.AuthConfig, *runtimeapi.PodSandboxConfig) (string, error) {
	return "", nil
}
func (s *ImageService) RemoveImage(context.Context, *runtimeapi.ImageSpec) error { return nil }
func (s *ImageService) ImageFsInfo(context.Context) (*runtimeapi.ImageFsInfoResponse, error) {
	return &runtimeapi.ImageFsInfoResponse{}, nil
}
func (s *ImageService) Close() error { return nil }
