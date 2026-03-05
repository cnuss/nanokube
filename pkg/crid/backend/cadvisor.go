package backend

import (
	"context"

	v1 "github.com/google/cadvisor/info/v1"
	v2 "github.com/google/cadvisor/info/v2"
	"k8s.io/kubernetes/pkg/kubelet/cadvisor"
)

func NewCadvisor(backend *BackendImpl) cadvisor.Interface {
	cadvisor := &CadvisorImpl{backend: backend}
	// TODO: start
	return cadvisor
}

type CadvisorImpl struct {
	backend *BackendImpl
}

// ContainerFsInfo implements [cadvisor.Interface].
func (c *CadvisorImpl) ContainerFsInfo(context.Context) (v2.FsInfo, error) {
	panic("ContainerFsInfo: unimplemented")
}

// ContainerInfoV2 implements [cadvisor.Interface].
func (c *CadvisorImpl) ContainerInfoV2(name string, options v2.RequestOptions) (map[string]v2.ContainerInfo, error) {
	panic("ContainerInfoV2: unimplemented")
}

// GetDirFsInfo implements [cadvisor.Interface].
func (c *CadvisorImpl) GetDirFsInfo(path string) (v2.FsInfo, error) {
	panic("GetDirFsInfo: unimplemented")
}

// GetRequestedContainersInfo implements [cadvisor.Interface].
func (c *CadvisorImpl) GetRequestedContainersInfo(containerName string, options v2.RequestOptions) (map[string]*v1.ContainerInfo, error) {
	panic("GetRequestedContainersInfo: unimplemented")
}

// ImagesFsInfo implements [cadvisor.Interface].
func (c *CadvisorImpl) ImagesFsInfo(context.Context) (v2.FsInfo, error) {
	panic("ImagesFsInfo: unimplemented")
}

// MachineInfo implements [cadvisor.Interface].
func (c *CadvisorImpl) MachineInfo() (*v1.MachineInfo, error) {
	panic("MachineInfo: unimplemented")
}

// RootFsInfo implements [cadvisor.Interface].
func (c *CadvisorImpl) RootFsInfo() (v2.FsInfo, error) {
	panic("RootFsInfo: unimplemented")
}

// Start implements [cadvisor.Interface].
func (c *CadvisorImpl) Start() error {
	panic("Start: unimplemented")
}

// VersionInfo implements [cadvisor.Interface].
func (c *CadvisorImpl) VersionInfo() (*v1.VersionInfo, error) {
	panic("VersionInfo: unimplemented")
}

var _ cadvisor.Interface = &CadvisorImpl{}
