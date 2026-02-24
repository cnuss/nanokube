package cri

import (
	"context"

	cadvisorapi "github.com/google/cadvisor/info/v1"
	cadvisorapiv2 "github.com/google/cadvisor/info/v2"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/mem"
	"k8s.io/kubernetes/pkg/kubelet/cadvisor"
)

var _ cadvisor.Interface = (*Cadvisor)(nil)

type Cadvisor struct {
	nodeName string
}

func NewCadvisor(nodeName string) *Cadvisor {
	return &Cadvisor{nodeName: nodeName}
}

func (c *Cadvisor) Start() error {
	return nil
}

func (c *Cadvisor) MachineInfo() (*cadvisorapi.MachineInfo, error) {
	cores, err := cpu.Counts(true)
	if err != nil {
		cores = 1
	}

	var memTotal uint64
	if v, err := mem.VirtualMemory(); err == nil {
		memTotal = v.Total
	}

	return &cadvisorapi.MachineInfo{
		NumCores:       cores,
		InstanceID:     cadvisorapi.InstanceID(c.nodeName),
		MemoryCapacity: memTotal,
	}, nil
}

func (c *Cadvisor) VersionInfo() (*cadvisorapi.VersionInfo, error) {
	kernelVersion, _ := host.KernelVersion()
	platform, _, osVersion, _ := host.PlatformInformation()
	containerOS := platform
	if osVersion != "" {
		containerOS = platform + " " + osVersion
	}

	return &cadvisorapi.VersionInfo{
		KernelVersion:      kernelVersion,
		ContainerOsVersion: containerOS,
	}, nil
}

func (c *Cadvisor) ContainerInfoV2(name string, options cadvisorapiv2.RequestOptions) (map[string]cadvisorapiv2.ContainerInfo, error) {
	return map[string]cadvisorapiv2.ContainerInfo{}, nil
}

func (c *Cadvisor) GetRequestedContainersInfo(containerName string, options cadvisorapiv2.RequestOptions) (map[string]*cadvisorapi.ContainerInfo, error) {
	return map[string]*cadvisorapi.ContainerInfo{}, nil
}

func (c *Cadvisor) ImagesFsInfo(context.Context) (cadvisorapiv2.FsInfo, error) {
	return cadvisorapiv2.FsInfo{}, nil
}

func (c *Cadvisor) RootFsInfo() (cadvisorapiv2.FsInfo, error) {
	return cadvisorapiv2.FsInfo{}, nil
}

func (c *Cadvisor) ContainerFsInfo(context.Context) (cadvisorapiv2.FsInfo, error) {
	return cadvisorapiv2.FsInfo{}, nil
}

func (c *Cadvisor) GetDirFsInfo(path string) (cadvisorapiv2.FsInfo, error) {
	return cadvisorapiv2.FsInfo{}, nil
}
