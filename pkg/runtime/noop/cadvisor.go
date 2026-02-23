package noop

import (
	"context"

	cadvisorapi "github.com/google/cadvisor/info/v1"
	cadvisorapiv2 "github.com/google/cadvisor/info/v2"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
	"k8s.io/kubernetes/pkg/kubelet/cadvisor"
)

type Cadvisor struct{ NodeName string }

func NewCadvisor() cadvisor.Interface {
	return &Cadvisor{NodeName: "noop"}
}

func (s *Cadvisor) Start() error { return nil }
func (s *Cadvisor) ContainerInfoV2(string, cadvisorapiv2.RequestOptions) (map[string]cadvisorapiv2.ContainerInfo, error) {
	return nil, nil
}
func (s *Cadvisor) GetRequestedContainersInfo(string, cadvisorapiv2.RequestOptions) (map[string]*cadvisorapi.ContainerInfo, error) {
	return nil, nil
}
func (s *Cadvisor) MachineInfo() (*cadvisorapi.MachineInfo, error) {
	cores, _ := cpu.Counts(true)
	if cores == 0 {
		cores = 1
	}
	var memory uint64
	if v, err := mem.VirtualMemory(); err == nil {
		memory = v.Total
	}
	return &cadvisorapi.MachineInfo{
		NumCores:       cores,
		MemoryCapacity: memory,
	}, nil
}
func (s *Cadvisor) VersionInfo() (*cadvisorapi.VersionInfo, error) {
	return &cadvisorapi.VersionInfo{}, nil
}
func (s *Cadvisor) ImagesFsInfo(context.Context) (cadvisorapiv2.FsInfo, error) {
	return cadvisorapiv2.FsInfo{}, nil
}
func (s *Cadvisor) RootFsInfo() (cadvisorapiv2.FsInfo, error) {
	return cadvisorapiv2.FsInfo{}, nil
}
func (s *Cadvisor) ContainerFsInfo(context.Context) (cadvisorapiv2.FsInfo, error) {
	return cadvisorapiv2.FsInfo{}, nil
}
func (s *Cadvisor) GetDirFsInfo(string) (cadvisorapiv2.FsInfo, error) {
	return cadvisorapiv2.FsInfo{}, nil
}
