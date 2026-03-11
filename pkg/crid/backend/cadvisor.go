package backend

import (
	"context"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cnuss/nanokube/pkg/component"
	v1 "github.com/google/cadvisor/info/v1"
	v2 "github.com/google/cadvisor/info/v2"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/kubernetes/pkg/kubelet/cadvisor"
)

type containerInfoBuilder struct {
	spec v2.ContainerSpec
	ts   time.Time
	cpu  *v1.CpuStats
	mem  *v1.MemoryStats
}

func newContainerInfo() *containerInfoBuilder {
	now := time.Now()
	return &containerInfoBuilder{
		spec: v2.ContainerSpec{
			CreationTime: now,
			HasCpu:       true,
			HasMemory:    true,
			HasNetwork:   true,
		},
		ts: now,
	}
}

func (b *containerInfoBuilder) WithHostInfo(host *HostInfo) *containerInfoBuilder {
	if host == nil {
		return b
	}
	b.spec.Cpu = v2.CpuSpec{Limit: uint64(len(host.CpuInfo)) * 1024}
	if mem, err := host.MemInfo(); err == nil {
		memBytes := uint64(mem.MemTotal)
		b.spec.Memory = v2.MemorySpec{Limit: memBytes}
		b.mem = &v1.MemoryStats{Usage: memBytes, WorkingSet: memBytes}
	}
	if cpuNs, err := host.CpuUsage(); err == nil {
		b.cpu = &v1.CpuStats{Usage: v1.CpuUsage{Total: cpuNs}}
	}
	return b
}

func (b *containerInfoBuilder) WithContainerStats(stats *runtimeapi.ContainerStats) *containerInfoBuilder {
	if stats == nil {
		return b
	}
	if stats.Cpu != nil && stats.Cpu.UsageCoreNanoSeconds != nil {
		b.cpu = &v1.CpuStats{Usage: v1.CpuUsage{Total: stats.Cpu.UsageCoreNanoSeconds.Value}}
	}
	if stats.Memory != nil {
		b.mem = &v1.MemoryStats{}
		if stats.Memory.UsageBytes != nil {
			b.mem.Usage = stats.Memory.UsageBytes.Value
		}
		if stats.Memory.WorkingSetBytes != nil {
			b.mem.WorkingSet = stats.Memory.WorkingSetBytes.Value
		}
		if stats.Memory.RssBytes != nil {
			b.mem.RSS = stats.Memory.RssBytes.Value
		}
	}
	if stats.Cpu != nil && stats.Cpu.Timestamp > 0 {
		b.ts = time.Unix(0, stats.Cpu.Timestamp)
	}
	return b
}

func (b *containerInfoBuilder) WithContainerStatus(resp *runtimeapi.ContainerStatusResponse) *containerInfoBuilder {
	if resp == nil || resp.Status == nil {
		return b
	}
	b.spec.CreationTime = time.Unix(0, resp.Status.CreatedAt)
	b.spec.Labels = resp.Status.Labels
	b.spec.Image = resp.Status.GetImage().GetImage()
	return b
}

func (b *containerInfoBuilder) Build() v2.ContainerInfo {
	return v2.ContainerInfo{
		Spec: b.spec,
		Stats: []*v2.ContainerStats{{
			Timestamp: b.ts,
			Cpu:       b.cpu,
			Memory:    b.mem,
		}},
	}
}

const fsCacheTTL = 60 * time.Second

type fsCacheEntry struct {
	info      v2.FsInfo
	expiresAt time.Time
}

func NewCadvisor(backend *BackendImpl) cadvisor.Interface {
	return &CadvisorImpl{
		backend: backend,
		log:     component.NewLogger("cadvisor"),
		fsCache: make(map[string]*fsCacheEntry),
	}
}

type CadvisorImpl struct {
	backend *BackendImpl
	log     component.Logger
	mu      sync.Mutex
	fsCache map[string]*fsCacheEntry
}

// ContainerFsInfo implements [cadvisor.Interface].
func (c *CadvisorImpl) ContainerFsInfo(ctx context.Context) (v2.FsInfo, error) {
	c.log.Trace().Msg("ContainerFsInfo")
	return c.GetDirFsInfo("/")
}

// ContainerInfoV2 implements [cadvisor.Interface].
func (c *CadvisorImpl) ContainerInfoV2(name string, options v2.RequestOptions) (map[string]v2.ContainerInfo, error) {
	c.log.Trace().Str("name", name).Bool("recursive", options.Recursive).Msg("ContainerInfoV2")

	rt := c.backend.Containers()
	if rt == nil {
		return map[string]v2.ContainerInfo{}, nil
	}

	// Root: synthetic entry from host info
	if name == "/" {
		host, err := c.backend.HostInfo()
		if err != nil {
			c.log.Warn().Err(err).Msg("HostInfo failed")
		}
		result := map[string]v2.ContainerInfo{
			"/": newContainerInfo().WithHostInfo(host).Build(),
		}

		if options.Recursive {
			ctx, cancel := context.WithTimeout(c.backend.Context(), 30*time.Second)
			defer cancel()
			containers, err := rt.ListContainers(ctx, nil)
			if err != nil {
				c.log.Warn().Err(err).Msg("ListContainers failed")
				return result, nil
			}
			for _, ctr := range containers {
				if info, err := c.ContainerInfoV2(ctr.Id, v2.RequestOptions{}); err == nil {
					maps.Copy(result, info)
				}
			}
		}

		return result, nil
	}

	// Non-root: look up a specific container
	ctx, cancel := context.WithTimeout(c.backend.Context(), 10*time.Second)
	defer cancel()
	stats, err := rt.ContainerStats(ctx, name)
	if err != nil {
		c.log.Debug().Err(err).Str("name", name).Msg("ContainerStats failed")
		root, err := c.ContainerInfoV2("/", v2.RequestOptions{})
		if err != nil {
			return nil, err
		}
		return map[string]v2.ContainerInfo{name: root["/"]}, nil
	}
	ctr, err := rt.ContainerStatus(ctx, name, false)
	if err != nil {
		c.log.Debug().Err(err).Str("name", name).Msg("ContainerStatus failed")
	}

	return map[string]v2.ContainerInfo{
		name: newContainerInfo().WithContainerStats(stats).WithContainerStatus(ctr).Build(),
	}, nil
}

// GetDirFsInfo implements [cadvisor.Interface].
func (c *CadvisorImpl) GetDirFsInfo(path string) (v2.FsInfo, error) {
	c.log.Trace().Str("path", path).Msg("GetDirFsInfo")

	c.mu.Lock()
	if entry, ok := c.fsCache[path]; ok && time.Now().Before(entry.expiresAt) {
		info := entry.info
		c.mu.Unlock()
		return info, nil
	}
	c.mu.Unlock()

	hostMount := "/host"
	probePath := hostMount
	if path != "/" {
		probePath = hostMount + path
	}

	var info v2.FsInfo
	err := c.backend.Driver.Run("busybox",
		[]string{"stat", "-f", "-c", "%S %b %a", probePath},
		[]string{path + ":" + hostMount + ":ro"},
		true,
		func(out string) error {
			fields := strings.Fields(strings.TrimSpace(out))
			if len(fields) < 3 {
				return fmt.Errorf("expected 3 fields, got %d: %q", len(fields), out)
			}
			blockSize, err := strconv.ParseUint(fields[0], 10, 64)
			if err != nil {
				return fmt.Errorf("parse block size: %w", err)
			}
			totalBlocks, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return fmt.Errorf("parse total blocks: %w", err)
			}
			availBlocks, err := strconv.ParseUint(fields[2], 10, 64)
			if err != nil {
				return fmt.Errorf("parse avail blocks: %w", err)
			}
			capacity := blockSize * totalBlocks
			available := blockSize * availBlocks
			info = v2.FsInfo{
				Capacity:  capacity,
				Available: available,
				Usage:     capacity - available,
			}
			return nil
		},
	)
	if err != nil {
		c.log.Warn().Err(err).Str("path", path).Msg("fs probe failed")
		return v2.FsInfo{}, err
	}

	c.mu.Lock()
	c.fsCache[path] = &fsCacheEntry{info: info, expiresAt: time.Now().Add(fsCacheTTL)}
	c.mu.Unlock()

	return info, nil
}

// GetRequestedContainersInfo implements [cadvisor.Interface].
func (c *CadvisorImpl) GetRequestedContainersInfo(containerName string, options v2.RequestOptions) (map[string]*v1.ContainerInfo, error) {
	c.log.Trace().Str("name", containerName).Bool("recursive", options.Recursive).Msg("GetRequestedContainersInfo")

	v2Info, err := c.ContainerInfoV2(containerName, options)
	if err != nil {
		return nil, err
	}

	result := make(map[string]*v1.ContainerInfo, len(v2Info))
	for name, info := range v2Info {
		var cpuStats v1.CpuStats
		var memStats v1.MemoryStats
		var ts time.Time
		if len(info.Stats) > 0 {
			ts = info.Stats[0].Timestamp
			if info.Stats[0].Cpu != nil {
				cpuStats = *info.Stats[0].Cpu
			}
			if info.Stats[0].Memory != nil {
				memStats = *info.Stats[0].Memory
			}
		}
		result[name] = &v1.ContainerInfo{
			ContainerReference: v1.ContainerReference{
				Name:      name,
				Namespace: string(c.backend.Name()),
			},
			Spec: v1.ContainerSpec{
				CreationTime: info.Spec.CreationTime,
				Labels:       info.Spec.Labels,
				HasCpu:       info.Spec.HasCpu,
				HasMemory:    info.Spec.HasMemory,
				HasNetwork:   info.Spec.HasNetwork,
				Image:        info.Spec.Image,
			},
			Stats: []*v1.ContainerStats{{
				Timestamp: ts,
				Cpu:       cpuStats,
				Memory:    memStats,
			}},
		}
	}

	return result, nil
}

// ImagesFsInfo implements [cadvisor.Interface].
func (c *CadvisorImpl) ImagesFsInfo(ctx context.Context) (v2.FsInfo, error) {
	c.log.Trace().Msg("ImagesFsInfo")
	return c.ContainerFsInfo(ctx)
}

// MachineInfo implements [cadvisor.Interface].
func (c *CadvisorImpl) MachineInfo() (*v1.MachineInfo, error) {
	c.log.Trace().Msg("MachineInfo")

	host, err := c.backend.HostInfo()
	if err != nil {
		c.log.Warn().Err(err).Msg("HostInfo failed, reporting minimal capacity")
		return &v1.MachineInfo{NumCores: 1}, nil
	}

	info := &v1.MachineInfo{
		NumCores:   len(host.CpuInfo),
		InstanceID: v1.InstanceID(host.Hostname),
		BootID:     host.BootID,
		SystemUUID: host.SystemUUID,
		MachineID:  host.MachineID,
	}

	if mem, err := host.MemInfo(); err == nil {
		info.MemoryCapacity = uint64(mem.MemTotal)
	} else {
		c.log.Warn().Err(err).Msg("MemInfo failed")
	}

	return info, nil
}

// RootFsInfo implements [cadvisor.Interface].
func (c *CadvisorImpl) RootFsInfo() (v2.FsInfo, error) {
	c.log.Trace().Msg("RootFsInfo")
	return c.GetDirFsInfo("/")
}

// Start implements [cadvisor.Interface].
func (c *CadvisorImpl) Start() error {
	c.log.Trace().Msg("Start")
	return nil
}

// VersionInfo implements [cadvisor.Interface].
func (c *CadvisorImpl) VersionInfo() (*v1.VersionInfo, error) {
	c.log.Trace().Msg("VersionInfo")

	host, err := c.backend.HostInfo()
	if err != nil {
		c.log.Warn().Err(err).Msg("HostInfo failed for VersionInfo")
		return &v1.VersionInfo{}, nil
	}

	return &v1.VersionInfo{
		KernelVersion:      host.KernelVersion,
		ContainerOsVersion: host.OSVersion,
	}, nil
}

var _ cadvisor.Interface = &CadvisorImpl{}
