package cri

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	cadvisorapi "github.com/google/cadvisor/info/v1"
	cadvisorapiv2 "github.com/google/cadvisor/info/v2"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/kubernetes/pkg/kubelet/cadvisor"
)

var _ cadvisor.Interface = (*Cadvisor)(nil)

type Cadvisor struct {
	ctx      context.Context
	nodeName string
	backend  Backend

	mu      sync.Mutex
	fsCache map[string]*fsCacheEntry
}

type fsCacheEntry struct {
	info      cadvisorapiv2.FsInfo
	expiresAt time.Time
}

const fsCacheTTL = 60 * time.Second

func NewCadvisor(ctx context.Context, nodeName string, backend Backend) *Cadvisor {
	return &Cadvisor{
		ctx:      ctx,
		nodeName: nodeName,
		backend:  backend,
		fsCache:  make(map[string]*fsCacheEntry),
	}
}

func (c *Cadvisor) Start() error {
	return nil
}

func (c *Cadvisor) MachineInfo() (*cadvisorapi.MachineInfo, error) {
	cpus, memBytes, _, _, err := c.backend.HostInfo(c.ctx)
	if err != nil {
		logger.Warn().Err(err).Msg("cadvisor: HostInfo failed, reporting minimal capacity")
		cpus = 1
		memBytes = 0
	}

	bootID, systemUUID, machineID, err := c.backend.HostIDs(c.ctx)
	if err != nil {
		logger.Warn().Err(err).Msg("cadvisor: HostIDs probe failed")
	}

	return &cadvisorapi.MachineInfo{
		NumCores:       cpus,
		InstanceID:     cadvisorapi.InstanceID(c.nodeName),
		MemoryCapacity: uint64(memBytes),
		BootID:         bootID,
		SystemUUID:     systemUUID,
		MachineID:      machineID,
	}, nil
}

func (c *Cadvisor) VersionInfo() (*cadvisorapi.VersionInfo, error) {
	_, _, kernelVersion, osVersion, err := c.backend.HostInfo(c.ctx)
	if err != nil {
		logger.Warn().Err(err).Msg("cadvisor: HostInfo failed for VersionInfo")
		return &cadvisorapi.VersionInfo{}, nil
	}

	return &cadvisorapi.VersionInfo{
		KernelVersion:      kernelVersion,
		ContainerOsVersion: osVersion,
	}, nil
}

func (c *Cadvisor) ContainerInfoV2(name string, options cadvisorapiv2.RequestOptions) (map[string]cadvisorapiv2.ContainerInfo, error) {
	if c.backend == nil {
		return map[string]cadvisorapiv2.ContainerInfo{}, nil
	}

	// For non-root cgroup queries, return a synthetic entry with node-level
	// stats. The kubelet calls getCgroupInfo(name) which expects exactly 1
	// entry keyed by name.
	if name != "/" {
		return map[string]cadvisorapiv2.ContainerInfo{
			name: c.rootContainerInfoV2(),
		}, nil
	}

	result := make(map[string]cadvisorapiv2.ContainerInfo)

	// "/" always gets a root entry — the kubelet needs it for RootFsStats
	// and node-level resource accounting.
	result["/"] = c.rootContainerInfoV2()

	// When Recursive=true, also include per-container entries.
	if options.Recursive {
		ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
		defer cancel()

		containers, err := c.backend.ListContainers(ctx, nil)
		if err != nil {
			logger.Warn().Err(err).Msg("cadvisor: ListContainers failed")
			return result, nil
		}

		for _, ctr := range containers {
			stats, err := c.backend.ContainerStats(ctx, ctr.Id)
			if err != nil {
				logger.Debug().Err(err).Str("id", ctr.Id[:12]).Msg("cadvisor: ContainerStats failed, skipping")
				continue
			}
			result[ctr.Id] = c.criStatsToV2(ctr.CreatedAt, ctr.Labels, ctr.GetImage().GetImage(), stats)
		}
	}

	return result, nil
}

// rootContainerInfoV2 returns a synthetic root ("/") container entry with
// node-level CPU and memory from HostInfo.
func (c *Cadvisor) rootContainerInfoV2() cadvisorapiv2.ContainerInfo {
	now := time.Now()
	cpus, memBytes, _, _, _ := c.backend.HostInfo(c.ctx)
	return cadvisorapiv2.ContainerInfo{
		Spec: cadvisorapiv2.ContainerSpec{
			CreationTime: now,
			HasCpu:       true,
			HasMemory:    true,
			HasNetwork:   true,
			Cpu:          cadvisorapiv2.CpuSpec{Limit: uint64(cpus)},
			Memory:       cadvisorapiv2.MemorySpec{Limit: uint64(memBytes)},
		},
		Stats: []*cadvisorapiv2.ContainerStats{{
			Timestamp: now,
			Cpu:       &cadvisorapi.CpuStats{},
			Memory: &cadvisorapi.MemoryStats{
				Usage:      uint64(memBytes),
				WorkingSet: uint64(memBytes),
			},
		}},
	}
}

// criStatsToV2 converts CRI container stats to cadvisor v2 ContainerInfo.
func (c *Cadvisor) criStatsToV2(createdAtNs int64, labels map[string]string, image string, stats *runtimeapi.ContainerStats) cadvisorapiv2.ContainerInfo {
	var cpuStats *cadvisorapi.CpuStats
	if stats.Cpu != nil && stats.Cpu.UsageCoreNanoSeconds != nil {
		cpuStats = &cadvisorapi.CpuStats{
			Usage: cadvisorapi.CpuUsage{
				Total: stats.Cpu.UsageCoreNanoSeconds.Value,
			},
		}
	}

	var memStats *cadvisorapi.MemoryStats
	if stats.Memory != nil {
		memStats = &cadvisorapi.MemoryStats{}
		if stats.Memory.UsageBytes != nil {
			memStats.Usage = stats.Memory.UsageBytes.Value
		}
		if stats.Memory.WorkingSetBytes != nil {
			memStats.WorkingSet = stats.Memory.WorkingSetBytes.Value
		}
		if stats.Memory.RssBytes != nil {
			memStats.RSS = stats.Memory.RssBytes.Value
		}
	}

	ts := time.Now()
	if stats.Cpu != nil && stats.Cpu.Timestamp > 0 {
		ts = time.Unix(0, stats.Cpu.Timestamp)
	}

	return cadvisorapiv2.ContainerInfo{
		Spec: cadvisorapiv2.ContainerSpec{
			CreationTime: time.Unix(0, createdAtNs),
			Labels:       labels,
			HasCpu:       true,
			HasMemory:    true,
			HasNetwork:   true,
			Image:        image,
		},
		Stats: []*cadvisorapiv2.ContainerStats{{
			Timestamp: ts,
			Cpu:       cpuStats,
			Memory:    memStats,
		}},
	}
}

func (c *Cadvisor) GetRequestedContainersInfo(containerName string, options cadvisorapiv2.RequestOptions) (map[string]*cadvisorapi.ContainerInfo, error) {
	if c.backend == nil {
		return map[string]*cadvisorapi.ContainerInfo{}, nil
	}

	result := make(map[string]*cadvisorapi.ContainerInfo)

	// For root or non-root cgroup queries, return a synthetic entry.
	if containerName != "/" || !options.Recursive {
		now := time.Now()
		_, memBytes, _, _, _ := c.backend.HostInfo(c.ctx)
		result[containerName] = &cadvisorapi.ContainerInfo{
			ContainerReference: cadvisorapi.ContainerReference{
				Name: containerName,
			},
			Spec: cadvisorapi.ContainerSpec{
				CreationTime: now,
				HasCpu:       true,
				HasMemory:    true,
				HasNetwork:   true,
			},
			Stats: []*cadvisorapi.ContainerStats{{
				Timestamp: now,
				Memory: cadvisorapi.MemoryStats{
					Usage:      uint64(memBytes),
					WorkingSet: uint64(memBytes),
				},
			}},
		}
		if !options.Recursive {
			return result, nil
		}
	}

	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()

	containers, err := c.backend.ListContainers(ctx, nil)
	if err != nil {
		logger.Warn().Err(err).Msg("cadvisor: ListContainers failed")
		return result, nil
	}

	for _, ctr := range containers {
		stats, err := c.backend.ContainerStats(ctx, ctr.Id)
		if err != nil {
			logger.Debug().Err(err).Str("id", ctr.Id[:12]).Msg("cadvisor: ContainerStats failed, skipping")
			continue
		}
		result[ctr.Id] = c.criStatsToV1(ctr, stats)
	}
	return result, nil
}

// criStatsToV1 converts CRI container stats to cadvisor v1 ContainerInfo.
func (c *Cadvisor) criStatsToV1(ctr *runtimeapi.Container, stats *runtimeapi.ContainerStats) *cadvisorapi.ContainerInfo {
	var cpuStats cadvisorapi.CpuStats
	if stats.Cpu != nil && stats.Cpu.UsageCoreNanoSeconds != nil {
		cpuStats.Usage.Total = stats.Cpu.UsageCoreNanoSeconds.Value
	}

	var memStats cadvisorapi.MemoryStats
	if stats.Memory != nil {
		if stats.Memory.UsageBytes != nil {
			memStats.Usage = stats.Memory.UsageBytes.Value
		}
		if stats.Memory.WorkingSetBytes != nil {
			memStats.WorkingSet = stats.Memory.WorkingSetBytes.Value
		}
		if stats.Memory.RssBytes != nil {
			memStats.RSS = stats.Memory.RssBytes.Value
		}
	}

	ts := time.Now()
	if stats.Cpu != nil && stats.Cpu.Timestamp > 0 {
		ts = time.Unix(0, stats.Cpu.Timestamp)
	}

	return &cadvisorapi.ContainerInfo{
		ContainerReference: cadvisorapi.ContainerReference{
			Id:        ctr.Id,
			Name:      ctr.Id,
			Namespace: "docker",
		},
		Spec: cadvisorapi.ContainerSpec{
			CreationTime: time.Unix(0, ctr.CreatedAt),
			Labels:       ctr.Labels,
			HasCpu:       true,
			HasMemory:    true,
			HasNetwork:   true,
			Image:        ctr.GetImage().GetImage(),
		},
		Stats: []*cadvisorapi.ContainerStats{{
			Timestamp: ts,
			Cpu:       cpuStats,
			Memory:    memStats,
		}},
	}
}

func (c *Cadvisor) ImagesFsInfo(ctx context.Context) (cadvisorapiv2.FsInfo, error) {
	return c.probeFsInfo("/")
}

func (c *Cadvisor) RootFsInfo() (cadvisorapiv2.FsInfo, error) {
	return c.probeFsInfo("/")
}

func (c *Cadvisor) ContainerFsInfo(ctx context.Context) (cadvisorapiv2.FsInfo, error) {
	return c.probeFsInfo("/")
}

func (c *Cadvisor) GetDirFsInfo(path string) (cadvisorapiv2.FsInfo, error) {
	return c.probeFsInfo(path)
}

// probeFsInfo uses the backend's RunProbe to run a busybox container that
// executes `stat -f` on a bind-mounted host path. Results are cached with a 60s TTL.
func (c *Cadvisor) probeFsInfo(path string) (cadvisorapiv2.FsInfo, error) {
	c.mu.Lock()
	if entry, ok := c.fsCache[path]; ok && time.Now().Before(entry.expiresAt) {
		info := entry.info
		c.mu.Unlock()
		return info, nil
	}
	c.mu.Unlock()

	if c.backend == nil {
		return cadvisorapiv2.FsInfo{}, nil
	}

	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()

	// Bind-mount the host path into /host and probe there
	hostMount := "/host"
	probePath := hostMount + path
	if path == "/" {
		probePath = hostMount
	}

	// stat -f -c '%S %b %a' outputs: block_size total_blocks avail_blocks
	out, err := c.backend.RunProbe(ctx, "busybox",
		[]string{"stat", "-f", "-c", "%S %b %a", probePath},
		[]string{path + ":" + hostMount + ":ro"},
	)
	if err != nil {
		logger.Warn().Err(err).Str("path", path).Msg("cadvisor: fs probe failed")
		return cadvisorapiv2.FsInfo{}, nil
	}

	info, err := parseStatF(strings.TrimSpace(string(out)))
	if err != nil {
		logger.Warn().Err(err).Str("output", string(out)).Msg("cadvisor: failed to parse stat -f output")
		return cadvisorapiv2.FsInfo{}, nil
	}

	c.mu.Lock()
	c.fsCache[path] = &fsCacheEntry{info: info, expiresAt: time.Now().Add(fsCacheTTL)}
	c.mu.Unlock()

	return info, nil
}

// parseStatF parses output from `stat -f -c '%S %b %a'`:
// block_size total_blocks avail_blocks
func parseStatF(output string) (cadvisorapiv2.FsInfo, error) {
	fields := strings.Fields(output)
	if len(fields) < 3 {
		return cadvisorapiv2.FsInfo{}, fmt.Errorf("expected 3 fields, got %d: %q", len(fields), output)
	}

	blockSize, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return cadvisorapiv2.FsInfo{}, fmt.Errorf("parse block size: %w", err)
	}
	totalBlocks, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return cadvisorapiv2.FsInfo{}, fmt.Errorf("parse total blocks: %w", err)
	}
	availBlocks, err := strconv.ParseUint(fields[2], 10, 64)
	if err != nil {
		return cadvisorapiv2.FsInfo{}, fmt.Errorf("parse avail blocks: %w", err)
	}

	capacity := blockSize * totalBlocks
	available := blockSize * availBlocks

	return cadvisorapiv2.FsInfo{
		Capacity:  capacity,
		Available: available,
		Usage:     capacity - available,
	}, nil
}
