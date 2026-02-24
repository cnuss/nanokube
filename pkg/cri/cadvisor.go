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
	"github.com/rs/zerolog/log"
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
		log.Warn().Err(err).Msg("cadvisor: HostInfo failed, reporting minimal capacity")
		cpus = 1
		memBytes = 0
	}

	return &cadvisorapi.MachineInfo{
		NumCores:       cpus,
		InstanceID:     cadvisorapi.InstanceID(c.nodeName),
		MemoryCapacity: uint64(memBytes),
	}, nil
}

func (c *Cadvisor) VersionInfo() (*cadvisorapi.VersionInfo, error) {
	_, _, kernelVersion, osVersion, err := c.backend.HostInfo(c.ctx)
	if err != nil {
		log.Warn().Err(err).Msg("cadvisor: HostInfo failed for VersionInfo")
		return &cadvisorapi.VersionInfo{}, nil
	}

	return &cadvisorapi.VersionInfo{
		KernelVersion:      kernelVersion,
		ContainerOsVersion: osVersion,
	}, nil
}

func (c *Cadvisor) ContainerInfoV2(name string, options cadvisorapiv2.RequestOptions) (map[string]cadvisorapiv2.ContainerInfo, error) {
	return map[string]cadvisorapiv2.ContainerInfo{}, nil
}

func (c *Cadvisor) GetRequestedContainersInfo(containerName string, options cadvisorapiv2.RequestOptions) (map[string]*cadvisorapi.ContainerInfo, error) {
	return map[string]*cadvisorapi.ContainerInfo{}, nil
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
		log.Warn().Err(err).Str("path", path).Msg("cadvisor: fs probe failed")
		return cadvisorapiv2.FsInfo{}, nil
	}

	info, err := parseStatF(strings.TrimSpace(string(out)))
	if err != nil {
		log.Warn().Err(err).Str("output", string(out)).Msg("cadvisor: failed to parse stat -f output")
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
