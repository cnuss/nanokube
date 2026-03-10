package backend

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/dustin/go-humanize"
)

type HostInfo struct {
	Hostname      string
	MachineID     string
	SystemUUID    string
	BootID        string
	KernelVersion string
	CpuInfo       []CpuInfo
	mu            sync.Mutex
	driver        Driver
}

type CpuInfo struct {
	BogoMIPS     float32
	Features     []string
	Implementor  byte
	Architecture byte
	Variant      byte
	Part         uint16
	Revision     byte
}

type MemoryInfo struct {
	MemTotal       int64
	MemFree        int64
	MemAvailable   int64
	Buffers        int64
	Cached         int64
	SwapCached     int64
	Active         int64
	Inactive       int64
	SwapTotal      int64
	SwapFree       int64
	Dirty          int64
	Writeback      int64
	AnonPages      int64
	Mapped         int64
	Shmem          int64
	KReclaimable   int64
	Slab           int64
	SReclaimable   int64
	SUnreclaim     int64
	KernelStack    int64
	PageTables     int64
	CommitLimit    int64
	CommittedAS    int64
	VmallocTotal   int64
	VmallocUsed    int64
	HugePagesTotal int64
	HugePagesFree  int64
	Hugepagesize   int64
}

// NewHostInfo probes immutable host information (hostname, cpuinfo) in parallel.
// The returned HostInfo can fetch dynamic data (meminfo) on demand via MemInfo().
func NewHostInfo(driver Driver) (*HostInfo, error) {
	h := &HostInfo{
		CpuInfo: []CpuInfo{},
		driver:  driver,
	}

	run := func(cmd []string, cb func(string) error) error {
		return driver.Run("busybox", cmd, []string{}, true, cb)
	}

	if err := run([]string{"hostname"}, func(out string) error {
		h.WithHostname(out)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to probe hostname: %w", err)
	}

	if err := run([]string{"cat", "/proc/cpuinfo"}, func(out string) error {
		h.WithCpuInfo(out)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to probe cpuinfo: %w", err)
	}

	return h, nil
}

// MemInfo fetches current memory information by probing /proc/meminfo.
func (h *HostInfo) MemInfo() (MemoryInfo, error) {
	var mem MemoryInfo
	err := h.driver.Run("busybox", []string{"cat", "/proc/meminfo"}, []string{}, true, func(out string) error {
		for line := range strings.SplitSeq(out, "\n") {
			key, val := parseKV(line, ":")
			if key == "" {
				continue
			}
			bytes, err := humanize.ParseBytes(strings.Replace(val, "kB", "KiB", 1))
			if err != nil {
				continue
			}
			n := int64(bytes)
			switch key {
			case "MemTotal":
				mem.MemTotal = n
			case "MemFree":
				mem.MemFree = n
			case "MemAvailable":
				mem.MemAvailable = n
			case "Buffers":
				mem.Buffers = n
			case "Cached":
				mem.Cached = n
			case "SwapCached":
				mem.SwapCached = n
			case "Active":
				mem.Active = n
			case "Inactive":
				mem.Inactive = n
			case "SwapTotal":
				mem.SwapTotal = n
			case "SwapFree":
				mem.SwapFree = n
			case "Dirty":
				mem.Dirty = n
			case "Writeback":
				mem.Writeback = n
			case "AnonPages":
				mem.AnonPages = n
			case "Mapped":
				mem.Mapped = n
			case "Shmem":
				mem.Shmem = n
			case "KReclaimable":
				mem.KReclaimable = n
			case "Slab":
				mem.Slab = n
			case "SReclaimable":
				mem.SReclaimable = n
			case "SUnreclaim":
				mem.SUnreclaim = n
			case "KernelStack":
				mem.KernelStack = n
			case "PageTables":
				mem.PageTables = n
			case "CommitLimit":
				mem.CommitLimit = n
			case "Committed_AS":
				mem.CommittedAS = n
			case "VmallocTotal":
				mem.VmallocTotal = n
			case "VmallocUsed":
				mem.VmallocUsed = n
			case "HugePages_Total":
				mem.HugePagesTotal = n
			case "HugePages_Free":
				mem.HugePagesFree = n
			case "Hugepagesize":
				mem.Hugepagesize = n
			}
		}
		return nil
	})
	return mem, err
}

func (h *HostInfo) WithHostname(hostname string) *HostInfo {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.Hostname = hostname
	return h
}

func (h *HostInfo) WithMachineID(machineID string) *HostInfo {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.MachineID = machineID
	return h
}

func (h *HostInfo) WithSystemUUID(systemUUID string) *HostInfo {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.SystemUUID = systemUUID
	return h
}

func (h *HostInfo) WithBootID(bootID string) *HostInfo {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.BootID = bootID
	return h
}

func (h *HostInfo) WithKernelVersion(kernelVersion string) *HostInfo {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.KernelVersion = kernelVersion
	return h
}

// WithCpuInfo parses /proc/cpuinfo output. Processor blocks are separated
// by blank lines. Fields not in the struct are silently ignored.
func (h *HostInfo) WithCpuInfo(output string) *HostInfo {
	h.mu.Lock()
	defer h.mu.Unlock()
	var cur *CpuInfo
	for line := range strings.SplitSeq(output, "\n") {
		if strings.TrimSpace(line) == "" {
			if cur != nil {
				h.CpuInfo = append(h.CpuInfo, *cur)
				cur = nil
			}
			continue
		}
		key, val := parseKV(line, ":")
		if key == "" {
			continue
		}
		if key == "processor" {
			cur = &CpuInfo{}
			continue
		}
		if cur == nil {
			continue
		}
		switch key {
		case "BogoMIPS":
			if f, err := strconv.ParseFloat(val, 32); err == nil {
				cur.BogoMIPS = float32(f)
			}
		case "Features":
			cur.Features = strings.Fields(val)
		case "CPU implementer":
			if v, err := strconv.ParseUint(val, 0, 8); err == nil {
				cur.Implementor = byte(v)
			}
		case "CPU architecture":
			if v, err := strconv.ParseUint(val, 0, 8); err == nil {
				cur.Architecture = byte(v)
			}
		case "CPU variant":
			if v, err := strconv.ParseUint(val, 0, 8); err == nil {
				cur.Variant = byte(v)
			}
		case "CPU part":
			if v, err := strconv.ParseUint(val, 0, 16); err == nil {
				cur.Part = uint16(v)
			}
		case "CPU revision":
			if v, err := strconv.ParseUint(val, 0, 8); err == nil {
				cur.Revision = byte(v)
			}
		}
	}
	if cur != nil {
		h.CpuInfo = append(h.CpuInfo, *cur)
	}
	return h
}

// parseKV splits a line on the given separator and returns trimmed key and value.
func parseKV(line, sep string) (string, string) {
	parts := strings.SplitN(line, sep, 2)
	if len(parts) != 2 {
		return "", ""
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
}
