package backend

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/cnuss/nanokube/pkg/component"
	"github.com/dustin/go-humanize"
)

type HostInfo struct {
	Domain        string
	Hostname      string
	MachineID     string
	SystemUUID    string
	BootID        string
	KernelVersion string
	OSVersion     string
	Nameservers   []string
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
func NewHostInfo(driver Driver, hosts Hosts) (*HostInfo, error) {
	log := component.NewLogger("host-info")

	h := &HostInfo{
		Domain:   driver.Domain(),
		Hostname: hosts.Hostname(),
		CpuInfo:  []CpuInfo{},
		driver:   driver,
	}

	type probe struct {
		name string
		cmd  string             // shell command; stderr suppressed commands should use 2>/dev/null || echo ''
		fn   func(string) error // called with trimmed output
	}

	probes := []probe{
		{"hostname", "cat /etc/hostname 2>/dev/null || echo ''", func(out string) error {
			if out != "" {
				h.WithHostname(out)
			}
			log.Info().Str("hostname", h.Hostname).Msg("probed hostname")
			return nil
		}},
		{"machine-id", "cat /etc/machine-id 2>/dev/null || echo ''", func(out string) error {
			if out != "" {
				h.WithMachineID(out)
				log.Info().Str("machineID", h.MachineID).Msg("probed machine-id")
			} else {
				log.Warn().Msg("machine-id not available, falling back to empty")
				h.WithMachineID("")
			}
			return nil
		}},
		{"cpuinfo", "cat /proc/cpuinfo", func(out string) error {
			if out == "" {
				return fmt.Errorf("empty output")
			}
			h.WithCpuInfo(out)
			log.Info().Int("cpus", len(h.CpuInfo)).Msg("probed cpuinfo")
			return nil
		}},
		{"boot_id", "cat /proc/sys/kernel/random/boot_id", func(out string) error {
			if out == "" {
				return fmt.Errorf("empty output")
			}
			h.WithBootID(out)
			log.Info().Str("bootID", h.BootID).Msg("probed boot_id")
			return nil
		}},
		{"system_uuid", "cat /host/sys/class/dmi/id/product_uuid 2>/dev/null || echo ''", func(out string) error {
			if out != "" {
				h.WithSystemUUID(out)
				log.Info().Str("systemUUID", h.SystemUUID).Msg("probed system_uuid")
			} else {
				log.Warn().Msg("product_uuid not available, falling back to boot_id")
				h.WithSystemUUID(h.BootID)
			}
			return nil
		}},
		{"kernel", "uname -r", func(out string) error {
			if out == "" {
				return fmt.Errorf("empty output")
			}
			h.WithKernelVersion(out)
			log.Info().Str("kernel", h.KernelVersion).Msg("probed kernel version")
			return nil
		}},
		{"os-release", "cat /etc/os-release 2>/dev/null || echo ''", func(out string) error {
			if out != "" {
				for line := range strings.SplitSeq(out, "\n") {
					key, val := parseKV(line, "=")
					if key == "PRETTY_NAME" {
						h.WithOSVersion(strings.Trim(val, "\""))
						break
					}
				}
			}
			if h.OSVersion == "" {
				log.Warn().Msg("os-release not available, falling back to unknown")
				h.WithOSVersion("unknown")
			} else {
				log.Info().Str("os", h.OSVersion).Msg("probed os-release")
			}
			return nil
		}},
	}

	// Build a single shell script that runs all probes separated by a delimiter
	const delim = "---PROBE---"
	cmds := make([]string, 0, len(probes)*2)
	for i, p := range probes {
		cmds = append(cmds, p.cmd)
		if i < len(probes)-1 {
			cmds = append(cmds, "echo "+delim)
		}
	}

	if err := driver.Run("busybox", []string{"sh", "-c", strings.Join(cmds, ";")}, []string{
		"/etc/machine-id:/etc/machine-id:ro",
		"/etc/os-release:/etc/os-release:ro",
		"/sys:/host/sys:ro",
	}, true, func(out string) error {
		sections := strings.Split(out, delim)
		if len(sections) != len(probes) {
			return fmt.Errorf("expected %d probe sections, got %d", len(probes), len(sections))
		}
		for i, p := range probes {
			if err := p.fn(strings.TrimSpace(sections[i])); err != nil {
				return fmt.Errorf("probe %s failed: %w", p.name, err)
			}
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("host info probe failed: %w", err)
	}

	// Probe DNS from a separate container (no host mounts) to see what Docker gives containers
	if err := driver.Run("busybox", []string{"cat", "/etc/resolv.conf"}, nil, false, func(out string) error {
		for line := range strings.SplitSeq(out, "\n") {
			if strings.HasPrefix(line, "nameserver ") {
				h.Nameservers = append(h.Nameservers, strings.TrimSpace(strings.TrimPrefix(line, "nameserver ")))
			}
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to probe nameservers: %w", err)
	}
	if len(h.Nameservers) == 0 {
		return nil, fmt.Errorf("failed to probe nameservers: no nameserver lines in /etc/resolv.conf")
	}
	log.Info().Strs("nameservers", h.Nameservers).Msg("probed nameservers")

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

// CpuUsage fetches cumulative CPU usage in nanoseconds by probing /proc/stat.
// Parses the first "cpu" line: user nice system idle iowait irq softirq steal.
// Returns the sum of active fields (all except idle and iowait) in nanoseconds.
func (h *HostInfo) CpuUsage() (uint64, error) {
	var totalNs uint64
	err := h.driver.Run("busybox", []string{"cat", "/proc/stat"}, []string{}, true, func(out string) error {
		for line := range strings.SplitSeq(out, "\n") {
			if !strings.HasPrefix(line, "cpu ") {
				continue
			}
			fields := strings.Fields(line)
			if len(fields) < 5 {
				return fmt.Errorf("unexpected /proc/stat cpu line: %q", line)
			}
			// fields[0]="cpu", [1]=user, [2]=nice, [3]=system, [4]=idle,
			// [5]=iowait, [6]=irq, [7]=softirq, [8]=steal
			var activeJiffies uint64
			for i, f := range fields[1:] {
				v, err := strconv.ParseUint(f, 10, 64)
				if err != nil {
					continue
				}
				if i == 3 || i == 4 { // skip idle and iowait
					continue
				}
				activeJiffies += v
			}
			// Convert jiffies (1/100s) to nanoseconds
			totalNs = activeJiffies * 10_000_000
			return nil
		}
		return fmt.Errorf("/proc/stat: no cpu line found")
	})
	return totalNs, err
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

func (h *HostInfo) WithOSVersion(osVersion string) *HostInfo {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.OSVersion = osVersion
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
