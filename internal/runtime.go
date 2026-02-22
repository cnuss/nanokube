package internal

import (
	"context"
	"os"
	"sync"

	"github.com/rs/zerolog/log"
)

type RuntimeType string

const (
	Containerd RuntimeType = "containerd"
	CriDockerd RuntimeType = "cri-dockerd"
	CriO       RuntimeType = "cri-o"
	Docker     RuntimeType = "docker"
	Podman     RuntimeType = "podman"
)

type runtimeCandidate struct {
	kind      RuntimeType
	socks     []string
	preflight func() string // returns CRI endpoint, or "" if preflight fails
}

type Runtime struct {
	ctx      context.Context
	once     sync.Once
	kind     RuntimeType
	endpoint string
}

var candidates = []runtimeCandidate{
	// Virtual runtimes (checked first)
	{
		kind: Docker,
		socks: []string{
			"/var/run/docker.sock", "/run/docker.sock",
			os.Getenv("HOME") + "/.docker/run/docker.sock",
			os.Getenv("HOME") + "/.colima/default/docker.sock",
			os.Getenv("HOME") + "/.rd/docker.sock",
		},
		preflight: func() string {
			// Docker requires cri-dockerd as a CRI shim
			sock := "/var/run/cri-dockerd.sock"
			if _, err := os.Stat(sock); err != nil {
				log.Warn().Str("socket", sock).Msg("cri-dockerd not found, skipping docker")
				return ""
			}
			return "unix://" + sock
		},
	},
	{
		kind: Podman,
		socks: []string{
			"/var/run/podman/podman.sock", "/run/podman/podman.sock",
			os.Getenv("HOME") + "/.local/share/podman/podman.sock",
			os.Getenv("XDG_RUNTIME_DIR") + "/podman/podman.sock",
		},
		// TODO: podman may need a CRI shim
	},
	// Native runtimes
	{kind: Containerd, socks: []string{"/var/run/containerd/containerd.sock"}},
	{kind: CriDockerd, socks: []string{"/var/run/cri-dockerd.sock"}},
	{kind: CriO, socks: []string{"/var/run/crio/crio.sock"}},
}

func NewRuntime(ctx context.Context) *Runtime {
	r := &Runtime{ctx: ctx}
	r.detect()
	log.Info().Str("runtime", r.Name()).Str("endpoint", r.endpoint).Msg("using container runtime")
	return r
}

func (r *Runtime) detect() {
	r.once.Do(func() {
		var checked []string
		for _, c := range candidates {
			for _, sock := range c.socks {
				checked = append(checked, sock)
				if _, err := os.Stat(sock); err != nil {
					continue
				}
				// Socket exists, run preflight if defined
				if c.preflight != nil {
					if endpoint := c.preflight(); endpoint != "" {
						r.kind = c.kind
						r.endpoint = endpoint
						return
					}
					// Preflight failed, try next candidate
					break
				}
				r.kind = c.kind
				r.endpoint = "unix://" + sock
				return
			}
		}
		log.Fatal().Strs("checked", checked).Msg("no container runtime found")
	})
}

func (r *Runtime) Name() string {
	return string(r.kind)
}

func (r *Runtime) ContainerRuntimeEndpoint() string {
	return r.endpoint
}

// TODO: return different hostname based on RuntimeType
func (r *Runtime) Hostname() string {
	return "localhost"
}

// TODO: return different domain based on RuntimeType
func (r *Runtime) Domain() string {
	return "cluster.local"
}

// TODO: return different nameservers based on RuntimeType
func (r *Runtime) Nameservers() []string {
	return []string{"10.96.0.10"}
}
