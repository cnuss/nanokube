package config

import (
	"context"
	"os"
	"sync"

	"github.com/cnuss/nanokube/pkg/stub"
	"github.com/rs/zerolog/log"
	internalapi "k8s.io/cri-api/pkg/apis"
	cadvisor "k8s.io/kubernetes/pkg/kubelet/cadvisor"
	cm "k8s.io/kubernetes/pkg/kubelet/cm"
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
	kind             RuntimeType
	socks            []string
	preflight        func() string // returns CRI endpoint, or "" if preflight fails
	cadvisor         cadvisor.Interface
	imageService     internalapi.ImageManagerService
	runtimeService   internalapi.RuntimeService
	containerManager cm.ContainerManager
}

type Runtime struct {
	ctx       context.Context
	once      sync.Once
	kind      RuntimeType
	endpoint  string
	candidate *runtimeCandidate
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
		cadvisor:         stub.NewCadvisor(),
		runtimeService:   stub.NewRuntimeService(),
		imageService:     stub.NewImageService(),
		containerManager: stub.NewContainerManager(),
	},
	{
		kind: Podman,
		socks: []string{
			"/var/run/podman/podman.sock", "/run/podman/podman.sock",
			os.Getenv("HOME") + "/.local/share/podman/podman.sock",
			os.Getenv("XDG_RUNTIME_DIR") + "/podman/podman.sock",
		},
		// TODO: podman may need a CRI shim
		cadvisor:         stub.NewCadvisor(),
		runtimeService:   stub.NewRuntimeService(),
		imageService:     stub.NewImageService(),
		containerManager: stub.NewContainerManager(),
	},
	// Native runtimes
	{
		kind: Containerd,
		socks: []string{
			"/var/run/containerd/containerd.sock",
			os.Getenv("HOME") + "/.colima/default/containerd.sock",
		},
		cadvisor:         stub.NewCadvisor(),
		runtimeService:   stub.NewRuntimeService(),
		imageService:     stub.NewImageService(),
		containerManager: stub.NewContainerManager(),
	},
	{
		kind:             CriDockerd,
		socks:            []string{"/var/run/cri-dockerd.sock"},
		cadvisor:         stub.NewCadvisor(),
		runtimeService:   stub.NewRuntimeService(),
		imageService:     stub.NewImageService(),
		containerManager: stub.NewContainerManager(),
	},
	{
		kind:             CriO,
		socks:            []string{"/var/run/crio/crio.sock"},
		cadvisor:         stub.NewCadvisor(),
		runtimeService:   stub.NewRuntimeService(),
		imageService:     stub.NewImageService(),
		containerManager: stub.NewContainerManager(),
	},
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
		for i := range candidates {
			c := &candidates[i]
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
						r.candidate = c
						return
					}
					// Preflight failed, try next candidate
					break
				}
				r.kind = c.kind
				r.endpoint = "unix://" + sock
				r.candidate = c
				return
			}
		}
		log.Warn().Strs("checked", checked).Msg("no container runtime found")
		r.candidate = &runtimeCandidate{
			cadvisor:         stub.NewCadvisor(),
			imageService:     stub.NewImageService(),
			runtimeService:   stub.NewRuntimeService(),
			containerManager: stub.NewContainerManager(),
		}
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

func (r *Runtime) Cadvisor() cadvisor.Interface {
	return r.candidate.cadvisor
}

func (r *Runtime) ImageService() internalapi.ImageManagerService {
	return r.candidate.imageService
}

func (r *Runtime) RuntimeService() internalapi.RuntimeService {
	return r.candidate.runtimeService
}

func (r *Runtime) ContainerManager() cm.ContainerManager {
	return r.candidate.containerManager
}
