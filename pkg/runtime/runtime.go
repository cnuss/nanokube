package runtime

import (
	"context"
	"os"
	"sync"

	noop "github.com/cnuss/nanokube/pkg/runtime/noop"
	"github.com/rs/zerolog/log"
	internalapi "k8s.io/cri-api/pkg/apis"
	"k8s.io/kubernetes/pkg/kubelet/cadvisor"
	"k8s.io/kubernetes/pkg/kubelet/cm"
)

type RuntimeType string

const (
	Containerd RuntimeType = "containerd"
	CriDockerd RuntimeType = "cri-dockerd"
	CriO       RuntimeType = "cri-o"
	Docker     RuntimeType = "docker"
	Podman     RuntimeType = "podman"
)

type Candidate struct {
	Kind             RuntimeType
	Sockets          []string
	Preflight        func(dataDir string) string // returns CRI endpoint, or "" if preflight fails
	Cadvisor         cadvisor.Interface
	ImageService     internalapi.ImageManagerService
	RuntimeService   internalapi.RuntimeService
	ContainerManager cm.ContainerManager
}

type Runtime struct {
	Candidate

	Ctx      context.Context
	Endpoint string
	once     sync.Once
}

var candidates = []Candidate{
	// Virtual runtimes (checked first)
	{
		Kind: Docker,
		Sockets: []string{
			"/var/run/docker.sock", "/run/docker.sock",
			os.Getenv("HOME") + "/.docker/run/docker.sock",
			os.Getenv("HOME") + "/.colima/default/docker.sock",
			os.Getenv("HOME") + "/.rd/docker.sock",
		},
		Preflight: func(dataDir string) string {
			// TODO: Docker Desktop disables the CRI plugin on its containerd.
			// Until we can enable CRI or use cri-dockerd, fall back to noop services.
			return ""
		},
		Cadvisor:         noop.NewCadvisor(),
		RuntimeService:   noop.NewRuntimeService(),
		ImageService:     noop.NewImageService(),
		ContainerManager: noop.NewContainerManager(),
	},
	{
		Kind: Podman,
		Sockets: []string{
			"/var/run/podman/podman.sock", "/run/podman/podman.sock",
			os.Getenv("HOME") + "/.local/share/podman/podman.sock",
			os.Getenv("XDG_RUNTIME_DIR") + "/podman/podman.sock",
		},
		// TODO: podman may need a CRI shim
		Cadvisor:         noop.NewCadvisor(),
		RuntimeService:   noop.NewRuntimeService(),
		ImageService:     noop.NewImageService(),
		ContainerManager: noop.NewContainerManager(),
	},
	// Native runtimes
	{
		Kind: Containerd,
		Sockets: []string{
			"/var/run/containerd/containerd.sock",
			os.Getenv("HOME") + "/.colima/default/containerd.sock",
		},
		Cadvisor:         noop.NewCadvisor(),
		RuntimeService:   noop.NewRuntimeService(),
		ImageService:     noop.NewImageService(),
		ContainerManager: noop.NewContainerManager(),
	},
	{
		Kind:             CriDockerd,
		Sockets:          []string{"/var/run/cri-dockerd.sock"},
		Cadvisor:         noop.NewCadvisor(),
		RuntimeService:   noop.NewRuntimeService(),
		ImageService:     noop.NewImageService(),
		ContainerManager: noop.NewContainerManager(),
	},
	{
		Kind:             CriO,
		Sockets:          []string{"/var/run/crio/crio.sock"},
		Cadvisor:         noop.NewCadvisor(),
		RuntimeService:   noop.NewRuntimeService(),
		ImageService:     noop.NewImageService(),
		ContainerManager: noop.NewContainerManager(),
	},
}

func NewRuntime(ctx context.Context, dataDir string) *Runtime {
	r := &Runtime{Ctx: ctx}
	r.detect(ctx, dataDir)
	log.Info().Str("runtime", r.Name()).Str("endpoint", r.Endpoint).Msg("using container runtime")
	return r
}

func (r *Runtime) detect(ctx context.Context, dataDir string) {
	r.once.Do(func() {
		var checked []string
		for i := range candidates {
			c := &candidates[i]
			for _, sock := range c.Sockets {
				checked = append(checked, sock)
				if _, err := os.Stat(sock); err != nil {
					continue
				}
				// Socket exists, run preflight if defined
				if c.Preflight != nil {
					if endpoint := c.Preflight(dataDir); endpoint != "" {
						r.Ctx = ctx
						r.Kind = c.Kind
						r.Endpoint = endpoint
						r.Candidate = *c
						return
					}
					// Preflight failed, try next candidate
					break
				}
				r.Ctx = ctx
				r.Kind = c.Kind
				r.Endpoint = "unix://" + sock
				r.Candidate = *c
				return
			}
		}
		log.Warn().Strs("checked", checked).Msg("no container runtime found")
		r.Candidate = Candidate{
			Cadvisor:         noop.NewCadvisor(),
			ImageService:     noop.NewImageService(),
			RuntimeService:   noop.NewRuntimeService(),
			ContainerManager: noop.NewContainerManager(),
		}
	})
}

func (r *Runtime) Name() string {
	return string(r.Kind)
}

func (r *Runtime) ContainerRuntimeEndpoint() string {
	return r.Endpoint
}

// SetCRIEndpoint overrides the container runtime endpoint with a CRI socket.
func (r *Runtime) SetCRIEndpoint(endpoint string) {
	r.Endpoint = endpoint
}

func (r *Runtime) Stop() {
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
	// TODO: maybe need to initialize based on type
	return r.Candidate.Cadvisor
}

func (r *Runtime) ImageService() internalapi.ImageManagerService {
	// TODO: maybe need to initialize based on type
	return r.Candidate.ImageService
}

func (r *Runtime) RuntimeService() internalapi.RuntimeService {
	// TODO: maybe need to initialize based on type
	return r.Candidate.RuntimeService
}

func (r *Runtime) ContainerManager() cm.ContainerManager {
	// TODO: maybe need to initialize based on type
	return r.Candidate.ContainerManager
}
