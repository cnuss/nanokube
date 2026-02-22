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

type Runtime struct {
	ctx      context.Context
	once     sync.Once
	kind     RuntimeType
	endpoint string
}

func NewRuntime(ctx context.Context) *Runtime {
	r := &Runtime{ctx: ctx}
	r.detect()
	log.Info().Str("runtime", r.Name()).Str("socket", r.endpoint).Msg("using container runtime")
	return r
}

var nativeRuntimes = map[RuntimeType]string{
	Containerd: "/var/run/containerd/containerd.sock",
	CriDockerd: "/var/run/cri-dockerd.sock",
	CriO:       "/var/run/crio/crio.sock",
}

// And include home directories for rootless runtimes
var virtualRuntimes = map[RuntimeType][]string{
	Docker: {"/var/run/docker.sock", "/run/docker.sock", os.Getenv("HOME") + "/.docker/run/docker.sock", os.Getenv("HOME") + "/.colima/default/docker.sock", os.Getenv("HOME") + "/.rd/docker.sock"},
	Podman: {"/var/run/podman/podman.sock", "/run/podman/podman.sock", os.Getenv("HOME") + "/.local/share/podman/podman.sock", os.Getenv("XDG_RUNTIME_DIR") + "/podman/podman.sock"},
}

func (r *Runtime) detect() {
	r.once.Do(func() {
		for kind, socks := range virtualRuntimes {
			for _, sock := range socks {
				if _, err := os.Stat(sock); err == nil {
					r.kind = kind
					r.endpoint = "unix://" + sock
					return
				}
			}
		}
		for kind, sock := range nativeRuntimes {
			if _, err := os.Stat(sock); err == nil {
				r.kind = kind
				r.endpoint = "unix://" + sock
				return
			}
		}
		socks := make([]string, 0, len(virtualRuntimes)+len(nativeRuntimes))
		for _, socksList := range virtualRuntimes {
			socks = append(socks, socksList...)
		}
		for _, sock := range nativeRuntimes {
			socks = append(socks, sock)
		}
		log.Fatal().Strs("checked", socks).Msg("no container runtime found")
	})
}

func (r *Runtime) Name() string {
	return string(r.kind)
}

func (r *Runtime) ContainerRuntimeEndpoint() string {
	switch r.kind {
	case Docker:
		// Docker requires cri-dockerd as a CRI shim
		sock := "/var/run/cri-dockerd.sock"
		if _, err := os.Stat(sock); err != nil {
			log.Fatal().Str("socket", sock).Msg("cri-dockerd is required for Docker runtime but not running")
		}
		return "unix://" + sock
	case Podman:
		// TODO: podman may need a CRI shim
		return r.endpoint
	default:
		return r.endpoint
	}
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
