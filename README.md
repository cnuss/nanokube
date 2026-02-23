# nanokube

A single binary that runs all Kubernetes control plane components in one process: etcd, kube-apiserver, kube-controller-manager, kube-scheduler, and kubelet.

## Quick Start

```bash
go install github.com/cnuss/nanokube@latest
nanokube
```

## Usage

```
nanokube [flags]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--name` | `nanokube` | Cluster name |
| `--data` | `~/.nanokube` | Data directory |
| `--clean` | `false` | Clean data directory before starting |
| `-v, --verbose` | `false` | Enable debug logging |

## Container Runtimes

nanokube auto-detects the container runtime by probing for sockets:

| Runtime | Detection |
|---------|-----------|
| Docker | `/var/run/docker.sock` (requires cri-dockerd) |
| Podman | `/var/run/podman/podman.sock` |
| containerd | `/var/run/containerd/containerd.sock` |
| CRI-O | `/var/run/crio/crio.sock` |

If no runtime is found, nanokube starts with stub implementations (useful for development/testing).

## Install

```bash
go install github.com/cnuss/nanokube@latest
```

Or build from source:

```bash
git clone --recurse-submodules https://github.com/cnuss/nanokube.git
cd nanokube
make build
```

Requires Go 1.25+.

## License

See [LICENSE](LICENSE).
