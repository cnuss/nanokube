# nanokube

A single-binary Kubernetes distribution that runs etcd, kube-apiserver, kube-controller-manager, kube-scheduler, and kubelet in one process.

## Quick Start

```bash
git clone --recurse-submodules https://github.com/cnuss/nanokube.git
cd nanokube
make build
./nanokube --clean
```

Requires Go 1.25+ and a container runtime (Docker recommended).

## Usage

```
nanokube [flags]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--name` | `nanokube` | Cluster name |
| `--data` | `~/.nanokube` | Data directory |
| `--clean` | `false` | Clean data directory before starting |
| `--kubelet` | `true` | Start the kubelet component |
| `-v, --verbose` | `false` | Enable debug logging (`-v` debug, `-vv` trace) |

## Container Runtimes

nanokube includes its own CRI (Container Runtime Interface) daemon that bridges Kubernetes to container runtimes directly — no external CRI shim required.

| Runtime | Status |
|---------|--------|
| Docker | Supported |
| Podman | Planned |

Docker is auto-detected by probing common socket paths (`/var/run/docker.sock`, `~/.colima/default/docker.sock`, `~/.rd/docker.sock`, etc.). If no runtime is found, nanokube starts with a no-op backend.

## Build

```bash
make submodules   # init git submodules (etcd, kubernetes)
make build        # apply patches + build binary
make run          # fmt, build, and run
make run-clean    # fmt, build, and run with --clean
```

## Testing

```bash
make test                        # unit tests
make e2e                         # kuttl e2e suite (24 tests)
make e2e WHAT=exec               # single e2e test by name
make critest                     # CRI conformance tests
make critest WHAT="port mapping" # single critest by focus
make scenarios                   # kuttl scenario tests
```

## License

[Apache 2.0](LICENSE)
