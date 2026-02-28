# CLAUDE.md

## Project Overview

NanoKube is a single-binary, minimalist Kubernetes distribution written in Go. It runs all Kubernetes control plane components (etcd, kube-apiserver, kube-controller-manager, kube-scheduler, kubelet) in a single process.

## Build & Run

```bash
# Initialize git submodules (required before first build)
make submodules

# Build the binary (applies patches, outputs ./nanokube)
make build

# Format, build, and run with --clean flag
make run

# Run the binary directly
./nanokube --clean        # wipe data directory on startup
./nanokube --name mycluster --data ~/.nanokube -v
```

Build uses `CGO_ENABLED=0` and strips debug symbols (`-ldflags="-s -w"`).

## Testing

```bash
make test          # runs go test ./...
make e2e           # build, start nanokube, run kuttl e2e suite (10 tests)
make e2e E2E_TEST=exec   # run a single e2e test by name
make critest       # CRI conformance tests against the Docker backend
```

E2E test dirs: `tests/e2e/{pod,emptydir,configmap,secret,downwardapi,projected,pvc,hostpath,exec,dns}`

## Formatting

```bash
make fmt        # runs gofmt -w .
```

No linter is configured.

## Key Commands

| Command | Description |
|---------|-------------|
| `make submodules` | Init/update git submodules (etcd, kubernetes) |
| `make patch` | Apply Kubernetes patches from `patches/kubernetes.patch` |
| `make patch-save` | Save current Kubernetes changes back to patch file |
| `make build` | Patch + build the binary |
| `make clean` | Remove the binary |
| `make test` | Run Go unit tests |
| `make e2e` | Build + run kuttl e2e tests (10 tests) |
| `make critest` | CRI conformance tests |
| `make fmt` | Format Go code |
| `make run` | Format, build, and run |

## Architecture

- **`main.go`** — Entry point. Uses Cobra CLI. Initializes config and starts components sequentially.
- **`internal/`** — Component wrappers. Each Kubernetes component implements a `Component` interface with a `Start()` method. Components start in order: etcd → apiserver → controller-manager → scheduler → kubelet.
- **`pkg/config/`** — Central configuration: data directory, TLS cert generation (EC P-256, self-signed), kubeconfig, kubelet config, container runtime auto-detection.
- **`pkg/kubelet/`** — HollowKubelet wrapper using Kubernetes's kubemark.
- **`pkg/cri/`** — CRI (Container Runtime Interface) implementation. `backend.go` defines the Backend interface; `docker/` implements it using the Docker Engine API; `podman/` is a stub for future Podman support.
- **`pkg/stub/`** — Stub implementations for OS-level kubelet dependencies (mounter, hostutil, subpath, tracer, cAdvisor).
- **`patches/`** — Minimal patches to Kubernetes source (currently only kube-scheduler context handling).

## Dependencies

Go 1.25.4 with Kubernetes v1.35.1 and etcd v3.6. The `etcd/` and `kubernetes/` directories are git submodules pointing to upstream release branches.

## Code Conventions

- Standard Go project layout (`internal/`, `pkg/`)
- Structured logging via `zerolog`
- Error handling with standard Go error returns; fatal errors use `log.Fatal()`
- Components run in background goroutines with health-check polling loops
- Configuration uses lazy evaluation (files created on first access)

## CRI Docker Backend Notes

- Pods use Docker's default bridge network (no custom bridge)
- `Nameservers()` probes DNS via `RunProbe` (busybox cat /etc/resolv.conf) to match what containers see
- The kubelet reads container identity from CRI **Labels** (not Metadata): `io.kubernetes.container.name`, `io.kubernetes.pod.name`, `io.kubernetes.pod.namespace`, `io.kubernetes.pod.uid`
- The kubelet reads hash/restart count from CRI **Annotations**: `io.kubernetes.container.hash`, `io.kubernetes.container.restartCount`
- `extractLabels()` must NOT strip labels the kubelet needs — only internal ones (`docker.type`, `managed-by`, `sandbox.id`, `container.attempt`, `container.logPath`)
- Exec streaming: `proxyStreams` must close stdin and `resp.Conn` after output copy finishes, or interactive shells hang on exit
- WebSocket "use of closed network connection" at `conn.go:339` is cosmetic upstream noise — not fixable without patching kubernetes source
