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
make test        # build, start nanokube, apply all-volumes pod, print logs, wait for Ctrl+C
make unit-test   # runs go test ./...
```

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
| `make test` | Smoke test: build, start, apply all-volumes pod, print logs, wait for Ctrl+C |
| `make unit-test` | Run Go unit tests |
| `make fmt` | Format Go code |
| `make run` | Format, build, and run |

## Architecture

- **`main.go`** — Entry point. Uses Cobra CLI. Initializes config and starts components sequentially.
- **`internal/`** — Component wrappers. Each Kubernetes component implements a `Component` interface with a `Start()` method. Components start in order: etcd → apiserver → controller-manager → scheduler → kubelet.
- **`pkg/config/`** — Central configuration: data directory, TLS cert generation (EC P-256, self-signed), kubeconfig, kubelet config, container runtime auto-detection.
- **`pkg/kubelet/`** — HollowKubelet wrapper using Kubernetes's kubemark.
- **`pkg/stub/`** — Stub implementations for CRI runtime, image service, container manager, and cAdvisor. Allows running without a real container runtime.
- **`patches/`** — Minimal patches to Kubernetes source (currently only kube-scheduler context handling).

## Dependencies

Go 1.25.4 with Kubernetes v1.35.1 and etcd v3.6. The `etcd/` and `kubernetes/` directories are git submodules pointing to upstream release branches.

## Code Conventions

- Standard Go project layout (`internal/`, `pkg/`)
- Structured logging via `zerolog`
- Error handling with standard Go error returns; fatal errors use `log.Fatal()`
- Components run in background goroutines with health-check polling loops
- Configuration uses lazy evaluation (files created on first access)
