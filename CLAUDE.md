# CLAUDE.md

## Project Overview

NanoKube is a single-binary Kubernetes distribution written in Go. It runs etcd, kube-apiserver, kube-controller-manager, kube-scheduler, and kubelet in a single process. It includes its own CRI daemon (CRID) that bridges Kubernetes to container runtimes directly.

## Build & Run

```bash
make submodules        # init git submodules (required before first build)
make build             # apply patches + build binary
make run               # fmt, build, run
./nanokube             # run directly (--clean removes data dir first)
```

Build uses `CGO_ENABLED=0` and strips debug symbols (`-ldflags="-s -w"`).

## Testing

```bash
make test                        # go test ./... (placeholder, no unit tests yet)
make critest                     # CRI conformance tests (45/45 passing)
make critest WHAT="port mapping" # single critest by focus
make e2e                         # kuttl e2e suite (25 tests, all passing)
make e2e WHAT=exec               # single e2e test by name
make scenarios                   # composite kuttl tests (multiple functionalities)
```

E2E test dirs: `tests/e2e/` (25 dirs: pod, exec, dns, deployment, cronjob, job, pvc, configmap, secret, emptydir, hostpath, downwardapi, projected, env, probe, lifecycle, logs, init-container, multi-container, resource-limits, restart, node, host-network, host-pid, host-ipc)

### Development test flow

Run `make critest` first, then `make e2e`. Localize specific failures, fix, repeat. Commit after each fix or all-green run.

### Expected state after tests

**After `make critest`:**
- `docker ps -a`: empty (no containers)
- `docker info`: Containers 0

**After `make e2e`:**
- `docker ps -a`: empty (sandbox removal cascades to all containers, freeing ports for restart)
- `docker info`: Containers 0
- `docker volume ls`: volumes may remain (expected)

All tests are idempotent — `make critest` and `make e2e` can each be run multiple times in a row without `--clean`.

### Test environment

- All tests run against Docker only (other runtimes not yet implemented)
- stdout/stderr is blackholed during test runs; inspect nanokube logs in the temp data directory
- `--clean` removes the data directory on startup; not needed for test repeatability
- Increase verbosity (`-v`, `-vv`) or add logging to diagnose failures

## Key Commands

| Command | Description |
|---------|-------------|
| `make submodules` | Init/update git submodules (etcd, kubernetes) |
| `make patch` | Apply Kubernetes patches from `patches/kubernetes.patch` |
| `make patch-save` | Save current Kubernetes changes back to patch file |
| `make build` | Patch + build the binary |
| `make clean` | Remove the binary |
| `make test` | Run Go unit tests |
| `make e2e` | Build + run kuttl e2e tests |
| `make critest` | CRI conformance tests |
| `make scenarios` | Kuttl scenario tests |
| `make fmt` | Format Go code |
| `make run` | Format, build, and run |

## Architecture

- **`main.go`** — Entry point (Cobra CLI). Starts components sequentially: CRID -> Manifests -> Kubelet. Graceful shutdown in reverse order.
- **`pkg/component/`** — Component interface (Start/Stop lifecycle), structured logging setup, readiness polling helpers.
- **`pkg/config/`** — CLI options, data directory layout, TLS cert generation (ECDSA P-256, self-signed), kubeconfig generation.
- **`pkg/crid/`** — Custom CRI daemon. Detects container runtimes, starts gRPC CRI servers, manages container lifecycle events.
  - **`backend/`** — Runtime abstraction: Backend interface, gRPC server, streaming, CSI driver, cadvisor, volume provisioning, probes, event recording.
  - **`docker/`** — Docker backend: implements `RuntimeServiceServer` + `ImageServiceServer` via Docker Engine API.
  - **`podman/`** — Podman backend (stub, not yet implemented).
  - **`labels/`** — Container label builder for identifying managed containers.
- **`pkg/kubernetes/`** — Manifests component (embeds kube-system pod spec for apiserver/controller-manager/scheduler), kubelet component, kubeconfig writing.
- **`pkg/etcd/`** — Symlink to etcd submodule.
- **`patches/`** — Minimal patches to Kubernetes source.

## Dependencies

Go 1.25.4 with Kubernetes v1.35.1 and etcd v3.6. The `etcd/` and `kubernetes/` directories are git submodules pointing to upstream release branches. The go.mod uses `replace` directives to reference local submodules.

## Code Conventions

- Standard Go layout (`pkg/`)
- Structured logging via `zerolog`
- Components run in background goroutines with health-check polling
- Lazy initialization throughout (certs, backends, hosts)
- No linter configured; `make fmt` runs `gofmt`
