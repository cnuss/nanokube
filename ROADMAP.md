# Roadmap

## CRI Conformance

**Current: 42/45 passing (93%), 5 skipped**

### Known Failures (Docker Desktop macOS)

All 3 remaining failures are caused by Docker Desktop running containers inside a Linux VM. Container IPs (e.g. `172.17.0.x`) are not routable from the macOS host. These tests pass on native Linux where the host can reach container IPs directly.

#### 1. Streaming PortForward

- **Test**: `runtime should support portforward [Conformance]`
- **Root cause**: `PortForward` in `pkg/cri/docker/streaming.go` dials `<containerIP>:<port>` via `net.DialTimeout`. On Docker Desktop macOS, the container IP lives inside the VM network and is unreachable from the host.
- **Fix**: Use `docker exec` inside a sidecar or the workload container to proxy TCP traffic (e.g. exec `socat` or a built-in Go TCP proxy binary injected into the sandbox). Alternatively, use Docker's built-in port publishing as a fallback when direct dial fails.

#### 2. Port Mapping

- **Test**: `runtime should support port mapping with only container port [Conformance]`
- **Root cause**: Test creates a sandbox with port mappings, starts a container listening on that port, then tries to connect to `<containerIP>:<containerPort>`. Same VM networking issue — the container IP is unreachable from the host even though Docker correctly publishes the port.
- **Fix**: Same as PortForward — requires a proxy path that doesn't depend on direct IP reachability. On Docker Desktop, connecting via `localhost:<hostPort>` works but critest expects `<containerIP>:<containerPort>`.

#### 3. ExecSync with Timeout

- **Test**: `runtime should support execSync with timeout [Conformance]`
- **Root cause**: `ExecSync` in `pkg/cri/docker/container.go` uses `context.WithTimeout` to bound the exec. When the timeout fires, Go cancels the context and stops reading output, but Docker's exec process keeps running server-side. The test expects `stdout` to be empty on timeout, but we may have already read partial output before the deadline.
- **Fix**: After timeout, discard any buffered stdout/stderr and return empty byte slices. Alternatively, use the Docker API's `ContainerExecInspect` to check if the process is still running and explicitly signal it (though Docker doesn't expose a kill-exec API).

### Skipped Tests (5)

These are skipped by critest itself (not failures), typically platform-specific tests (e.g. Windows containers, AppArmor/SELinux profiles).

## HollowKubelet: Replace Fakes with Real Implementations

The kubelet uses `kubemark.NewHollowKubelet()` which injects several fake/stub dependencies. Some have been replaced with real Docker-backed implementations; others are OS-level and remain stubs.

### OS-Level Stubs (Need Rethinking)

| Stub | Interface | Status |
|------|-----------|--------|
| `ScopedMounter` | `mount.Interface` | Stubbed — all methods return `errNotImplemented` (except `CanSafelySkipMountPointCheck`). Previously had in-memory mount tracking; removed pending redesign. |
| `ScopedHostUtil` | `hostutil.HostUtils` | Stubbed — `PathExists`, `GetFileType`, `EvalHostSymlinks`, `GetOwner`, `GetMode` return `errNotImplemented`. No-ops: `MakeRShared`, `DeviceOpened`, `PathIsDevice`, `GetSELinuxSupport`, `GetSELinuxMountContext`. |
| `ScopedSubpath` | `subpath.Interface` | Partial — `SafeMakeDir` is real (scoped to DataDir). `CleanSubPaths` and `PrepareSafeSubpath` are no-ops. |

All stubbed methods emit `log.Warn()` when called.

### OS-Level — Real Implementations

| Component | Interface | Notes |
|-----------|-----------|-------|
| `ScopedOS` | `kubecontainer.OSInterface` | Real scoped implementation — remaps paths outside DataDir. |
| `FakeOOMAdjuster` | `OOMAdjuster` | Kernel-level `/proc` writes. Docker handles container OOM via container config. |
| `NoopTracerProvider` | `trace.TracerProvider` | No-op is correct unless tracing is explicitly wanted. |

## Docker Volume & Network Support

- **Volumes**: `CreateVolume`, `RemoveVolume`, `RemoveVolumes`, `ListVolumes` on the `Backend` interface. Docker implementation uses named volumes with `managed-by` labels and cluster-name prefix. Pod volumes are cleaned up on sandbox removal.
- **Network**: `EnsureNetwork`, `RemoveNetworks` on the `Backend` interface. `NetworkType` enum (`bridge`, `host`, `none`) in `pkg/cri/types/`. Bridge creates a dedicated `<cluster>-bridge` network by cloning the built-in bridge config. Host and none are no-ops. Non-host-network sandboxes lazily ensure bridge on `RunPodSandbox`.
- **Cleanup**: Centralized `Cleanup()` in `pkg/cri/backend.go` — calls `RemoveContainers` → `RemovePodSandboxes` → `RemoveVolumes` → `RemoveNetworks` with 30s timeout. Invoked via `ctx.Done`.

## Podman Backend

- **Status**: Not started
- **Location**: `pkg/cri/podman/`
- **Approach**: Use `github.com/containers/podman` Go bindings (not the Docker-compat shim) to implement the `cri.Backend` interface. Separate TDD cycle against critest with `--runtime-endpoint` pointed at a Podman-backed CRI socket.
