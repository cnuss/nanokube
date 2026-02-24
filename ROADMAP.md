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

### Completed

#### 1. Cadvisor — Machine Info, Version Info, Filesystem Info

- **Location**: `pkg/cri/cadvisor.go`, `pkg/cri/backend.go`
- **Status**: Done. All methods backed by the container runtime via the `Backend` interface (no gopsutil dependency).
- **MachineInfo**: CPU count, memory, boot ID, system UUID, machine ID — all from `Backend.HostInfo()` and `Backend.HostIDs()`. HostIDs runs a privileged busybox probe with host namespaces to read `/proc/sys/kernel/random/boot_id`, `/sys/class/dmi/id/product_uuid` (with device-tree fallbacks), and `/etc/machine-id`.
- **VersionInfo**: Kernel version and OS from `Backend.HostInfo()` (wraps `docker info`).
- **FsInfo methods** (`ImagesFsInfo`, `RootFsInfo`, `ContainerFsInfo`, `GetDirFsInfo`): Use `Backend.RunProbe()` to run `stat -f` inside a busybox container with host path bind-mounted. Results cached with 60s TTL.
- **Remaining stubs**: `ContainerInfoV2` and `GetRequestedContainersInfo` still return empty maps (per-container stats not yet implemented).

#### 2. ContainerManager — Resource Capacity

- **Location**: `pkg/kubernetes/kubelet.go`
- **Status**: Done. `buildContainerManager()` wraps the upstream stub with a `capacityContainerManager` that overrides `GetCapacity()` with real CPU/memory from `cadvisor.MachineInfo()`.
- **Remaining stubs**: `GetNodeAllocatableAbsolute`, `GetResources`, cgroup methods — delegated to the embedded stub.

#### 3. EventRecorder — Real API Server Events

- **Status**: Done. `hk.KubeletDeps.Recorder` set to `nil` after `NewHollowKubelet()`, causing the kubelet to create a real event broadcaster. Events (node ready, pod scheduled, image pulled) now appear in `kubectl get events`.

#### 4. ProbeManager — Health Check Execution

- **Status**: Done. `hk.KubeletDeps.ProbeManager` set to `nil`, causing the kubelet to create a real probe manager. Liveness, readiness, and startup probes execute via the CRI exec path.

### Future Work

- **Cadvisor container stats**: `ContainerInfoV2` and `GetRequestedContainersInfo` — use Docker `ContainerStats` API for per-container CPU/memory/network/disk I/O. Enables `kubectl top pods`.
- **ContainerManager allocatable**: `GetNodeAllocatableAbsolute` — derive from host resources minus system-reserved.

### OS-Level — Keep as Stubs

These fakes operate at the OS/kernel level and have no Docker API equivalent. They are appropriate stubs for NanoKube's single-process architecture.

| Fake | Interface | Why Keep |
|------|-----------|----------|
| `ScopedOS` | `kubecontainer.OSInterface` | Filesystem ops scoped to DataDir. Docker doesn't manage kubelet's volume mount paths. |
| `FakeOOMAdjuster` | `OOMAdjuster` | Linux cgroup OOM score adjustment. Kernel-level, not exposed via Docker. |
| `FakeMounter` | `mount.Interface` | OS mount/unmount syscalls. Docker manages container mounts internally. |
| `FakeSubpath` | `subpath.Interface` | Bind-mount subpaths within volumes. Pure kubelet filesystem logic. |
| `FakeHostUtil` | `hostutil.HostUtils` | Device checks, SELinux support, file type queries. OS-level. |
| `NoopTracerProvider` | `trace.TracerProvider` | OpenTelemetry tracing. No-op is correct unless tracing is explicitly wanted. |

## Podman Backend

- **Status**: Not started
- **Location**: `pkg/cri/podman/`
- **Approach**: Use `github.com/containers/podman` Go bindings (not the Docker-compat shim) to implement the `cri.Backend` interface. Separate TDD cycle against critest with `--runtime-endpoint` pointed at a Podman-backed CRI socket.
