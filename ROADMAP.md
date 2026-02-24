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

The kubelet uses `kubemark.NewHollowKubelet()` which injects several fake/stub dependencies. Some can be backed by the Docker API; others are OS-level and should remain stubs.

### Docker API-Backable

#### 1. Cadvisor — Container & Filesystem Info

- **Location**: `pkg/cri/cadvisor.go`
- **Status**: `MachineInfo()` and `VersionInfo()` already use real data (gopsutil). The remaining 6 methods return empty values.
- **Methods to implement**:
  - `ContainerInfoV2(name)` — Use `docker container stats` / `ContainerStats` API to report per-container CPU, memory, network, and disk I/O. Map Docker container names back to pod sandbox IDs.
  - `GetRequestedContainersInfo(name)` — Same data source, different return type (`v1.ContainerInfo` vs `v2.ContainerInfo`).
  - `ImagesFsInfo()` — Use `docker system df` / `DiskUsage` API to report image layer storage (total, used, inodes).
  - `RootFsInfo()` — Use gopsutil `disk.Usage("/")` for the root filesystem. Not Docker-specific but needed for accurate node status.
  - `ContainerFsInfo()` — Use `DiskUsage` API for container writable layer storage.
  - `GetDirFsInfo(path)` — Use gopsutil `disk.Usage(path)` for arbitrary directory filesystem info.
- **Impact**: Enables `kubectl top nodes`, accurate node capacity reporting, and eviction manager decisions.

#### 2. ContainerManager — Resource Capacity & Allocation

- **Location**: Currently uses upstream `cm.NewStubContainerManager()`
- **Status**: All methods return nil/empty/zero.
- **Methods to implement**:
  - `GetCapacity(localStorageCapacityIsolation)` — Report real ephemeral storage capacity via gopsutil `disk.Usage`. Docker `Info` API can supplement with storage driver details.
  - `GetNodeAllocatableAbsolute()` — Currently hardcoded to 4 CPU / 4Gi in the fake. Should derive from actual host resources (gopsutil) minus any configured system-reserved.
  - `GetResources(pod, container)` — Use `docker container inspect` to return device mounts and resource configs applied to running containers.
  - `Start()` — Initialize Docker client connection, verify runtime is accessible.
- **Approach**: Create `pkg/cri/container_manager.go` implementing `cm.ContainerManager`. Delegate cgroup-specific methods (QOS cgroups, cgroup root, CPU/memory managers) to no-op stubs since NanoKube doesn't manage cgroups directly — Docker handles that.

#### 3. EventRecorder — Real API Server Events

- **Location**: Currently uses `record.FakeRecorder{}` (drops events or writes to a channel)
- **Status**: No events are recorded to the API server.
- **Fix**: Replace with `record.NewBroadcaster()` + `recorder.NewRecorder()` using the existing k8s clientset. This is straightforward — the kubelet already has a client connection. The comment in upstream says "With real recorder we attempt to read /dev/kmsg" — investigate whether this is a blocker or can be avoided with config.
- **Impact**: `kubectl get events` will show kubelet lifecycle events (node ready, pod scheduled, image pulled, container started/failed).

#### 4. ProbeManager — Health Check Execution

- **Location**: Currently uses `probetest.FakeManager{}` (marks all containers Ready)
- **Status**: No actual health checks run. All containers are unconditionally reported as ready.
- **Fix**: Use the real `prober.Manager` from `pkg/kubelet/prober/`. It already integrates with the CRI runtime service for exec probes and uses net/http for HTTP probes. The real manager needs a functioning CRI exec path (which we have via `pkg/cri/docker/streaming.go`).
- **Impact**: Liveness, readiness, and startup probes will actually execute. Pods with failing probes will be restarted or removed from service endpoints.

### OS-Level — Keep as Stubs

These fakes operate at the OS/kernel level and have no Docker API equivalent. They are appropriate stubs for NanoKube's single-process architecture.

| Fake | Interface | Why Keep |
|------|-----------|----------|
| `FakeOS` | `kubecontainer.OSInterface` | Filesystem ops (mkdir, symlink, chmod). Docker doesn't manage kubelet's volume mount paths. |
| `FakeOOMAdjuster` | `OOMAdjuster` | Linux cgroup OOM score adjustment. Kernel-level, not exposed via Docker. |
| `FakeMounter` | `mount.Interface` | OS mount/unmount syscalls. Docker manages container mounts internally. |
| `FakeSubpath` | `subpath.Interface` | Bind-mount subpaths within volumes. Pure kubelet filesystem logic. |
| `FakeHostUtil` | `hostutil.HostUtils` | Device checks, SELinux support, file type queries. OS-level. |
| `NoopTracerProvider` | `trace.TracerProvider` | OpenTelemetry tracing. No-op is correct unless tracing is explicitly wanted. |

### Implementation Order

1. **EventRecorder** — Lowest effort, highest visibility. Swap `FakeRecorder` for a real broadcaster.
2. **Cadvisor filesystem methods** — `RootFsInfo` and `GetDirFsInfo` via gopsutil, `ImagesFsInfo` via Docker `DiskUsage`.
3. **ContainerManager capacity** — `GetCapacity` and `GetNodeAllocatableAbsolute` with real values.
4. **Cadvisor container stats** — `ContainerInfoV2` via Docker `ContainerStats` streaming API.
5. **ProbeManager** — Swap fake for real `prober.Manager`. Requires validated CRI exec path.

## Podman Backend

- **Status**: Not started
- **Location**: `pkg/cri/podman/`
- **Approach**: Use `github.com/containers/podman` Go bindings (not the Docker-compat shim) to implement the `cri.Backend` interface. Separate TDD cycle against critest with `--runtime-endpoint` pointed at a Podman-backed CRI socket.
