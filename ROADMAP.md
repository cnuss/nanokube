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

The kubelet uses `kubemark.NewHollowKubelet()` which injects several fake/stub dependencies. Some have been replaced with real Docker-backed implementations; others are OS-level and remain stubs. Kubelet deps live in `pkg/kubernetes/kubelet/`.

### OS-Level Stubs (Need Rethinking)

| Stub             | Interface            | Status                                                                                                                                                                                                             |
| ---------------- | -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `ScopedMounter`  | `mount.Interface`    | Partial — tmpfs mounts tracked via `sync.Map`, `IsLikelyNotMountPoint`/`IsMountPoint` check tracking map. Other fstypes return `errNotImplemented`. Lives in `pkg/kubernetes/kubelet/mounter/`. |
| `ScopedHostUtil` | `hostutil.HostUtils` | Partial — `PathExists`, `GetFileType`, `EvalHostSymlinks` are real (os.Stat + mode inspection). `GetOwner`, `GetMode` return `errNotImplemented`. No-ops: `MakeRShared`, `DeviceOpened`, `PathIsDevice`, `GetSELinuxSupport`, `GetSELinuxMountContext`. |
| `ScopedSubpath`  | `subpath.Interface`  | Partial — `SafeMakeDir` is real (scoped to DataDir). `CleanSubPaths` and `PrepareSafeSubpath` are no-ops.                                                                                                          |

All stubbed methods emit `Warn()` when called.

### OS-Level — Real Implementations

| Component         | Interface                   | Notes                                                                           |
| ----------------- | --------------------------- | ------------------------------------------------------------------------------- |
| `ScopedOS`        | `kubecontainer.OSInterface` | Real scoped implementation — remaps paths outside DataDir. `Hostname()` returns `os.Hostname()` — should return the configured node name instead. |
| `FakeOOMAdjuster` | `OOMAdjuster`               | Kernel-level `/proc` writes. Docker handles container OOM via container config. |
| `TracerProvider`  | `trace.TracerProvider`      | No-op wrapping `noop.TracerProvider` with warn logging.                         |

## Pod Volume Types

Support for non-deprecated volume types in the Pod spec. Stack-ranked by impact — what unblocks the most real workloads with the least effort.

### P0 — Required for basic pods

| # | Volume Type | Status | Notes |
|---|-------------|--------|-------|
| 1 | `EmptyDir` | **Done** | Default medium uses bind mount from kubelet dir. `Medium: Memory` uses Docker native tmpfs via `MountLookup` interface. |
| 2 | `Projected` | **Done** | SA token + `kube-root-ca.crt` ConfigMap + namespace DownwardAPI. Plugin wraps EmptyDir with `StorageMediumMemory`, AtomicWriter pre-writes files; `GetTmpfs` detects content and falls back to bind mount. |
| 3 | `Secret` | **Done** | Same write-then-mount pattern as Projected. `GetTmpfs` content check handles it. |
| 4 | `ConfigMap` | **Done** | Application config. Uses default (disk-based) EmptyDir — AtomicWriter pre-writes data, CRI creates bind mount naturally. Same write-and-mount pattern as Secret. |

### P1 — Common workloads

| # | Volume Type | Status | Notes |
|---|-------------|--------|-------|
| 5 | `DownwardAPI` | Not started | Pod metadata as files (labels, annotations, resource limits). Same mount pattern as Secret/ConfigMap. |
| 6 | `HostPath` | **Done** | System agents, log collectors, dev mounts. HostPath plugin doesn't mount — kubelet passes host path directly as CRI mount source. Required `ScopedHostUtil.PathExists`/`GetFileType`/`EvalHostSymlinks`. |
| 7 | `PersistentVolumeClaim` | Not started | Stateful workloads (databases, queues). Requires a volume plugin or CSI driver to provision/attach. |

### P2 — Advanced / infrastructure

| # | Volume Type | Status | Notes |
|---|-------------|--------|-------|
| 8 | `CSI` | Not started | Ephemeral CSI volumes. Requires CSI node plugin infrastructure. |
| 9 | `Ephemeral` | Not started | Cluster-driver ephemeral volumes. Built on CSI. |
| 10 | `NFS` | Not started | Network filesystem. Needs real `ScopedMounter.Mount` with NFS support and host nfs-utils. |
| 11 | `ISCSI` | Not started | iSCSI disk mount. Needs host-level iSCSI tooling (iscsiadm). |
| 12 | `FC` | Not started | Fibre Channel mount. Needs host-level FC tooling. Datacenter-only. |

## Kubelet Streaming (exec/attach/portforward)

- **Bug**: `kubectl exec` fails with `http: server gave HTTP response to HTTPS client`. The apiserver dials the kubelet streaming endpoint over HTTPS (`:10250/exec/...`) but the HollowKubelet serves plain HTTP.
- **Status**: Not started
- **Fix**: Configure the kubelet's streaming server with TLS using the same certificates generated by `pkg/config/`. Either pass the node cert/key to the kubelet server config or set up a TLS-terminating wrapper.

## Docker Volume & Network Support

- **Volumes**: `CreateVolume`, `RemoveVolume`, `RemoveVolumes`, `ListVolumes` on the `Backend` interface. Docker implementation uses named volumes with `managed-by` labels and cluster-name prefix. Pod volumes are cleaned up on sandbox removal.
- **Network**: `EnsureNetwork`, `RemoveNetworks` on the `Backend` interface. `NetworkType` enum (`bridge`, `host`, `none`) in `pkg/cri/types/`. Bridge creates a dedicated `<cluster>-bridge` network by cloning the built-in bridge config. Host and none are no-ops. Non-host-network sandboxes lazily ensure bridge on `RunPodSandbox`.
- **Cleanup**: Centralized `Cleanup()` in `pkg/cri/backend.go` — calls `RemoveContainers` → `RemovePodSandboxes` → `RemoveVolumes` → `RemoveNetworks` with 30s timeout. Invoked via `ctx.Done`.

## Logging

Structured logging via `zerolog` with component-scoped loggers. All packages use `component.NewLogger("name")` which delegates to a shared root logger at call time. `component.Setup()` handles bootstrap: flag parsing, log level, data dir clean, and log file mirroring (console + disk). Verbosity: default=info, `-v`=debug, `-vv`=trace.

## Podman Backend

- **Status**: Not started
- **Location**: `pkg/cri/podman/`
- **Approach**: Use `github.com/containers/podman` Go bindings (not the Docker-compat shim) to implement the `cri.Backend` interface. Separate TDD cycle against critest with `--runtime-endpoint` pointed at a Podman-backed CRI socket.
