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
| `mounter`        | `mount.Interface`    | Partial — tmpfs mounts tracked via `sync.Map`, `IsLikelyNotMountPoint`/`IsMountPoint` check tracking map. Other fstypes return `errNotImplemented`. Consolidated into `pkg/cri/volume_plugin.go`. |
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

### P1 — Common workloads

| # | Volume Type | Status | Notes |
|---|-------------|--------|-------|
| 1 | `PersistentVolumeClaim` | Done | Local PVs backed by Docker named volumes. ScopedMounter tracks bind mounts, CRI pre-creates volumes. |
| 2 | `PVC` auto-provisioning | Done | Default StorageClass created in `volumePlugin.Init()`. Watch-based provisioner in `kubelet.go` creates HostPath PVs backed by Docker named volumes for pending PVCs. |

### P2 — Advanced / infrastructure

| # | Volume Type | Status | Notes |
|---|-------------|--------|-------|
| 3 | `CSI` | Not started | Ephemeral CSI volumes. Requires CSI node plugin infrastructure. |
| 4 | `Ephemeral` | Not started | Cluster-driver ephemeral volumes. Built on CSI. |
| 5 | `NFS` | Not started | Network filesystem. Needs real `ScopedMounter.Mount` with NFS support and host nfs-utils. |
| 6 | `ISCSI` | Not started | iSCSI disk mount. Needs host-level iSCSI tooling (iscsiadm). |
| 7 | `FC` | Not started | Fibre Channel mount. Needs host-level FC tooling. Datacenter-only. |

## Etcd Compaction

- **Bug**: etcd's background `purgeFile` goroutine crashes with `open .../member/snap: no such file or directory` when the data directory is cleaned while etcd is running. This happens because `--clean` removes the data dir but etcd's compaction/snap purge loop still references the old path.
- **Status**: Not started
- **Fix**: Disable etcd auto-compaction (`--auto-compaction-retention=0`) or ensure the snap directory exists before etcd starts. For a single-node ephemeral cluster, compaction adds no value.

## ClusterDNS Configuration

- **Bug**: Kubelet warns `kubelet does not have ClusterDNS IP configured and cannot create Pod using "ClusterFirst" policy. Falling back to "Default" policy.` for every pod.
- **Status**: Not started
- **Fix**: Set `cfg.ClusterDNS` in `ApplyKubeletConfig` to the cluster DNS service IP (typically `10.96.0.10`). Optionally deploy CoreDNS as a static pod or in-process DNS server so ClusterFirst resolution actually works.

## Kubelet Streaming (exec/attach/portforward)

- **Bug**: `kubectl exec` fails with `http: server gave HTTP response to HTTPS client`. The apiserver dials the kubelet streaming endpoint over HTTPS (`:10250/exec/...`) but the HollowKubelet serves plain HTTP.
- **Status**: Done — set `KubeletDeps.TLSOptions` with the node cert/key from `pkg/config/` so the kubelet serves HTTPS on port 10250. `kubectl exec` now works.

## Docker Volume & Network Support

- **Volumes**: `CreateVolume`, `RemoveVolume`, `RemoveVolumes`, `ListVolumes` on the `Backend` interface. Docker implementation uses named volumes with `managed-by` labels and cluster-name prefix. Pod volumes are cleaned up on sandbox removal.
- **Network**: `EnsureNetwork`, `RemoveNetworks` on the `Backend` interface. `NetworkType` enum (`bridge`, `host`, `none`) in `pkg/cri/types/`. Bridge creates a dedicated `<cluster>-bridge` network by cloning the built-in bridge config. Host and none are no-ops. Non-host-network sandboxes lazily ensure bridge on `RunPodSandbox`.
- **Cleanup**: Centralized `Cleanup()` in `pkg/cri/backend.go` — calls `RemoveContainers` → `RemovePodSandboxes` → `RemoveVolumes` → `RemoveNetworks` with 30s timeout. Invoked via `ctx.Done`. Volume cleanup should be conditional based on the PV reclaim policy (Retain vs Delete) — currently all volumes are removed unconditionally.

## Logging

Structured logging via `zerolog` with component-scoped loggers. All packages use `component.NewLogger("name")` which delegates to a shared root logger at call time. `component.Setup()` handles bootstrap: flag parsing, log level, data dir clean, and log file mirroring (console + disk). Verbosity: default=info, `-v`=debug, `-vv`=trace.

## Testing

- **Status**: Not started
- **Approach**: Replace ad-hoc shell-based smoke tests (`tests/pods/`) with a Go integration test suite using `k8s.io/client-go` and the real nanokube control plane. Tests start nanokube in-process, apply resources via the API, and assert on pod status, volume contents (via `docker exec`), and container state.
- **Scope**: Volume types, pod lifecycle, CRI conformance regression, cleanup behavior.

## Podman Backend

- **Status**: Not started
- **Location**: `pkg/cri/podman/`
- **Approach**: Use `github.com/containers/podman` Go bindings (not the Docker-compat shim) to implement the `cri.Backend` interface. Separate TDD cycle against critest with `--runtime-endpoint` pointed at a Podman-backed CRI socket.
