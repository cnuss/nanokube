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

| # | Volume Type | Status | Notes |
|---|-------------|--------|-------|
| 1 | `CSI` | Not started | Ephemeral CSI volumes. Requires CSI node plugin infrastructure. |
| 2 | `Ephemeral` | Not started | Cluster-driver ephemeral volumes. Built on CSI. |
| 3 | `NFS` | Not started | Network filesystem. Needs real `ScopedMounter.Mount` with NFS support and host nfs-utils. |
| 4 | `ISCSI` | Not started | iSCSI disk mount. Needs host-level iSCSI tooling (iscsiadm). |
| 5 | `FC` | Not started | Fibre Channel mount. Needs host-level FC tooling. Datacenter-only. |

## ClusterDNS Configuration

- **Bug**: Kubelet warns `kubelet does not have ClusterDNS IP configured and cannot create Pod using "ClusterFirst" policy. Falling back to "Default" policy.` for every pod.
- **Status**: Not started
- **Fix**: Set `cfg.ClusterDNS` in `ApplyKubeletConfig` to the cluster DNS service IP (typically `10.96.0.10`). Optionally deploy CoreDNS as a static pod or in-process DNS server so ClusterFirst resolution actually works.

## Evented PLEG (GetContainerEvents)

- **Bug**: `GetContainerEvents` was a no-op, causing kubelet to fall back to generic PLEG polling (~1s intervals). This resulted in slow pod termination detection and namespace deletion failures.
- **Status**: In progress — Docker event stream implementation added (`client.Events()` → CRI `ContainerEventResponse`), `EventedPLEG` feature gate enabled. Needs validation.

## Probes as Docker Healthchecks

- **Status**: Not started
- **Approach**: Map Kubernetes liveness/readiness/startup probes to Docker `HEALTHCHECK` configs on container creation, instead of running them via CRI ExecSync. More native to Docker, avoids probe container overhead, and lets Docker manage probe lifecycle directly.

## Stub Implementations (from e2e WRN analysis)

Prioritized by noise/impact from `make e2e` runs:

| Priority | Stub | Warns/run | Notes |
|----------|------|-----------|-------|
| P1 | TracerProvider | 232 | Revert to trace level or true no-op. Kubelet instruments every CRI gRPC call with OpenTelemetry spans. |
| P1 | EventRecorder | 102 | Implement real event recording to API server, or drop to debug. Events like Started/Pulled/Created are useful for debugging. |
| P2 | CleanSubPaths | 17 | During pod teardown, one per volume. Harmless. |
| P2 | MakeRShared | 1 | Kubelet startup. Harmless on macOS. |

## Podman Backend

- **Status**: Not started
- **Location**: `pkg/cri/podman/`
- **Approach**: Use `github.com/containers/podman` Go bindings (not the Docker-compat shim) to implement the `cri.Backend` interface. Separate TDD cycle against critest with `--runtime-endpoint` pointed at a Podman-backed CRI socket.
