# Roadmap

## Log Noise Reduction

**Baseline: e2e run 2026-02-28 — 10 tests, 1143 lines, all PASS**

Total noise: 553/1143 lines (48.4%). Fixing P0 items drops to ~112 lines (9.8%).

### P0 — High volume (>50 per run)

| Pattern | Count | Source | Fix |
|---------|-------|--------|-----|
| TracerProvider "Start not implemented" | 379 (33%) | `pkg/stub/` tracer | No-op span provider or drop to trace level |
| etcd startup gRPC race | 62 (5%) | `logging.go:55` | Add etcd readiness gate before starting apiserver |

### P1 — Moderate (5-50 per run)

| Pattern | Count | Source | Actionable? |
|---------|-------|--------|-------------|
| Skipped API groups | 34 | `genericapiserver.go:787` | No — upstream behavior |
| Namespace deletion race | 24 | `namespace_controller.go:164` | No — inherent to 1s PLEG poll |
| CleanSubPaths | 18 | `pkg/stub/` subpath | Maybe — drop to debug |
| WebSocket close | 15 | `conn.go:339` | No — upstream exec teardown |

### P2 — One-off / low priority

PLEG startup (4), PVC PostFilter (3), server rejected event (2), /proc not found (2), node lease race (2), service CIDR deprecation (1), MakeRShared (1), flexvolume probe (1), swap_util (1), SetRLimit (1), plugin prober (1), file watcher (1), stale endpoints (1). All expected or macOS-specific.

### Recently Resolved

- ~~EventedPLEG / GetContainerEvents~~: disabled alpha feature gate, replaced Docker event stream with stub
- ~~MissingClusterDNS~~: `Nameservers()` probes via RunProbe; ClusterDNS configured
- ~~Custom bridge network~~: removed nanokube-bridge; pods use Docker default bridge
- ~~Server rejected events~~: dropped from 56 to 2 (96% reduction) after ClusterDNS fix
- ~~EventRecorder~~: real event recording implemented

## HollowKubelet: Replace Fakes with Real Implementations

The kubelet uses `kubemark.NewHollowKubelet()` which injects several fake/stub dependencies. Kubelet deps live in `pkg/kubernetes/kubelet/`.

### OS-Level Stubs

| Stub | Interface | Status |
|------|-----------|--------|
| `mounter` | `mount.Interface` | Partial — tmpfs mounts tracked via `sync.Map`. Other fstypes return `errNotImplemented`. |
| `ScopedHostUtil` | `hostutil.HostUtils` | Partial — `PathExists`, `GetFileType`, `EvalHostSymlinks` are real. SELinux/ownership stubs. |
| `ScopedSubpath` | `subpath.Interface` | Partial — `SafeMakeDir` is real. `CleanSubPaths` and `PrepareSafeSubpath` are no-ops. |
| `TracerProvider` | `trace.TracerProvider` | No-op with warn logging. **P0 noise source (379 warns/run).** |

### OS-Level — Real Implementations

| Component | Notes |
|-----------|-------|
| `ScopedOS` | Real scoped implementation — remaps paths outside DataDir. `Hostname()` delegates to CRI backend. |
| `FakeOOMAdjuster` | Kernel-level `/proc` writes. Docker handles container OOM via container config. |

## Pod Volume Types

Non-deprecated volume types not yet supported, stack-ranked by impact:

| # | Volume Type | Notes |
|---|-------------|-------|
| 1 | `CSI` | Ephemeral CSI volumes. Requires CSI node plugin infrastructure. |
| 2 | `Ephemeral` | Cluster-driver ephemeral volumes. Built on CSI. |
| 3 | `NFS` | Needs real `ScopedMounter.Mount` with NFS support. |
| 4 | `ISCSI` | Needs host-level iSCSI tooling. |
| 5 | `FC` | Fibre Channel. Datacenter-only. |

## Sonobuoy Conformance

- **Status**: Not started
- **Goal**: Run the upstream Kubernetes `[Conformance]` e2e suite via Sonobuoy against a running nanokube cluster
- **Reference**: k0s uses `sonobuoy run --mode=certified-conformance --kubernetes-version=v1.35.1`
- **Steps**:
  1. Add `make conformance` target that starts nanokube and runs Sonobuoy
  2. Start with `--e2e-focus='\[sig-network\].*\[Conformance\]'` (network subset) to establish baseline
  3. Expand to full `--mode=certified-conformance` and track pass rate
  4. Document skip list for Docker Desktop / macOS-specific failures

## Probes as Docker Healthchecks

- **Status**: Not started
- **Approach**: Map Kubernetes liveness/readiness/startup probes to Docker `HEALTHCHECK` configs on container creation.

## Podman Backend

- **Status**: Not started
- **Location**: `pkg/cri/podman/`
- **Approach**: Use `github.com/containers/podman` Go bindings to implement the `cri.Backend` interface. Separate TDD cycle against critest.
