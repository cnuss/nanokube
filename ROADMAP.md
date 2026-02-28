# Roadmap

> Forward-looking only. Move completed items to CLAUDE.md or remove them.

## FailedScheduling Taint Race

Every pod gets ~2 `FailedScheduling` warnings before being scheduled ("no nodes available" → "untolerated taints"). Node taint is removed too late relative to first scheduling attempt.

## HollowKubelet: Replace Remaining Stubs

The kubelet uses `kubemark.NewHollowKubelet()` with fake/stub dependencies in `pkg/kubernetes/kubelet/`.

| Stub | Interface | Status |
|------|-----------|--------|
| `mounter` | `mount.Interface` | Partial — tmpfs only, other fstypes return `errNotImplemented` |
| `ScopedHostUtil` | `hostutil.HostUtils` | Partial — SELinux/ownership stubs |
| `ScopedSubpath` | `subpath.Interface` | Partial — `CleanSubPaths` and `PrepareSafeSubpath` are no-ops |

## Pod Volume Types

| # | Volume Type | Notes |
|---|-------------|-------|
| 1 | `CSI` | Ephemeral CSI volumes. Requires CSI node plugin infrastructure. |
| 2 | `Ephemeral` | Cluster-driver ephemeral volumes. Built on CSI. |
| 3 | `NFS` | Needs real `ScopedMounter.Mount` with NFS support. |
| 4 | `ISCSI` | Needs host-level iSCSI tooling. |
| 5 | `FC` | Fibre Channel. Datacenter-only. |

## Sonobuoy Conformance

Run the upstream Kubernetes `[Conformance]` e2e suite via Sonobuoy against a running nanokube cluster (like k0s does with `--mode=certified-conformance`).

1. Add `make conformance` target
2. Start with `--e2e-focus='\[sig-network\].*\[Conformance\]'` to establish baseline
3. Expand to full `--mode=certified-conformance` and track pass rate

## Probes as Docker Healthchecks

Map Kubernetes liveness/readiness/startup probes to Docker `HEALTHCHECK` configs on container creation.

## Podman Backend

`pkg/cri/podman/` — implement `cri.Backend` using `github.com/containers/podman` Go bindings. TDD against critest.
