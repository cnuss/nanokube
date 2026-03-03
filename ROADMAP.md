# Roadmap

> Forward-looking only. Move completed items to CLAUDE.md or remove them.

## Stubs & Unimplemented

| Struct             | Method                            | Location                                      | Status                                                        |
| ------------------ | --------------------------------- | --------------------------------------------- | ------------------------------------------------------------- |
| `mounter`          | non-tmpfs/bind fstypes            | `pkg/cri/volume_plugin.go`                    | TODO (P2) — fall back to Docker volume drivers                |
| `ScopedHostUtil`   | `DeviceOpened`                    | `pkg/kubernetes/kubelet/hostutil.go`          | WONT DO — no block devices in Docker                          |
| `ScopedHostUtil`   | `PathIsDevice`                    | `pkg/kubernetes/kubelet/hostutil.go`          | WONT DO — no block devices in Docker                          |
| `ScopedHostUtil`   | `MakeRShared`                     | `pkg/kubernetes/kubelet/hostutil.go`          | WONT DO — mount propagation is kernel-level                   |
| `ScopedHostUtil`   | `GetOwner`                        | `pkg/kubernetes/kubelet/hostutil.go`          | TODO (P3) — needed for correct volume ownership               |
| `ScopedHostUtil`   | `GetMode`                         | `pkg/kubernetes/kubelet/hostutil.go`          | TODO (P3) — needed for correct volume permissions             |
| `ScopedHostUtil`   | `GetSELinuxSupport`               | `pkg/kubernetes/kubelet/hostutil.go`          | WONT DO — no SELinux on macOS/Docker Desktop                  |
| `ScopedHostUtil`   | `GetSELinuxMountContext`          | `pkg/kubernetes/kubelet/hostutil.go`          | WONT DO — no SELinux on macOS/Docker Desktop                  |
| `ScopedSubpath`    | `CleanSubPaths`                   | `pkg/kubernetes/kubelet/subpath.go`           | TODO (P1) — 19 calls/e2e run, cleanup of bind-mount subpaths |
| `ScopedSubpath`    | `PrepareSafeSubpath`              | `pkg/kubernetes/kubelet/subpath.go`           | TODO (P3) — needed for subPath volume mounts                  |
| `FakeOOMAdjuster`  | all                               | `kubemark`                                    | WONT DO — kernel-level `/proc` writes, Docker handles OOM     |
| `ContainerManager` | `GetNodeAllocatableReservation`   | `pkg/kubernetes/kubelet/container_manager.go` | TODO (P1) — 29 calls/e2e run, node reports full capacity      |
| `ContainerManager` | `GetDevicePluginResourceCapacity` | `pkg/kubernetes/kubelet/container_manager.go` | WONT DO — no device plugins (29 calls, safe as nil)           |
| `ContainerManager` | `PodMightNeedToUnprepareResources`| `pkg/kubernetes/kubelet/container_manager.go` | WONT DO — no DRA (16 calls, safe as false)                    |
| `ContainerManager` | `UpdateQOSCgroups`                | `pkg/kubernetes/kubelet/container_manager.go` | TODO (P2) — 22 calls/e2e run, cgroup QoS tiers               |
| `ContainerManager` | `UpdatePluginResources`           | `pkg/kubernetes/kubelet/container_manager.go` | WONT DO — no device plugins (11 calls)                        |
| `ContainerManager` | `UnprepareDynamicResources`       | `pkg/kubernetes/kubelet/container_manager.go` | WONT DO — no DRA (11 calls)                                   |
| `ContainerManager` | `PrepareDynamicResources`         | `pkg/kubernetes/kubelet/container_manager.go` | WONT DO — no DRA (11 calls)                                   |
| `ContainerManager` | `GetResources`                    | `pkg/kubernetes/kubelet/container_manager.go` | WONT DO — no device plugins (11 calls)                        |
| `ContainerManager` | `GetPodCgroupRoot`                | `pkg/kubernetes/kubelet/container_manager.go` | TODO (P3) — 1 call/e2e run, cgroup root path                 |
| `ContainerManager` | `GetPluginRegistrationHandlers`   | `pkg/kubernetes/kubelet/container_manager.go` | WONT DO — no device plugins                                   |
| `ContainerManager` | `GetNodeConfig`                   | `pkg/kubernetes/kubelet/container_manager.go` | TODO (P3) — called once at startup                            |
| `ContainerManager` | `GetHealthCheckers`               | `pkg/kubernetes/kubelet/container_manager.go` | TODO (P3) — called once at startup                            |
| `containerLifecycle`| `PreCreateContainer`             | `pkg/kubernetes/kubelet/container_manager.go` | TODO (P1) — 11 calls/e2e run, container setup hook            |
| `containerLifecycle`| `PreStartContainer`              | `pkg/kubernetes/kubelet/container_manager.go` | TODO (P1) — 11 calls/e2e run, container setup hook            |
| `podContainerManager`| `EnsureExists`                  | `pkg/kubernetes/kubelet/container_manager.go` | TODO (P2) — 11 calls/e2e run, ensure pod cgroup exists        |
| `podAdmitHandler`  | `Admit`                           | `pkg/kubernetes/kubelet/container_manager.go` | TODO (P2) — 11 calls/e2e run, always admits currently         |
| `Backend`          | `CheckpointContainer`             | `pkg/cri/docker/docker.go`                    | WONT DO — CRIU not available on Docker Desktop                |
| `Backend`          | `ListMetricDescriptors`           | `pkg/cri/docker/docker.go`                    | TODO (P3) — metrics/observability                             |
| `Backend`          | `ListPodSandboxMetrics`           | `pkg/cri/docker/docker.go`                    | TODO (P3) — metrics/observability                             |
| `Backend`          | `UpdatePodSandboxResources`       | `pkg/cri/docker/docker.go`                    | TODO (P3) — in-place pod resize                               |
| `ProbeManager`     | gRPC probes                       | `pkg/kubernetes/kubelet/probemanager.go`      | TODO (P2) — HTTP/TCP probes work, gRPC does not               |

## Startup Noise Reduction

| Issue                                   | Lines | Source                    | Fix                                                              |
| --------------------------------------- | ----- | ------------------------- | ---------------------------------------------------------------- |
| gRPC etcd connection spam               | ~54   | `logging.go:55`           | Delay apiserver/controller/scheduler start until etcd gRPC ready |
| Deprecated `--service-cluster-ip-range` | 1     | `options.go:369`          | Explicitly pass `--service-cluster-ip-range` to apiserver        |
| Flexvolume plugin dir permission        | 2     | `probe.go` / `plugins.go` | Set `--flex-volume-plugin-dir` under dataDir                     |
| Shutdown lease update race              | ~7    | `controller.go:251`       | Stop kubelet lease controller before stopping apiserver          |
| Static pod file watching                | 1     | `file_unsupported.go:29`  | Expected on macOS — no inotify, uses polling fallback            |

## Sonobuoy Conformance

Run the upstream Kubernetes `[Conformance]` e2e suite via Sonobuoy against a running nanokube cluster (like k0s does with `--mode=certified-conformance`).

1. Add `make conformance` target
2. Start with `--e2e-focus='\[sig-network\].*\[Conformance\]'` to establish baseline
3. Expand to full `--mode=certified-conformance` and track pass rate

## Probes as Docker Healthchecks

Map Kubernetes liveness/readiness/startup probes to Docker `HEALTHCHECK` configs on container creation.

## Podman Backend

`pkg/cri/podman/` — entire backend is stubbed (41 methods). Implement `cri.Backend` using `github.com/containers/podman` Go bindings. TDD against critest.
