# Roadmap

> Forward-looking only. Move completed items to CLAUDE.md or remove them.

## Stubs & Unimplemented

| Struct                 | Method                      | Location                                 | Status                                                    |
| ---------------------- | --------------------------- | ---------------------------------------- | --------------------------------------------------------- |
| `mounter`              | non-tmpfs/bind fstypes      | `pkg/cri/volume_plugin.go`               | TODO — fall back to Docker volume drivers                 |
| `ScopedHostUtil`       | `DeviceOpened`              | `pkg/kubernetes/kubelet/hostutil.go`     | WONT DO — no block devices in Docker                      |
| `ScopedHostUtil`       | `PathIsDevice`              | `pkg/kubernetes/kubelet/hostutil.go`     | WONT DO — no block devices in Docker                      |
| `ScopedHostUtil`       | `MakeRShared`               | `pkg/kubernetes/kubelet/hostutil.go`     | WONT DO — mount propagation is kernel-level               |
| `ScopedHostUtil`       | `GetOwner`                  | `pkg/kubernetes/kubelet/hostutil.go`     | TODO                                                      |
| `ScopedHostUtil`       | `GetMode`                   | `pkg/kubernetes/kubelet/hostutil.go`     | TODO                                                      |
| `ScopedHostUtil`       | `GetSELinuxSupport`         | `pkg/kubernetes/kubelet/hostutil.go`     | WONT DO — no SELinux on macOS/Docker Desktop              |
| `ScopedHostUtil`       | `GetSELinuxMountContext`    | `pkg/kubernetes/kubelet/hostutil.go`     | WONT DO — no SELinux on macOS/Docker Desktop              |
| `ScopedSubpath`        | `CleanSubPaths`             | `pkg/kubernetes/kubelet/subpath.go`      | TODO                                                      |
| `ScopedSubpath`        | `PrepareSafeSubpath`        | `pkg/kubernetes/kubelet/subpath.go`      | TODO                                                      |
| `FakeOOMAdjuster`      | all                         | `kubemark`                               | WONT DO — kernel-level `/proc` writes, Docker handles OOM |
| `StubContainerManager` | all cgroup methods          | `pkg/kubernetes/kubelet.go`              | WONT DO — Docker manages cgroups                          |
| `Backend`              | `CheckpointContainer`       | `pkg/cri/docker/docker.go`               | WONT DO — CRIU not available on Docker Desktop            |
| `Backend`              | `GetContainerEvents`        | `pkg/cri/docker/docker.go`               | TODO — stream Docker events                               |
| `Backend`              | `ListMetricDescriptors`     | `pkg/cri/docker/docker.go`               | TODO                                                      |
| `Backend`              | `ListPodSandboxMetrics`     | `pkg/cri/docker/docker.go`               | TODO                                                      |
| `Backend`              | `UpdatePodSandboxResources` | `pkg/cri/docker/docker.go`               | TODO                                                      |
| `ProbeManager`         | gRPC probes                 | `pkg/kubernetes/kubelet/probemanager.go` | TODO                                                      |

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
