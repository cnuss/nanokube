# nanokube

A single-binary Kubernetes distribution that runs etcd, kube-apiserver, kube-controller-manager, kube-scheduler, and kubelet in one process.

## Quick Start

```bash
git clone --recurse-submodules https://github.com/cnuss/nanokube.git
cd nanokube
make build
./nanokube --clean
```

Requires Go 1.25+ and a container runtime (Docker recommended).

## Usage

```
nanokube [flags]
```

| Flag             | Default      | Description                                      |
|------------------|--------------|--------------------------------------------------|
| `--name`         | `nanokube`   | Cluster name                                     |
| `--data`         | `~/.nanokube`| Data directory                                   |
| `--clean`        | `false`      | Clean data directory before starting             |
| `--kubelet`      | `true`       | Start the kubelet component                      |
| `-v, --verbose`  | `false`      | Enable debug logging (`-v` debug, `-vv` trace)   |

## Container Runtimes

nanokube includes its own CRI (Container Runtime Interface) daemon that bridges Kubernetes to container runtimes directly — no external CRI shim required.

| Runtime | Status    |
|---------|-----------|
| Docker  | Supported |
| Podman  | Planned   |

Docker is auto-detected by probing common socket paths (`/var/run/docker.sock`, `~/.colima/default/docker.sock`, `~/.rd/docker.sock`, etc.). If no runtime is found, nanokube starts with a no-op backend.

## Networking

nanokube uses Docker's networking directly — there is no kube-proxy or CNI plugin.

**Shared bridge (default)** — Pods without `hostPort` all run on Docker's default `bridge` network. Containers can listen on any port without conflict since no host port binding occurs. `containerPort` in a pod spec is treated as informational only, matching upstream Kubernetes semantics.

**Dedicated network (hostPort)** — When a pod specifies `hostPort`, nanokube creates a per-sandbox Docker network with explicit port publishing. This isolates the host port binding to that sandbox, preventing conflicts with other pods on the shared bridge.

**DNS** — nanokube runs an embedded DNS server that resolves pod and service names. For bridge-mode pods, Docker's built-in DNS (`127.0.0.11`) forwards to the CRI-configured DNS servers. For host-network pods, resolv.conf is written directly.

## Build

```bash
make submodules   # init git submodules (etcd, kubernetes)
make build        # apply patches + build binary
make run          # fmt, build, and run
make run-clean    # fmt, build, and run with --clean
```

## Testing

```bash
make test                           # unit tests
make e2e                            # kuttl e2e suite
make e2e SUITE=pods                 # single suite
make e2e SUITE=pods WHAT=dns-config # single test in a suite
make e2e-baseline                   # e2e suite against Kind (reference baseline)
make critest                        # CRI conformance tests
make critest WHAT="port mapping"    # single critest by focus
```

## Conformance

E2E tests from [kuttl-conformance](https://github.com/cnuss/kuttl-conformance) run against nanokube with Docker.

| Suite             | Test                     | Status         |
|-------------------|--------------------------|----------------|
| **api**           | crd-lifecycle            | :green_circle: |
|                   | finalizers               | :green_circle: |
|                   | labels-annotations       | :green_circle: |
|                   | namespace-lifecycle      | :green_circle: |
| **auth**          | serviceaccount-token     | :green_circle: |
|                   | clusterrole-binding      | :red_circle:   |
|                   | rbac                     | :red_circle:   |
| **configuration** | configmap-mount          | :green_circle: |
|                   | hostpath-volume          | :green_circle: |
|                   | persistent-volume-claim  | :green_circle: |
|                   | secret-mount             | :green_circle: |
|                   | volume-submounts         | :green_circle: |
|                   | volume-types             | :green_circle: |
| **networking**    | ingress                  | :green_circle: |
|                   | endpoint-slices          | :red_circle:   |
|                   | headless-service         | :red_circle:   |
|                   | network-policy           | :red_circle:   |
|                   | service-clusterip        | :red_circle:   |
|                   | service-nodeport         | :red_circle:   |
| **observability** | pod-events               | :green_circle: |
|                   | pod-logs                 | :green_circle: |
| **pods**          | dns-config               | :green_circle: |
|                   | init-containers          | :green_circle: |
|                   | pod-lifecycle            | :green_circle: |
|                   | pod-lifecycle-hooks      | :green_circle: |
|                   | resource-requests-limits | :green_circle: |
|                   | restart-policy           | :green_circle: |
|                   | pod-probes               | :green_circle: |
|                   | security-context         | :red_circle:   |
| **policy**        | horizontal-pod-autoscaler| :green_circle: |
|                   | limit-range              | :green_circle: |
|                   | resource-quota           | :green_circle: |
|                   | pod-disruption-budget    | :red_circle:   |
| **scheduling**    | pod-affinity             | :green_circle: |
|                   | priority-class           | :green_circle: |
|                   | taints-tolerations       | :green_circle: |
|                   | topology-spread          | :green_circle: |
|                   | node-affinity            | :red_circle:   |
|                   | node-selector            | :red_circle:   |
| **workloads**     | cronjob-scheduling       | :green_circle: |
|                   | daemonset                | :green_circle: |
|                   | job-completion           | :green_circle: |
|                   | deployment-lifecycle     | :red_circle:   |
|                   | deployment-strategy      | :red_circle:   |
|                   | replicaset               | :red_circle:   |
|                   | statefulset              | :red_circle:   |

**30/46 passing** — remaining failures are due to missing ClusterIP service networking, SecurityContext pass-through, and node label requirements.

## License

[Apache 2.0](LICENSE)
