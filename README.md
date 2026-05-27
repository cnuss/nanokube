# nanokube

## Overview

Single-binary Kubernetes. Runs kube-apiserver, kube-controller-manager,
kube-scheduler, and kubelet in one process against an embedded storage backend.

Container runtimes are pluggable via CRI. Built-in backends: Docker, Podman,
AWS Lambda. Cluster ingress runs through a Cloudflare named tunnel — no kube-proxy
or CNI plugin.

Tracks upstream Kubernetes via a git submodule (`kubernetes/`) plus a small
patch set in `patches/`.

## Quick Start

```bash
git clone --recurse-submodules https://github.com/cnuss/nanokube.git
cd nanokube
make build
./nanokube start
```

Requires Go 1.25+ and a container runtime (Docker recommended).

## Usage

```
nanokube start              # boot all-in-one cluster
nanokube run <component>    # run a single upstream component (kube-apiserver, kubelet, etc.)
```

State lives in `~/.nanokube/` by default.

## Build

```bash
make init        # init submodules + apply patches
make build       # patch + build
make patch-save  # capture changes in kubernetes/ back into patches/
make clean       # wipe binary, data dir, docker state
```

## Testing

```bash
make test SUITE=smoke
```

Chainsaw suites live under `tests/`.

## License

[Apache 2.0](LICENSE)
