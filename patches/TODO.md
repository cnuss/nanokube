# Patches deferred during the 1.35 → 1.36 cutover

These hunks existed in `kubernetes-1.35.patch` but were intentionally not carried into `kubernetes-1.36.patch`. See `kubernetes-1.35.patch` for the original diffs.

## A) Services-injection chain (defer together)

Goal: let nanokube backends register additional `restful.WebService`s on the kubelet's HTTP server so per-backend routes inherit its auth/tracing/metrics filters.

Working around it today: `pkg/kube.go` has a `TODO(1.36)` where `Dependencies.Services` used to be wired. Backend WebServices are not currently mounted on the kubelet's REST mux.

Pieces to restore as a unit:
- **`pkg/kubelet/kubelet.go`** — add `Dependencies.Services []*restful.WebService`, stash it on `Kubelet.additionalServices` in `NewMainKubelet`, expose `AdditionalServices()`.
- **`pkg/kubelet/server/server.go`** (hunks 1+2 of the original) — in `ListenAndServeKubeletServer`, iterate `host.AdditionalServices()` and `handler.restfulCont.Add(ws)`; add `AdditionalServices()` to `HostInterface`.
- **`pkg/kubelet/server/server_test.go`** — stub `AdditionalServices()` on `fakeKubelet` to satisfy the modified interface.
- **`cri-streaming/pkg/streaming/server.go`** (was `kubelet/pkg/cri/streaming/server.go` in 1.35) — add `WebService() *restful.WebService` to the streaming `Server` interface; expose the WebService that `NewServer` constructs internally so callers can mount it on their own container.

Also undo the `TODO(1.36)` in `pkg/kube.go` and restore the `k.kubeletDependencies.Services = k.Kubelet().Services(tunnel.URL())` line.

## B) Parked — verify code path still active

- **`pkg/volume/util/atomic_writer.go`** — soften the `lchown` failure on FsUser from `Errorf + return err` to `Warningf + return nil`. Original motivation: bind-mounted Mac host filesystem can't chown from a Linux container during local dev. 1.36 renamed `w.chown` to `w.lchown`; before re-applying, check whether this branch still runs in the projected/secret/configmap volume mount path on macOS or whether 1.36 already short-circuits.

## C) Tabled — try v4 first

- **`cri-streaming/pkg/streaming/remotecommand/websocket.go`** (was `kubelet/pkg/cri/streaming/remotecommand/websocket.go` in 1.35) — add `v5BinaryWebsocketProtocol = "v5." + wsstream.ChannelWebSocketProtocol`, register it in `createWebSocketStreams`'s protocols map, and include it in the `v4WriteStatusFunc` switch alongside the v4 protocols. v5 is v4 with an additional stream-close signal. Skip until we confirm v4 alone breaks something.
