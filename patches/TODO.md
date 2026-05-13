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

## B) Verified active — restore when smoke covers `runAsUser`

- **`pkg/volume/util/atomic_writer.go`** — soften the `lchown` failure on FsUser from `Errorf + return err` to `Warningf + return nil`. Original motivation: bind-mounted Mac host filesystem can't chown from a Linux container during local dev.

  **Investigation result:** code path is still active in 1.36 (`w.chown` was renamed to `w.lchown` but the failure semantics are the same). nanokube's `pkg/nanokube/host.go:283-289` registers the upstream `secret`/`configmap`/`projected`/`downwardapi` plugins verbatim, so the chain `operation_generator → mounter.SetUp(MounterArgs{FsUser: FsUserFrom(pod)}) → collectData → AtomicWriter.writePayloadToDir → w.lchown` is fully reached. The branch only fires when `FsUserFrom(pod)` returns non-nil — i.e. every container in the pod sets `securityContext.runAsUser` to the *same* non-root UID. The current chainsaw-kube smoke pod doesn't set `runAsUser` anywhere, which is why smoke is green without this patch.

  **Restore plan:** re-apply the soften (a single 2-line change) once smoke actually exercises `runAsUser` on a pod with a projected/secret/configmap mount — see section C.

## C) Smoke coverage — exercise the surfaces these patches affect

The 1.36 patch set still includes the apiserver↔kubelet streaming workarounds (UseLocationHost on the upgrade-aware proxy in `subresources.go`, 206 Partial Content on log responses in `server.go`, backend-status forwarding via `LocationStreamer.StatusCode`, 5s watch-timeout cap). None of these are reached by the current smoke test, which just asserts a Deployment's `availableReplicas`/`readyReplicas`. A regression in any of them would land silently. Extend the smoke (or sanity) suite to actually exercise:

- `kubectl logs` — both `--tail` (one-shot) and `-f` (follow), to cover the kubelet 206 + apiserver status-forward + Cloudflare flush path
- `kubectl exec` — interactive `sh` + non-interactive `command` form, to cover the upgrade-aware proxy with `UseLocationHost = true`
- `kubectl attach` — same code path as exec but the attach branch
- `kubectl port-forward` — different upgrade-aware proxy invocation; covers PortForward
- `kubectl get pods -w` — watch through the Cloudflare-fronted apiserver to cover the 5s timeout cap
- A pod that sets `securityContext.runAsUser` (uniformly across containers) and mounts a projected/secret/configmap volume — exercises the `atomic_writer.lchown` path that section B parks a fix for, so the macOS chown failure surfaces in CI rather than silently breaking real workloads.

## D) Tabled — try v4 first

- **`cri-streaming/pkg/streaming/remotecommand/websocket.go`** (was `kubelet/pkg/cri/streaming/remotecommand/websocket.go` in 1.35) — add `v5BinaryWebsocketProtocol = "v5." + wsstream.ChannelWebSocketProtocol`, register it in `createWebSocketStreams`'s protocols map, and include it in the `v4WriteStatusFunc` switch alongside the v4 protocols. v5 is v4 with an additional stream-close signal. Skip until we confirm v4 alone breaks something.
