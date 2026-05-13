# Patches deferred during the 1.35 → 1.36 cutover

These hunks existed in `kubernetes-1.35.patch` but were intentionally not carried into `kubernetes-1.36.patch`. See `kubernetes-1.35.patch` for the original diffs.

## Upstream candidates

Patches in `kubernetes-1.36.patch` that could plausibly be pitched as fixes/features to kubernetes/kubernetes itself instead of carried as a fork delta. Worth filing KEPs / issues for these so we can eventually drop them:

- **`pkg/kubelet/client/kubelet_client.go` — honor pre-formed `host:port` in the node's preferred address.** Currently the apiserver-side kubelet client always appends `DaemonEndpoints.KubeletEndpoint.Port` to the chosen node address, producing nonsense like `fqdn:443:10250` when an address already carries a port. Treating `net.SplitHostPort`-parseable addresses as authoritative is arguably a bug fix, not a feature. Small, defensible, no behavior change for stock nodes (which never embed ports).

- **`pkg/registry/core/pod/rest/subresources.go` — `UseLocationHost = true` on the apiserver's upgrade-aware proxy for pod subresources.** Today the apiserver forwards the *original* `Host` header when proxying exec/attach/logs/port-forward to the kubelet. Setting `UseLocationHost = true` uses the destination URL's host instead, which is what most reverse proxies do by default. Has independent value: prevents Host-header loops (our case) and matches the behavior of the same flag elsewhere in the codebase. Could be straight-up correctness or gated behind a kubelet client config knob.

- **`pkg/kubelet/kubelet.go` — soften oomWatcher creation failure outside UserNS.** Upstream already has an `inuserns.RunningInUserNS()` branch that logs and continues. Extending that to the non-UserNS path (i.e., when `/dev/kmsg` simply isn't available — macOS dev, BSDs, sandboxed environments) keeps the kubelet running with `oomWatcher = nil`. The whole pod-OOM eviction path already handles a nil watcher. Small, low-risk, fixes "kubelet won't start on macOS for local dev" complaints.

- **`staging/src/k8s.io/apiserver/pkg/endpoints/handlers/responsewriters/writers.go` + `…/registry/generic/rest/streamer.go` — propagate backend HTTP status code through `LocationStreamer`.** Today the apiserver swallows the backend's status and always writes the caller-supplied code (usually 200). Recording the backend's `resp.StatusCode` on the streamer and forwarding it from `StreamObject` is general-purpose: anywhere the apiserver proxies a streaming response, the upstream status survives. Useful beyond our Cloudflare-flush trick — would let backends signal 206 Partial Content, 304 Not Modified, etc. through the apiserver edge.

- **`pkg/kubelet/server/server.go` — return `206 Partial Content` from `getContainerLogs`.** Semantically `kubectl logs` (especially `-f`) is an open-ended streaming response whose body is, by definition, partial — 206 is the more accurate code than 200. It's also the widely-used escape hatch that signals "this is a streaming body, don't buffer it" to reverse proxies, CDNs, and load balancers (not just Cloudflare). Reasonable framing for upstream: a) more semantically correct for `Follow: true`, b) interoperates with the streamer-status-propagation patch above so the 206 reaches `kubectl`, c) low risk — all known kubectl/client-go consumers handle a streaming 206 identically to a streaming 200.

- **`staging/src/k8s.io/apiserver/pkg/endpoints/handlers/get.go` — cap watch request timeouts, exposed as a `--max-request-timeout` apiserver flag.** Today the apiserver has `--min-request-timeout` (a floor for List/Watch timeouts) but no symmetric ceiling — a watch can sit open for hundreds of seconds. Anyone running the apiserver behind an LB or CDN with an idle timeout (AWS ALB, GCP HTTP(S) LB, Cloudflare, on-prem F5/HAProxy, ingress controllers with finite request budgets) hits the same problem: the intermediary closes the connection before the configured timeout, the client sees an unclean EOF, and watch loops have to deal with it. A `--max-request-timeout` flag (default `0` = no cap, opt-in) lets the operator cap watches to something below the intermediary's idle timeout so the apiserver closes cleanly and the client reconnects via the normal watch-resumption path. Our hardcoded 5s becomes the configured value.

- **`pkg/controlplane/controller/kubernetesservice/controller.go` — gate the default `kubernetes` Service bootstrap behind an opt-out flag.** Today the apiserver unconditionally creates the `kubernetes` Service in the `default` namespace and reconciles its endpoints to its own pod IPs. For control planes that aren't reachable through a "normal" Service abstraction — hosted (HyperShift, Cluster API providers, vCluster), edge / single-binary distros (nanokube, k3s on weird networks), air-gapped, anything fronted by an external proxy/tunnel — that Service is at best useless and at worst confusing (its endpoints point at unreachable IPs). Upstream already exposes `--endpoint-reconciler-type=none` to skip endpoint reconciliation but still creates the Service itself. Pitch: extend that enum (or add a sibling like `--kubernetes-default-service={enabled,disabled}`) so the whole controller — Service creation and endpoint reconciliation — can be opted out together. Our current Start/Stop no-op becomes the `disabled` codepath. Defaults stay `enabled` for zero-friction upgrade.

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
