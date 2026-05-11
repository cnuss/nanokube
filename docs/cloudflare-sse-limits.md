# Cloudflare Tunnel: Streaming Responses & SSE Limits

Notes on exposing the Kubernetes API through `cloudflared` and why `kubectl`
watch operations hang on quick tunnels but not named ones.

## The symptom

Through a `*.trycloudflare.com` quick tunnel, watch-based `kubectl` operations
hang indefinitely:

- `kubectl delete <resource>` (default `--wait=true`)
- `kubectl get -w`, `kubectl wait`, `kubectl rollout status`
- any other client-go watch using `sendInitialEvents=true`

The client-go reflector emits `"Warning: event bookmark expired"` every 10s
because **zero bytes** of the watch stream are delivered.

Through a named tunnel on your own Cloudflare zone, the same operations
complete normally — the watch streams chunk-by-chunk.

## Diagnosis (curl, isolating each hop)

| Path | Watch behavior |
|---|---|
| Direct `https://127.0.0.1:32771/api/...?watch=true` | `ADDED` + `BOOKMARK` immediately |
| Through `*.trycloudflare.com` | open connection, 0 bytes in 10s+ |
| Through a named tunnel on a user zone | `ADDED` + `BOOKMARK` immediately |
| Through `*.trycloudflare.com` with `--http1.1` instead of HTTP/2 | still 0 bytes |
| Through `*.trycloudflare.com` → `kubectl proxy` HTTP origin | still 0 bytes |

So the buffering is at the Cloudflare *edge-to-client* hop, independent of HTTP
version and origin protocol (HTTPS direct vs HTTP via `kubectl proxy`).

## Root cause

Cloudflare applies hardcoded "guardrails" on the `trycloudflare.com` zone (which
they own, not your zone) that buffer streaming HTTP responses.

Quote from Cloudflare staff (`jcsf`) on
[cloudflared#1449](https://github.com/cloudflare/cloudflared/issues/1449):

> It works as expected with named tunnels. The reason it doesn't work with
> quick-tunnels is because we have some guardrails due to it being a demo
> product. Therefore... any user should just use named tunnels which is what is
> expected because quick-tunnels are only meant for demo purposes.

Official docs back this up
([Quick Tunnels limitations](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/do-more-with-tunnels/trycloudflare/)):

- "Quick Tunnels do not support Server-Sent Events (SSE)."
- "Currently, this limit is 200 in-flight requests."

For named tunnels there was historically a buffering default that opted out
on `Content-Type: text/event-stream`. A 2025 cloudflared fix improved streaming
detection so chunked `application/json` (what the kube API returns for watches)
streams without requiring SSE content-type. From `jcsf` on
[cloudflared#1095](https://github.com/cloudflare/cloudflared/issues/1095):

> We have made some improves to HTTP traffic that were meant to fix streaming
> when the right headers where not provided. Therefore, this should be working
> now for everyone.

## What we tried looking for (and didn't find)

A user-configurable knob to either:

1. **Disable** the quick-tunnel buffering (so quick tunnels stream like named),
   or
2. **Enable** equivalent buffering on a named tunnel (so we can reproduce the
   bug behind a stable URL).

Neither exists at any plan tier we can access:

- **Cloudflare zone setting `response_buffering`** exists but is **Enterprise
  only**. The full-buffer toggle is gated behind a contract.
- **Configuration Rule `response_body_buffering`** ([Jan 2026 changelog](https://developers.cloudflare.com/changelog/2026-01-27-body-buffering-settings/))
  is on all plans, but exposes only two modes: `Standard` (default — inspection
  prefix only, doesn't block streaming) and `None`. **No `Full` mode for
  responses.** Verified empirically in the dashboard: `Rules → Configuration
  Rules → +Add → Response Body Buffering → dropdown` shows exactly two options.
  (`Request Body Buffering` does have `Full` — only responses are limited.)
- **`cf` CLI / Cloudflare API schema search**: `grep -i buffer` against
  `cf schema --list` returns zero hits in zone settings. No knob exposed.
- **cloudflared origin parameters**: `disableChunkedEncoding`, `http2Origin`,
  etc. — these affect cloudflared↔origin, not edge↔client. No buffering knob.
- **Tunnel config / `originRequest`**: same — no buffering controls.

## Recommendations

For nanokube's case (exposing the kube API):

1. **Use a named tunnel** on a user zone. Confirmed working for `kubectl`
   watches end-to-end.
2. **Skip quick tunnels** for anything beyond ad-hoc demos that don't watch.
3. If a quick tunnel is unavoidable, `kubectl delete --wait=false` is the only
   reliable workaround — it skips the watch.

## Gotchas during setup

- Starting a quick tunnel (`cloudflared tunnel --url ...`) on a machine that
  also has `~/.cloudflared/config.yml` for a named tunnel will pick up that
  config and the trycloudflare URL will 404 at the edge before reaching
  cloudflared. Pass `--config /dev/null` when starting an ad-hoc quick tunnel
  to avoid this.
- macOS `cloudflared` doesn't reload config on SIGHUP — it terminates.
  Restart the process to apply config changes.
- For named tunnels, Cloudflare presents a valid cert for the user zone, so
  the kubeconfig context should **not** need `insecure-skip-tls-verify: true`.
  (Quick tunnels do need it, since the origin's cert is for the minikube IP,
  not the trycloudflare host.)
