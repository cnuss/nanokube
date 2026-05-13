# nanokube actions

Reusable GitHub composite actions for [nanokube](https://github.com/cnuss/nanokube). This branch is rooted at the initial commit and exists only to host `.github/actions/*`, so callers can pull each action without checking out the full nanokube source.

## Usage

Reference any action from your workflow:

```yaml
- uses: cnuss/nanokube/.github/actions/<name>@github
```

Each action handles its own `actions/checkout` as its first step, so caller workflows don't need an explicit checkout to use them.

## Actions

### `build`

Builds nanokube for the current runner and uploads the binary plus a `build-meta-<runner>.json` artifact describing it.

| Input    | Required | Description |
|----------|----------|-------------|
| `runner` | yes      | Runner label (e.g. `ubuntu-24.04`). Used in the cache key and metadata filename. |

### `collect`

Downloads every `build-meta-*` artifact and aggregates them into a single runner → artifact JSON map.

| Output      | Description |
|-------------|-------------|
| `artifacts` | JSON map of `runner -> artifact name`. |

### `setup`

Provisions a container runtime on the runner (native Docker on Linux, Colima on macOS, with a QEMU/TCG fallback when hardware virtualization isn't exposed).

| Input     | Default  | Description |
|-----------|----------|-------------|
| `runtime` | `docker` | Container runtime. Only `docker` is supported today. |

### `test`

Downloads a built nanokube artifact, starts it via `./nanokube start`, runs a Chainsaw suite from `tests/<suite>/`, then stops nanokube and uploads logs.

| Input           | Required | Description |
|-----------------|----------|-------------|
| `suite`         | yes      | Chainsaw test suite under `tests/` (e.g. `smoke`). |
| `artifact`      | yes      | Name of the build artifact to download. |
| `log-name`      | yes      | Name for the uploaded log artifact. |
| `chainsaw-args` | no       | Extra arguments forwarded to chainsaw (e.g. `--assert-timeout=1h`). |
