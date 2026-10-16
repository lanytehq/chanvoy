# Build storage

Ordinary development builds reuse a host cache across source worktrees. Use
`make check`, `make build`, `make test`, `make msrv`, or `make pr-final`.
These targets run through `scripts/build-campaign.py`; Python 3 and a Unix host
(Linux or macOS) are required. Rust toolchains and downloaded dependencies keep
their existing rustup/Cargo home locations.

Managed Cargo stages require a Git checkout to track build identity. Use a
source worktree for mutation experiments, or initialize and record an explicit
baseline for a private source copy before running the controls lane.

## Placement and ownership

The default cache root is `~/Library/Caches/chanvoy-cargo` on macOS and
`${XDG_CACHE_HOME:-~/.cache}/chanvoy-cargo` on Linux. Its `dev` directory is
shared by ordinary checkouts. `controls` is a separate reusable directory for
mutation experiments. The source checkout does not acquire a new `target/`.

One advisory kernel lock serializes both lanes for the complete campaign, not
just individual Cargo invocations. A competing campaign refuses immediately.
The lock descriptor is inherited by nested stages. Interrupts are forwarded to
the owned command, and the supervisor waits for that command before returning.
Do not remove `campaign.lock` to release a busy cache: the file may remain after
completion, while kernel ownership is released when its last descriptor closes.

The runner supplies `CARGO_TARGET_DIR`, development/test debug level `0`, and
`CARGO_INCREMENTAL=0` only to Cargo stages. These settings are not exported into
the calling shell or the separate production-tooling test process. A source
selector invalidates embedded build identity on worktree, commit, or dirty-state
changes; the build script still obtains the actual identity from Git.

Use the runner for a focused Cargo command:

```sh
python3 scripts/build-campaign.py run -- cargo test -p chanvoy-core
python3 scripts/build-campaign.py run --lane controls -- cargo test -p chanvoy-core
```

For a multi-stage campaign, hold the outer lease and use `step` for every Cargo
invocation, including artifact capture before leaving the campaign:

```sh
python3 scripts/build-campaign.py run --lane controls -- sh -eu -c '
  python3 scripts/build-campaign.py step -- cargo test -p chanvoy-core
  # Change the experimental source, run another managed step, restore it,
  # and record the restoration and results before this shell exits.
'
```

An orchestration program using `subprocess` must preserve the inherited
`CHANVOY_BUILD_CAMPAIGN_FD` with `pass_fds`, or a nested step refuses. Internal
`managed-*` Make targets are implementation details and require that descriptor.
Direct, unwrapped `cargo` and arbitrary commands inside `run` can bypass the
policy; this runner is not a sandbox. Use the public Make targets or managed
steps for all campaign compilation. Do not point an unrelated job at these
caches or launch background commands that outlive the campaign.

## Storage admission

Defaults are a combined **12 GiB** development/controls budget and **20 GiB**
minimum free space on the cache volume. The runner measures allocated bytes,
counts hard links once, and does not follow cache-content symlinks. It checks
before starting a campaign, before each Cargo stage, and after completion.
JSON diagnostics on stderr record placement, limits and observations. A failed
command keeps its exit code; a successful command that exceeds storage limits
returns a failure.

These are admission limits, not a filesystem quota. A single compiler stage can
grow past the budget before the next check, and other applications can consume
space concurrently. Post-stage refusal does not delete partial results. Existing
worktree targets are not included in the managed-cache budget and need separate
inventory; minimum free space still reflects their volume usage.

For a host with a different measured capacity, pass explicit limits and record
the choice with the campaign evidence:

```sh
make check BUILD_CAMPAIGN_ARGS='--budget-gib 12 --min-free-gib 20'
```

Disposable CI check jobs explicitly select a 4 GiB free-space floor; shared-host
defaults remain 20 GiB. Neither setting changes Rust test deadlines or parallelism.

`--cache-root /absolute/path` selects another permanent cache location, such as
a roomier volume. It establishes a different lock domain: all contributors on
the host must use the same location. Do not allocate a new root per worktree or
campaign to evade limits. Caller target selectors, incompatible profile overrides
and per-command Cargo configuration overrides are refused.

## Production builds and installation

Fresh production builds and qualification keep their existing separate local
`target/` and provenance rules. The producer and qualifier still reject shared
target/profile overrides and Cargo configuration files. This runner does not
relax or replace their receipts, and a development build is not a qualified
production artifact.

Run `make build-release` and `make install` outside a managed development
campaign. They verify Cargo resolves the expected local `target/` before
building, so installation cannot silently copy an older local binary after a
redirected build. The existing install reporting behavior is unchanged.

Before a production campaign, reserve room for its measured peak, designate one
owner, and account for its fresh target separately. The development cache budget
does not enforce a production budget. Keep at most one active fresh production
target per host by coordination until production admission has its own reviewed
integration. Avoid global `.cargo/config.toml` placement changes: those affect
production qualification and unrelated repositories.

## Retention and cleanup

A shared cache is mutable. Copy any binary needed for later review into its
designated evidence location, and record/verify its hash and source identity
while the campaign still owns the cache. Retain logs, source archives and
receipts separately from rebuildable compiler outputs.

`make clean` refuses automatic cleanup. Inspect cache ownership, active jobs,
required artifacts and the exact paths before a separately authorized cleanup.
Do not remove another campaign's target or use worktree deletion as an implicit
cache cleanup policy. Source worktrees can remain for review without retaining
full build trees. No cache or worktree is automatically deleted by this tooling.
