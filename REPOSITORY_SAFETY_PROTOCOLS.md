# Repository Safety Protocols

This file defines the safety rules contributors and agents must
follow when working in this repository. It's chanvoy-specific and
written to be public-readable — anyone with read access to the
repository (lanytehq agents, operators in adopter orgs, future
open-source contributors) is the audience.

For agent-session conventions (warmup, attribution, branching
discipline), see [`AGENTS.md`](./AGENTS.md). For the runtime
permission model, see [`docs/architecture.md`](./docs/architecture.md).

---

## Never commit

- **Mattermost bot tokens.** Tokens go in env (`LANYTE_MM_TOKEN` or
  whatever `CHANVOY_TOKEN_ENV_NAME` points at), sourced from a
  per-role identity script kept outside this repo. Test fixtures
  must use synthetic tokens (`test-token-...` literals) only.
- **Profile state files.** Real profile JSON contains the bot
  username, server URL, and team binding for a live deployment.
  Test fixtures use synthetic profile names and synthetic server
  URLs (`http://localhost:<port>` against `wiremock`).
- **Attention-state snapshots from real deployments.** They contain
  the bot's complete read history (channel ids, post ids,
  timestamps). Synthetic-only in commits.
- **Live Mattermost server URLs or workspace identifiers.** Use
  obvious-placeholder hostnames (`mm.example.com`,
  `mattermost.test.invalid`) in docs and tests.
- **Diagnostic-harness output** from real deployments. The
  `scripts/per015-diag.sh` redactor strips token-shaped values to
  name + length, but real channel ids, team ids, post ids, and bot
  usernames remain. Diag output goes to a private path
  (`~/.cache/chanvoy-per015-diag/`) by default; share with
  supervisors out-of-band, never in commits.
- **Anything resembling a private hostname, internal URL, or
  customer-named workspace.** When in doubt, assume the diff is
  visible to anyone who can read this repo and ask before adding.

The default branch and feature branches are equally bound by these
rules — there is no "drafts will be cleaned later" carve-out.

---

## Permission contract (treat as a contract, not a hint)

The chanvoy daemon and CLI enforce a specific permission model on
the files and sockets they create. Changes to this model are
downstream contract changes and require explicit review.

| Surface | Mode | Purpose |
|---|---|---|
| Config dirs (`$CONFIG_ROOT/`, `profiles/`, `attention/`) | `0700` | Reduce accidental cross-user exposure. |
| Profile JSON files | `0600` | Same. |
| Attention state JSON files | `0600` | Same. |
| Runtime dir (`$RUNTIME_ROOT/chanvoy/`) | `0700` | Same. |
| UDS socket (parent dir mode applies) | parent `0700` | Same. |
| Pid files | `0600` | Same. |
| Bootstrap-state handoff file | `0600` | Same; contains a one-shot validated identity nonce. |

If you change any of these masks, update
[`docs/architecture.md` §Storage layout](./docs/architecture.md#storage-layout)
in the same PR. Any code path that creates a new file under
`$CONFIG_ROOT/` or `$RUNTIME_ROOT/` must use the matching mode.

---

## Trust boundary

Chanvoy assumes the local Unix account is the trust boundary.
This is **intentional and not a defect**.

- Chanvoy does not attempt to protect one process from another
  process running as the same Unix user.
- The permission masks above reduce *accidental* cross-process
  exposure (other users on the box, world-readable misconfigured
  backups, archive tooling that ignores ownership). They do not
  claim to defeat a local attacker.
- Multi-user / multi-tenant deployments require the deferred remote
  control plane with attested transport. There is no local-mode
  workaround that changes this property; PRs that try to add one
  will be rejected as a category mistake.

If you find yourself wanting to "tighten" the trust boundary inside
local mode (e.g., signed RPC envelopes between same-user CLI and
daemon, encrypted on-disk state files), the right channel is a
discussion in the brief stream rather than an inline PR.

---

## Downstream contract surfaces

Some chanvoy-internal changes ripple to downstream consumers (other
chanvoy crates, integration tests, and — when wired — the Lanyte
core peer contract over channel 260). These changes need explicit
review:

- **Public types in `chanvoy-core`** (`ResolvedChannel`,
  `ResolutionSource`, `TeamInfo`, `TeamChannels`, `MigrationOutcome`,
  `QuarantinedCursor`, `AttentionState`, `Channel`, `Message`,
  `SearchResult`, etc.). Adding fields is fine; removing or
  re-typing them is a breaking change.
- **Daemon RPC method names and shapes.** New methods are additive;
  renamed methods are breaking. The drift gate
  (`LOCAL_ONLY_METHODS` in `crates/chanvoy-daemon/src/lib.rs`)
  classifies every RPC; new RPCs must be classified explicitly.
- **CLI argument shape.** Renaming a flag, changing a default, or
  swapping a positional/optional is breaking for any script or agent
  that invokes chanvoy. Add the new shape; deprecate the old in a
  visible way; remove on a release boundary.
- **Profile capability classes.** The set of classes and the
  operations gated by each are an admin-policy contract for
  downstream operators. Changes need a `CHANGELOG.md` entry and a
  call-out in the operator guide.
- **The cursor-advance taxonomy.** Whether a verb is pure-read, a
  probe, or cursor-advancing is part of the contract loop scripts
  rely on. Changing a verb's classification is a breaking change
  even if the implementation is "more correct."
- **The bootstrap-state handoff file format.** Read by the detached
  daemon at startup; format changes need to handle the prior shape
  via `#[serde(default)]` or an explicit migration path.

The general rule: contributors should ask "does this change a
property that someone outside this PR's scope is relying on?" If
yes, it's a contract change.

---

## Sandbox-permission asks

Sandboxed agents that can't run chanvoy under their default
configuration may need to escalate to a supervisor or operator (see
[`docs/getting-started.md` §Sandboxed agents](./docs/getting-started.md#sandboxed-agents)
for the three-path decision tree). The supervisor decides whether
to grant access; chanvoy does not negotiate or enforce the policy
itself.

This means:

- Chanvoy code paths must not silently degrade to a less-secure mode
  to "work around" a sandbox restriction. Failing loudly with a
  clear diagnostic is the correct behavior; the operator escalates
  from there.
- New CLI flags or env vars that change the trust boundary (e.g.,
  loosening permission masks, accepting unsigned bootstrap state)
  are contract changes per the previous section. They need explicit
  review and probably a separate brief.

If you're tempted to add an `--allow-broad-permissions` flag to
unblock a sandbox use case, that's the moment to file a discussion
in the brief stream instead.

---

## Required reviews

- Pause for review after the crate compiles, all tests pass, and
  `make pr-final` is green. The merge gate is `make pr-final`.
- Escalate to a maintainer if any of the following apply:
  - You changed a public type in `chanvoy-core`.
  - You changed a daemon RPC method name, shape, or drift-gate
    classification.
  - You changed the permission contract above.
  - You changed the cursor-advance taxonomy of any verb.
  - You added a CLI flag or env var that affects the trust
    boundary.
  - You added a network call from anywhere other than the parent
    CLI's bootstrap path. (The detached daemon must not need
    network at startup; this is the sandbox-aware-bootstrap
    contract.)

If none of the above apply, the standard PR review by a designated
reviewer is sufficient.

---

## Reporting a security issue

Suspected security issues — token leaks in commits, permission-mask
regressions, sandbox bypasses, identity-attribution bugs — should
be reported to the repository maintainers privately first, not as
a public issue or PR. The chanvoy maintainers will acknowledge and
coordinate disclosure on a case-by-case basis.
