# Security Policy

Chanvoy authenticates as a Mattermost bot, holds long-lived credentials,
writes posts on behalf of agents, and runs a local daemon that holds
per-channel cursor state. We take security issues seriously.

## Reporting a Vulnerability

**Please do not report security vulnerabilities via public GitHub issues.**

Instead, please report them privately to:

- **Email**: security@3leaps.net
- **Preferred contact**: @3leapsdave on GitHub (with "Security Issue" in the subject)

When reporting, please include:

- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Any suggested mitigation

We will acknowledge receipt within 48 hours and aim to provide a timeline for remediation.

## Supported Versions

We provide security updates for the latest stable release and the active
in-development release branch/PR line. Security fixes use private coordination
until disclosure is appropriate.

## Security-Sensitive Pull Requests

Do not put bot tokens, profile state, attention-state snapshots, live
Mattermost server URLs, or exploit details in public PR text, commit
messages, logs, fixtures, screenshots, or CI artifacts. See
[`REPOSITORY_SAFETY_PROTOCOLS.md`](./REPOSITORY_SAFETY_PROTOCOLS.md) for the
full never-commit list and permission contract.

For credential-handling, daemon-lifecycle, sandbox-bypass, and
identity-attribution changes:

- Run `make pr-final` before pushing a PR branch.
- Request normal correctness review (`devrev`).
- Request security review (`secrev`) before merge.
- Verify errors, logs, and CLI output do not disclose tokens, server
  URLs, or other live-deployment identifiers.

## Verifying Release Binaries

Chanvoy distributes signed binaries via GitHub Releases. To verify a
download, follow the published procedure in
[`RELEASE_CHECKLIST.md`](./RELEASE_CHECKLIST.md) at the repository root.

The release procedure publishes, alongside each release:

- The release binary for each supported platform
- A checksum file (SHA-256) over the binaries
- A signature file (minisign + PGP) over the checksum file
- The signing public keys with stable fingerprints

The checklist documents the exact verification commands and the
canonical signing-key fingerprints to compare against. Public-key
material attached to releases is the authoritative source; check the
fingerprints against the checked-in expected values rather than
trusting the uploaded key file alone.

## Trust Boundary

Chanvoy's runtime trust boundary is the local Unix account.
[`REPOSITORY_SAFETY_PROTOCOLS.md`](./REPOSITORY_SAFETY_PROTOCOLS.md)
documents the permission contract (`0700` dirs, `0600` files) and the
intentional decision not to protect against same-user attackers in
local-mode. Multi-user / multi-tenant deployments require the deferred
remote control plane; we do not accept patches that loosen the
permission contract or silently degrade trust posture inside local-mode.

## Security-Issue Classes

The following are considered chanvoy security issues. Please report
them via the channel above:

- **Token leaks in commits or logs.** Mattermost bot tokens, GitHub
  PATs, or other credential material appearing in repository content,
  CI artifacts, or runtime logs.
- **Permission-mask regressions.** Changes that loosen the `0700`
  directory or `0600` file contract on profile, attention-state,
  runtime-socket, or bootstrap-handoff files.
- **Sandbox-bypass class.** A chanvoy code path that silently degrades
  to a less-secure mode under sandbox restriction rather than failing
  loudly. Chanvoy's design is "fail visibly, let the operator
  escalate"; any path that bypasses that should be reported.
- **Identity-attribution bugs.** Cases where the daemon-side
  identity-drift gate fails to refuse network-backed RPCs after a
  token-rotation-to-different-bot situation, or any other path that
  could mis-attribute posts.
- **Bootstrap-handoff corruption.** Tampering or replay of the
  per-profile bootstrap-state file that bridges the parent CLI to the
  detached daemon. The handoff is one-shot and nonce-protected; any
  bypass is a security issue.

## Upstream Dependencies

Chanvoy depends on:

- [`reqwest`](https://crates.io/crates/reqwest) — HTTP client (Mattermost REST + WebSocket transport)
- [`tokio`](https://tokio.rs/) — async runtime
- [`ipcprims`](https://github.com/3leaps/ipcprims) — IPC primitives for the future Lanyte-core peer channel

We monitor these projects for security updates. The release procedure
runs cargo-audit + cargo-deny against the workspace before tagging;
known unmaintained-but-non-exploitable advisories are documented in
`deny.toml`.

## Disclosure Policy

We follow responsible disclosure:

- We work with reporters to understand and fix the issue.
- We publish patches as soon as reasonably possible.
- We credit reporters in the release notes (unless anonymity is requested).

Thank you for helping keep chanvoy and its users secure.
