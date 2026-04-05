# REPOSITORY SAFETY PROTOCOLS

This repository defines runtime verification results consumed by downstream agent systems. Treat
it as API-sensitive.

## Never Commit

- secrets (API keys, tokens, credentials)
- private hostnames, internal URLs, or customer data
- test fixtures that contain real tokens or private file contents

## Core Constraints

- `lanyte-verify` remains standalone and must not depend on crates from `/Users/davethompson/dev/lanytehq/lanyte`.
- Built-in verifiers must stay cheap enough for per-tool runtime use.
- Changes to serialized result fields are downstream contract changes and require explicit review.
- `HttpVerifier` must remain optional behind the `http` feature.

## Required Reviews

- Pause for four-eyes review after the crate compiles with passing tests.
- Escalate to `entarch` or `cxotech` if the core verifier trait or result model changes materially.
