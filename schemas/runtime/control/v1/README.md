# Local runtime-control consumer contract

These files are copied byte-for-byte from
[Lanyte Crucible v0.2.0](https://github.com/lanytehq/crucible/tree/71f77894b7ac5d6065f20ecbbbaaa936409128f6/schemas/ipc/local).
`manifest.json` records the full upstream commit, original paths and SHA-256
digests. The family, selected schemas, root policy, semantics and fixture corpus
retain their upstream bytes. Fixture manifest paths are upstream-relative; tests
remove the `schemas/ipc/local/` prefix to resolve the vendored copies.

`chanvoy_core::runtime::control` embeds the nine selected schemas and uses the
locked JSON Schema 2020-12 validator. Default remote/file retrieval features are
disabled, and a refusing retriever also prevents retrieval through feature
unification. The consumer never resolves a floating tag or fetches a schema.

The caller selects `ControlRoot` from its transport. `ControlDocument::decode`
checks the byte limit before UTF-8 and recursive strict JSON parsing, rejects
duplicate decoded keys at every depth and trailing values, and validates the
explicit selected root. Numeric tokens are checked with decimal arithmetic before
JSON conversion: the pinned closed contract permits only nonnegative integers
bounded by `u64::MAX`. Exact integer spellings such as `1.0`, `1e0` and negative
zero remain accepted where the schema allows their value. Fractional and
out-of-range values cannot round into valid counts, identities or revisions.
Accepted numbers are stored and encoded as exact integer values; original number
spellings are not retained. The returned object is immutable. Debug and error output
do not expose the body. `correlate` checks the request/response method pair and
exact non-null request id; it does not establish target identity or native facts.

`read_rpc` consumes one LF-delimited frame with the delimiter outside the 16 KiB
budget and preserves buffered subsequent frames. It consumes at most cap+1 bytes.
Any read, framing, parsing or validation failure must end that exchange; callers
must close the transport rather than interpret an oversized frame's remaining
suffix as another request. Callers supply deadlines. Non-RPC roots use
`read_document`, which reads through EOF with a cap+1 limit. Bootstrap uses 32 KiB;
readiness and local reports use 16 KiB. The reader accepts an already-authorized
source and does not replace credential-tier file checks or native binding.

This module is a consumer foundation. It registers no RPC, changes no legacy
bootstrap format and activates no lifecycle behavior. Native identity, epoch and
revision binding, sticky retirement facts, admission closure, lease transfer and
process/listener cleanup remain integration obligations. A validated application
claim is never proof of native death or permission to retire work.

The core tests validate all 100 upstream fixture expectations through the actual
decoder and readers, verify vendored digests, and exercise strict parsing,
transport selection, correlation, framing, byte boundaries and unavailable
external references. Run them with the managed build cache:

```sh
python3 scripts/build-campaign.py run -- cargo test --locked -p chanvoy-core runtime::control
```

The vendored contracts and documentation are CC0-1.0, as recorded in the manifest.
