# `keys/` — chanvoy release-signing public material

This directory holds the public-key fingerprint contract that
`scripts/verify-public-keys.sh` asserts against. The full release-
operation flow lives in [`/RELEASE_CHECKLIST.md`](../RELEASE_CHECKLIST.md).

## What's checked in

| File | Purpose |
|---|---|
| `expected-fingerprints.txt` | Trust contract — expected fingerprints for `chanvoy-minisign.pub` (minisign) and `chanvoy-release-signing-key.asc` (GPG). Falsifiable: a release-cycle whose exported public keys don't match these values fails `make release-verify-keys` |
| `README.md` | This file |

## What's NOT checked in

- **Private signing keys** — these are in Dave's local environment only, never in this repo or in CI. See PER-030 for the manual-signing baseline.
- **Public-key files** (`chanvoy-minisign.pub`, `chanvoy-release-signing-key.asc`) — exported into the release working directory by `scripts/export-release-keys.sh` at release time. The fingerprint contract here lets external adopters re-verify the public-key files they pull from a release without needing to clone this repo.

## Updating fingerprints

After dispatch's keypair provisioning completes, replace the `TBD-*`
placeholders in `expected-fingerprints.txt` with the real fingerprints.
This is a single-file change and the only blocker between the current
checked-in scaffolding and a fully-runnable release cycle.

Format:

```
minisign  <10-byte hex key id from the minisign public key>
gpg       <40-hex-char OpenPGP fingerprint>
```

Lines starting with `#` are comments.
