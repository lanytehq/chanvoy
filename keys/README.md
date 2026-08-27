# `keys/` — chanvoy release-signing public material

This directory holds the public-key fingerprint contract that
`scripts/verify-public-keys.sh` asserts against. The full release-
operation flow lives in [`/RELEASE_CHECKLIST.md`](../RELEASE_CHECKLIST.md).

## What's checked in

| File | Purpose |
|---|---|
| `expected-fingerprints.txt` | Trust contract — expected fingerprints for `chanvoy.pub` (minisign) and `chanvoy.gpg.asc` (GPG). Falsifiable: a release-cycle whose exported public keys don't match these values fails `make release-verify-keys` |
| `README.md` | This file |

## What's NOT checked in

- **Private signing keys** — these are in Dave's local environment only, never in this repo or in CI. See PER-030 for the manual-signing baseline.
- **Public-key files** (`chanvoy.pub`, `chanvoy.gpg.asc`) — exported into the release working directory by `scripts/export-release-keys.sh` at release time. The fingerprint contract here lets external adopters re-verify the public-key files they pull from a release without needing to clone this repo.

## Updating fingerprints

Fill `expected-fingerprints.txt` only with `decernor` **0.1.4+**
records — never by hand.

```bash
make insert-expected-fingerprints \
  MINISIGN_PUB=/path/to/chanvoy.pub \
  GPG_ASC=/path/to/chanvoy.gpg.asc
```

The inserter scans **explicit public files** (`--class public`).
GPG uses `--gpg-role primary` (exactly one primary record).
Minisign uses `minisign-public-blob-sha256-v1` (64 lowercase hex).
Both lines are written together; any failure leaves the dest unchanged.

`scripts/verify-public-keys.sh` recomputes the same records and
compares them to the checked-in file. TBD placeholders fail closed.

Format:

```
minisign  <64 lowercase hex, minisign-public-blob-sha256-v1>
gpg       <40 uppercase hex, OpenPGP primary>
```

Lines starting with `#` are comments.
