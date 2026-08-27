# chanvoy Release Checklist

Canonical step-by-step procedure for cutting a `vX.Y.Z` chanvoy release.
This file lives at the repo root and is **public-readable** so external
adopters can verify a downloaded binary against the stable key
fingerprints below without needing org access.

Release model: **manual signing** as the v0.2.2 baseline.
- CI ([`.github/workflows/release.yml`](.github/workflows/release.yml))
  produces a draft GitHub release on tag push.
- Dave runs `make release-download` → `release-sign` → `release-verify`
  → `release-upload` → `release-undraft` **locally**.
- Signing keys never touch CI.

GHA-automated signing using a separate automation-variant key is
deliberately deferred to a later release.

Canonical release sequence (top-to-bottom; each numbered section
below corresponds to one step):

```
make release-prep      → release-smoke → release-preflight
release-tag → release-tag-push → GHA produces draft
(first public release only) verify draft → visibility PRIVATE → PUBLIC
make release-download  → release-sign → release-verify
make release-upload    → release-undraft
release announcement   → operational announcement
```

---

## 1. Pre-release verification

- [ ] All feature PRs for this release are merged to `main`
- [ ] `main` CI is green
- [ ] Working tree is clean (`git status` empty)
- [ ] `VERSION` matches `Cargo.toml` workspace + crate versions
      (`make version-check`)
- [ ] Release notes exist at `docs/releases/vX.Y.Z.md`
      (see §11 — must inline fingerprints + verification
      commands OR hard-pointer to this file)
- [ ] `keys/expected-fingerprints.txt` contains both stable public-key
      fingerprints with no `TBD` values
- [ ] All planned briefs for this release are at "done" status

### Initialize or rotate the fingerprint contract

Run this only when establishing or rotating the release-signing keyset.
Start from the repository root. The host release environment must provide
`CHANVOY_MINISIGN_PUB`, `CHANVOY_PGP_KEY_ID`, and, when the public GPG
key is in a dedicated keyring, `CHANVOY_GPG_HOMEDIR`. Host-specific
profile paths stay outside this repository.

```bash
(
  set -euo pipefail
  cd /path/to/chanvoy
  : "${CHANVOY_MINISIGN_PUB:?set CHANVOY_MINISIGN_PUB}"
  : "${CHANVOY_PGP_KEY_ID:?set CHANVOY_PGP_KEY_ID}"
  if [ -n "$(git status --porcelain=v1)" ]; then
    echo "error: fingerprint update requires a clean working tree" >&2
    git status --short >&2
    exit 1
  fi
  public_key_dir="$(mktemp -d)"

  make release-export-keys RELEASE_DIR="$public_key_dir"
  make insert-expected-fingerprints \
    MINISIGN_PUB="$public_key_dir/chanvoy.pub" \
    GPG_ASC="$public_key_dir/chanvoy.gpg.asc"
  make release-verify-keys RELEASE_DIR="$public_key_dir"

  fingerprint_git_status="$(git status --porcelain=v1)"
  case "$fingerprint_git_status" in
    "") echo "[--] fingerprint contract is already current" ;;
    " M keys/expected-fingerprints.txt") ;;
    *)
      echo "error: expected only keys/expected-fingerprints.txt to change" >&2
      git status --short >&2
      exit 1
      ;;
  esac
  git diff --check
  git diff -- keys/expected-fingerprints.txt
)
```

The export contains public material only. The inserter writes both
fingerprints atomically from `decernor` records; the verification target
independently recomputes and compares both values. Stop if the export is
incomplete, any command fails, an unexpected file changes, or a `TBD`
value remains.

## 2. `make release-prep` (commit-cycle gate)

Runs the full per-PR gate plus license / security / SBOM scans.

```bash
make release-prep
```

Expects: `pr-final ✓` (clippy + tests + restart_harness + MSRV `--locked`
+ workflow-lint), `license-check ✓`, `security-scan ✓` (0 high / 0
critical), SBOM generated under `sbom/`.

## 3. `make release-smoke` (PER-032)

Live-Mattermost URL-shape smoke against a disposable test channel.

```bash
make release-smoke
```

**A failed smoke halts the release cycle here** — no tag is created,
no draft release exists, no signed artifacts are produced. Fix the
underlying URL/contract issue and re-run before proceeding.

## 4. `make release-preflight` (pre-tag readiness gate)

Pre-tag, non-draft-dependent gate. Validates clean tree, version sync,
no conflicting tag/release, tooling on PATH, signing keys present.

```bash
export CHANVOY_MINISIGN_KEY=/path/to/minisign-secret-key
export CHANVOY_PGP_KEY_ID=ABC123...
export CHANVOY_GPG_HOMEDIR=/path/to/isolated/gnupg
export CHANVOY_RELEASE_TAG="v$(cat VERSION)"
export RELEASE_TAG="$CHANVOY_RELEASE_TAG"
make release-preflight
```

Checks (each fails fast with a clear hint):
- `make release-prep` green
- Working tree clean (`git status` empty)
- `VERSION` + `Cargo.toml` consistent
- No conflicting `vX.Y.Z` tag locally OR on origin
- No published GitHub release for this version
- `gh`, `minisign`, `gpg` available on PATH
- `decernor` **0.1.4+** available (`DECERNOR=` override allowed; strict `X.Y.Z` only)
- `CHANVOY_MINISIGN_KEY` set and points at an existing file
- `CHANVOY_PGP_KEY_ID` set and present in the GPG keyring
- `CHANVOY_GPG_HOMEDIR` set to the isolated release keyring
  (GPG signature over `checksums.txt` is mandatory for v0.2.2 trust
  posture — devrev PR #33 review)
- `CHANVOY_RELEASE_TAG` and `RELEASE_TAG`, when set, are identical and
  exactly match `v$(cat VERSION)`
- Clean `main` is synchronized to `origin/main`; the desired local and
  remote tag refs are absent
- `docs/releases/vX.Y.Z.md` exists

**This step does NOT inspect a draft release** — none exists yet.
Post-GHA draft checks live in §8 `release-download` + `release-verify`.

## 5. Create and push the signed tag

Only proceed if §2 / §3 / §4 are all green.

```bash
export CHANVOY_RELEASE_TAG="v$(cat VERSION)"
export RELEASE_TAG="$CHANVOY_RELEASE_TAG"
export GNUPGHOME="$CHANVOY_GPG_HOMEDIR"

# Creates and verifies the signed annotated tag locally. Does not push.
make release-tag

# Repeats the exact-tag, pinned-signer, clean-main, synced-commit, and
# origin-absence guards, then pushes only this tag ref.
make release-tag-push
```

Both targets fail before signing or pushing if the tag overrides disagree or
do not equal `v$(cat VERSION)`. `release-tag` requires an untagged clean `main`
at the exact live `origin` `main` commit. `release-tag-push` requires the
annotated tag to peel to that commit, the selected isolated-keyring primary to
equal the GPG value in `keys/expected-fingerprints.txt`, and the tag signature
to validate against that contracted fingerprint.
Neither target force-updates a tag.

## 6. GHA workflow (PER-031)

The `release` workflow fires on the tag push. It:
- Validates the tag matches the `VERSION` file
- Builds 3 binaries:
  - `chanvoy-vX.Y.Z-linux-x86_64` (`ubuntu-22.04`)
  - `chanvoy-vX.Y.Z-linux-aarch64` (`ubuntu-latest-arm64-s`, native)
  - `chanvoy-vX.Y.Z-macos-aarch64` (`macos-14`)
- Generates `checksums.txt` (SHA-256)
- Creates a **draft** GitHub release with title `chanvoy vX.Y.Z`,
  notes from `docs/releases/vX.Y.Z.md`, all binaries + checksums
  attached

Monitor the workflow run:

```bash
gh run watch --repo lanytehq/chanvoy
```

When the workflow completes, the draft URL is in the job summary.

## 7. First-public visibility gate

This section applies only while the repository is private. Keep the repository
private through the signed tag-only push and the exact-tag workflow/draft proof
in §6. Confirm the draft contains all three binaries plus `checksums.txt`, then
perform the explicit principal visibility change:

```bash
gh repo view lanytehq/chanvoy --json visibility -q .visibility
gh repo edit lanytehq/chanvoy --visibility public
gh repo view lanytehq/chanvoy --json visibility -q .visibility
```

If the first command already reports `PUBLIC`, the edit is a no-op and must not
be repeated. Stop if tag CI or draft verification is not green. The repository
must be public before any crates.io publication or GitHub Release publication.

v0.3.1 is GitHub-binary-only: do not run `cargo publish`. The root binary is
not a valid standalone registry inventory because it depends on unpublished
workspace crates and runtime git dependencies.

## 8. Local signing flow

All steps run locally. Idempotent: any step can be re-run safely.

```bash
# 8.1 — Download the draft release into a local working directory
make release-download                # writes release/vX.Y.Z/

# 8.2 — Regenerate checksums.txt locally (must byte-match what
#        the GHA workflow produced)
make release-checksums

# 8.3 — Export public signing keys into the release dir
make release-export-keys

# 8.4 — Sign: minisign per binary + GPG over checksums.txt
make release-sign

# 8.5 — Verify signatures AND that exported public-key files
#        match keys/expected-fingerprints.txt
make release-verify

# 8.6 — Attach signed artifacts + public keys to the draft
#        release (atomic — does NOT flip draft state)
make release-upload

# 8.7 — Execute the downloaded host binary and require VERSION, the tagged
#        commit, and Dirty: false; release-undraft depends on this gate.
make release-verify-identity

# 8.8 — Re-run the executable identity gate, then flip the GitHub release
#        from draft → published
#        (atomic — does NOT touch assets)
make release-undraft

# Or, the composite for the end-to-end publish step:
# make release-upload-all
```

If any step fails, fix the underlying issue and re-run. Don't skip
ahead with a half-signed release.

## 9. Release announcement

Post the release-published notice through the project release channel. Include:
- Release URL
- SHA-256 checksums (top of `release/vX.Y.Z/checksums.txt`)
- The verification commands from §11 below
- Download URLs for each binary

## 10. Operational announcement

Post the operational notification through the project's maintainer channel,
following its current version-note pin convention.

---

## 11. Verification commands for external adopters

These commands run **after** download from the published GitHub
release. No chanvoy clone or org access required — keys + signatures
are attached to the release.

```bash
# Verify a downloaded binary against its minisign signature
minisign -Vm chanvoy-vX.Y.Z-linux-x86_64 -p chanvoy.pub

# Verify the checksums manifest against the GPG signature
gpg --verify checksums.txt.asc checksums.txt

# Verify the downloaded binary matches the checksum in the manifest
sha256sum -c checksums.txt --ignore-missing
```

### Stable key fingerprints

External adopters should pin against these fingerprints. They change
only through a documented public key-rotation announcement.

| Algorithm | Fingerprint |
|---|---|
| minisign (`minisign-public-blob-sha256-v1`) | `36a80acfa44f5cf9ac402d3ce8e51fcc083e5a1dca22180d6a0ea85b7e5340ad` |
| GPG (OpenPGP primary, `--gpg-role primary`) | `83FCC69CB060EDB8374EDE0547AAC7D6EB946A84` |

The same values are checked into `keys/expected-fingerprints.txt` so
`make release-verify-keys` asserts an exported `chanvoy.pub` /
`chanvoy.gpg.asc` matches them.

---

## Troubleshooting

### `release-preflight` fails on "VERSION ($A) != Cargo.toml ($B)"
Run `make version-sync` to bring `Cargo.toml` in line with `VERSION`,
or `make version-set V=X.Y.Z` to set both atomically.

### `release-preflight` fails on "CHANVOY_MINISIGN_KEY not set" or "CHANVOY_PGP_KEY_ID not set"
Source the env file that exports your signing-key paths. Both are
mandatory for v0.2.2 trust posture (devrev PR #33 review — silent
skip of GPG would let a release ship without manifest-level
authenticity):
```bash
export CHANVOY_MINISIGN_KEY=$HOME/.minisign/chanvoy-secret.key
export CHANVOY_PGP_KEY_ID=ABC123...
```

### `release-verify-keys` fails on "TBD placeholder"
`keys/expected-fingerprints.txt` still has placeholders. Fill both
lines together with the inserter (decernor **0.1.4+**, explicit
public files). Do not hand-type hex. A missing minisign `.pub` or
GPG export leaves the dest unchanged.

```bash
make insert-expected-fingerprints MINISIGN_PUB=... GPG_ASC=...
```

### `insert-expected-fingerprints` / `verify-public-keys` fail on "decernor … too old"
The host binary is older than 0.1.4. Install 0.1.4 or later and set
`DECERNOR=` if PATH still points at an older copy. Do not verify
against a pre-0.1.4 contract.

### `release-checksums` fails on "no chanvoy-v*-* binaries found"
You ran the target before `make release-download` populated the
working dir, or `RELEASE_DIR` is pointing at a different path. Default:
`release/v$(cat VERSION)/`.

### `release-upload-all` partially failed
The atomic split means you can re-run just the failing half:
- `make release-upload` re-attaches assets (idempotent via `--clobber`)
- `make release-undraft` re-flips draft state (idempotent — no-op if
  already published)
