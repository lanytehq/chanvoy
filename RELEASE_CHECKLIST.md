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
deliberately deferred to v0.2.3+.

Canonical release sequence (top-to-bottom; each numbered section
below corresponds to one step):

```
make release-prep      → release-smoke → release-preflight
git tag → git push     → GHA produces draft
make release-download  → release-sign → release-verify
make release-upload    → release-undraft
#release-chanvoy-vXYZ announcement → #ops-updates announcement
(v0.2.2 only) public-flip: repo visibility PRIVATE → PUBLIC
```

---

## 1. Pre-release verification

- [ ] All feature PRs for this release are merged to `main`
- [ ] `main` CI is green
- [ ] Working tree is clean (`git status` empty)
- [ ] `VERSION` matches `Cargo.toml` workspace + crate versions
      (`make version-check`)
- [ ] Release notes exist at `docs/releases/vX.Y.Z.md`
      (see §10 AC #3a — must inline fingerprints + verification
      commands OR hard-pointer to this file)
- [ ] All planned briefs for this release are at "done" status

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
make release-preflight
```

Checks (each fails fast with a clear hint):
- `make release-prep` green
- Working tree clean (`git status` empty)
- `VERSION` + `Cargo.toml` consistent
- No conflicting `vX.Y.Z` tag locally OR on origin
- No published GitHub release for this version
- `gh`, `minisign`, `gpg` available on PATH
- `CHANVOY_MINISIGN_KEY` set and points at an existing file
- `CHANVOY_PGP_KEY_ID` set and present in the GPG keyring
  (GPG signature over `checksums.txt` is mandatory for v0.2.2 trust
  posture — devrev PR #33 review)
- `docs/releases/vX.Y.Z.md` exists

**This step does NOT inspect a draft release** — none exists yet.
Post-GHA draft checks live in §7 `release-download` + `release-verify`.

## 5. Tag push

Only proceed if §2 / §3 / §4 are all green.

```bash
VERSION=$(cat VERSION)
git tag -a "v${VERSION}" -m "v${VERSION}"
git push origin "v${VERSION}"
```

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

## 7. Local signing flow

All steps run locally. Idempotent: any step can be re-run safely.

```bash
# 7.1 — Download the draft release into a local working directory
make release-download                # writes release/vX.Y.Z/

# 7.2 — Regenerate checksums.txt locally (must byte-match what
#        the GHA workflow produced)
make release-checksums

# 7.3 — Export public signing keys into the release dir
make release-export-keys

# 7.4 — Sign: minisign per binary + GPG over checksums.txt
make release-sign

# 7.5 — Verify signatures AND that exported public-key files
#        match keys/expected-fingerprints.txt
make release-verify

# 7.6 — Attach signed artifacts + public keys to the draft
#        release (atomic — does NOT flip draft state)
make release-upload

# 7.7 — Flip the GitHub release from draft → published
#        (atomic — does NOT touch assets)
make release-undraft

# Or, the composite for the end-to-end publish step:
# make release-upload-all
```

If any step fails, fix the underlying issue and re-run. Don't skip
ahead with a half-signed release.

## 8. `#release-chanvoy-vXYZ` announcement

Post the release-published notice to `#release-chanvoy-vXYZ` (rotating
per-release channel). Include:
- Release URL
- SHA-256 checksums (top of `release/vX.Y.Z/checksums.txt`)
- The verification commands from §11 below
- Download URLs for each binary

## 9. `#ops-updates` announcement

Post the operational notification to `#ops-updates`. Pin per the
chanvoy version-notes pin convention (unpin the prior version's note
first; see `reference_chanvoy_version_pin_convention.md`).

## 10. Public-flip terminal action (v0.2.2 only)

After §8 + §9 are out, flip the repo visibility from PRIVATE → PUBLIC.
This is the LAST action of the cycle.

```bash
gh repo edit lanytehq/chanvoy --visibility public
```

For v0.2.3+ this step is omitted (repo is already public).

---

## 11. Verification commands for external adopters

These commands run **after** download from the published GitHub
release. No chanvoy clone or org access required — keys + signatures
are attached to the release.

```bash
# Verify a downloaded binary against its minisign signature
minisign -Vm chanvoy-vX.Y.Z-linux-x86_64 -p chanvoy-minisign.pub

# Verify the checksums manifest against the GPG signature
gpg --verify checksums.txt.asc checksums.txt

# Verify the downloaded binary matches the checksum in the manifest
sha256sum -c checksums.txt --ignore-missing
```

### Stable key fingerprints

External adopters should pin against these fingerprints. They change
only via a documented key-rotation announcement on `#ops-updates`.

| Algorithm | Fingerprint |
|---|---|
| minisign | `TBD — pinned at impl time from dispatch's keypair provisioning` |
| GPG | `TBD — pinned at impl time from dispatch's keypair provisioning` |

The same values are checked into `keys/expected-fingerprints.txt` so
`make release-verify-keys` asserts an exported `chanvoy-minisign.pub` /
`chanvoy-release-signing-key.asc` matches them.

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
The expected fingerprints in `keys/expected-fingerprints.txt` are
still the pre-provisioning placeholders. Fill in the real values
after dispatch's keypair provisioning completes (Dave's step A on
`#release-chanvoy-v022`).

### `release-checksums` fails on "no chanvoy-v*-* binaries found"
You ran the target before `make release-download` populated the
working dir, or `RELEASE_DIR` is pointing at a different path. Default:
`release/v$(cat VERSION)/`.

### `release-upload-all` partially failed
The atomic split means you can re-run just the failing half:
- `make release-upload` re-attaches assets (idempotent via `--clobber`)
- `make release-undraft` re-flips draft state (idempotent — no-op if
  already published)
