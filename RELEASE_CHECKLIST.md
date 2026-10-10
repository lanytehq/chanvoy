# Chanvoy release checklist

The maintainer signs locally. Tag CI has `contents: read`, builds exactly three
native binaries with locked dependencies, and uploads artifacts. It does not
create a GitHub draft, publish a release, or receive signing keys.

## Prepare the reviewed cut

Merge the independently reviewed release changes. Finalize the actual date in
`CHANGELOG.md`, `RELEASE_NOTES.md` and `docs/releases/vX.Y.Z.md` before signing.
Run fresh `make release-prep`, hosted checks and `make release-smoke`. The live
smoke creates a disposable Mattermost channel; failed smoke stops the ceremony.
`make release-preflight` first checks the pinned scanner's effective configuration
and reads an owned synthetic payload through its production snapshot mount.
The outer evidence directory remains private; the single-file snapshot is
read-only and traversable by the isolated scanner. Docker must be available;
the check prints the retained evidence directory.
Use clean main synchronized with live origin/main. Signing and publication each
require the maintainer's separate approval.

### Required repository readiness review

Before local tag creation, confirm and record all of the following. Preflight
scripts may automate these checks; any check they do not enforce must be
completed manually. Successful signing or signature verification alone does
not establish repository readiness.

- [ ] All repository PRs are complete: merged or closed, with no open PRs,
      including drafts. Review their dispositions and confirm all intended release
      changes are merged. An unmerged PR with failed checks stops the ceremony.
- [ ] The checkout is on `main`, updated from a fresh fetch of `origin/main`,
      and `HEAD` equals `origin/main`. Stop on divergence; do not force an update.
- [ ] The working tree and index are clean, with no unstaged or uncommitted
      changes and no untracked files reported by Git.
- [ ] Required checks passed for the exact final merged commit. If updating
      main changes the reviewed cut, obtain the required review and qualification
      for that new commit before tagging.

Inspect the complete open-PR list, then update an already clean checkout:

```bash
gh pr list --state open --limit 1000 --json number,title,isDraft,url
git status --porcelain=v1 --untracked-files=all
# Proceed only when the PR list and working-tree status are empty.
git switch main
git pull --ff-only origin main
git rev-parse HEAD refs/remotes/origin/main
git status --porcelain=v1 --untracked-files=all
# Require identical commit hashes and empty status.
```

An incomplete or failed PR query/fetch is unknown, not proof of readiness.
Recheck PR completion, fresh main synchronization and clean status immediately
before the separate tag push. If the cut changed after local signing, stop and
resolve it before pushing; do not replace or retarget the signed tag.

Configure existing approved inputs outside the checkout:

```bash
export CHANVOY_RELEASE_TAG=vX.Y.Z
export CHANVOY_DECERNOR_BIN=/absolute/path/to/decernor
export CHANVOY_GPG_SIGNING_FINGERPRINT=<approved-40-uppercase-hex-primary>
export CHANVOY_PGP_KEY_ID='<approved-40-uppercase-hex-signing-subkey>!'
export CHANVOY_GPG_HOMEDIR=/absolute/path/to/isolated/gnupg
export CHANVOY_MINISIGN_KEY=/absolute/path/to/minisign-secret.key
export CHANVOY_MINISIGN_PUB=/absolute/path/to/minisign.pub
export CHANVOY_TAGGER_NAME='3 Leaps Infosec Team'
export CHANVOY_TAGGER_EMAIL=infosec@3leaps.net
export CHANVOY_TAG_MESSAGE_DIR=/absolute/external/path/vX.Y.Z
```

Decernor must be a regular executable at the explicit absolute path, version
**0.1.8 or later** with matching short and extended identity. The primary and
exact signing subkey must match the reviewed public pin. Private material stays
outside the repository and CI. These commands do not generate new keys.
An optional `CHANVOY_APPROVED_ENV_LOADER` must be an external absolute regular
shell file already approved by the maintainer; it cannot change the intended cut.

## Public trust setup and rotation

This is a separate reviewed change before the release tag. Independently confirm
the approved existing GPG primary, exactly one live signing subkey, and minisign
public-blob SHA-256. Do not copy fingerprints from another product or hand-edit
hex. Use `make release-export-pin` for the initial public ASC and
`make release-insert-anchors` for the TXT/NDJSON pair. Both refuse silent overwrite.
The existing TXT-only layout requires explicit rotation approval:

```bash
old_txt_sha256=$(shasum -a 256 keys/expected-fingerprints.txt | awk '{print $1}')
bash scripts/release-insert-anchors.sh --rotate-from "$old_txt_sha256"
# When replacing an existing ASC, independently review its prior digest first:
# bash scripts/release-export-pin.sh --replace-from <reviewed-current-asc-sha256>
make release-validate-pin
```

Review the resulting ASC and paired anchors independently, including public-only
content, subkey capabilities, expiration/revocation and re-derived fingerprints.
Publish the reviewed old/new identity and effective version in the rotation notice.
Retain [v0.3.1 verification](docs/security/release-verification.md#historical-v031).
Tooling alone does not supply or approve new public identities.

## Prepare, sign and push the tag separately

```bash
make release-prepare-tag-message
# Review/edit the public external message.txt; preparation preserves an existing file.
make release-preflight
make release-tag
# Inspect the exact local annotated object, target, body and pinned signature.
make release-tag-push
```

`release-tag` creates and verifies the local tag only. Does not push.
Neither target force-updates a tag. They require clean synchronized main,
absent remote tag, approved tagger, reviewed external message and exact subkey.
Read-only rules inspection is advisory: UNKNOWN or FOUND does not authorize
signing or pushing. After an ambiguous push, inspect the remote ref and exact
object before doing anything else; do not delete or replace it.

## Read-only artifact CI and receipt-bound staging

Wait for the exact successful `release.yml` **push** run for the tag commit.
CI restores the annotated ref, verifies its committed public pin in an isolated
keyring plus GitHub Verified/valid, checks VERSION, and builds natively:

| Platform      | Asset                          | Runner                  |
| ------------- | ------------------------------ | ----------------------- |
| Linux x86_64  | `chanvoy-vX.Y.Z-linux-x86_64`  | `ubuntu-22.04`          |
| Linux aarch64 | `chanvoy-vX.Y.Z-linux-aarch64` | `ubuntu-latest-arm64-s` |
| macOS aarch64 | `chanvoy-vX.Y.Z-macos-aarch64` | `macos-14`              |

Confirm runner availability and hosted success on the actual cut. CI also creates
a versioned CycloneDX SBOM and stages both licenses in one exact base inventory.

```bash
make release-fetch-ci-artifacts
make release-stage-anchors
make release-checksums
make release-create-draft
```

Staging must start empty with no prior receipt. `RELEASE_DIR` defaults to
`release/$CHANVOY_RELEASE_TAG`. Its sibling `.anchor` receipt binds repository
(via fixed checks), tag object, peeled commit and exact successful run. The receipt
is local evidence and is never uploaded. Trusted checkout helpers read tagged
notes, pins, paired anchors and licenses as inert git blobs. Post-tag verification
works after main changes VERSION, notes and anchors; it never executes tagged
helpers or substitutes current-main metadata.

The maintainer creates the draft from the exact receipt-bound checksummed set.
There must be no existing release for this tag. CI never creates this draft.

## Sign, upload, verify fresh bytes and promote

```bash
make release-sign
make release-export-keys
make release-verify
make release-upload
make release-verify-draft
# Only after the separate publication approval:
export CHANVOY_CONFIRM_PUBLISH="$CHANVOY_RELEASE_TAG"
make release-undraft
```

One exact inventory covers the three binaries, versioned SBOM, two licenses,
`release-notes-vX.Y.Z.md`, TXT/NDJSON anchors, `SHA256SUMS`, `SHA512SUMS`, both
`.asc` and `.minisig` manifest signatures, and legacy `checksums.txt`,
`checksums.txt.asc`, per-binary `.minisig`, `chanvoy.pub`, `chanvoy.gpg.asc`.
Manifests cover payload and metadata only; they do not include themselves,
signatures, exported keys or the local receipt. `checksums.txt` is byte-identical
to `SHA256SUMS`, and its `.asc` is the same detached signature.

Upload enumerates only the missing provenance names from the exact expected draft
state. It never clobbers. Fresh draft verification downloads every asset anew,
checks exact inventory, tagged metadata, checksums, both signature formats and
byte equality to the approved local signed cut. Only then does it execute the
native host binary and require the tag version, tag commit and `Dirty: false`.
Direct promotion repeats these gates, including host identity, before changing
draft state. A Make dependency is not the sole enforcement boundary.

A partial or ambiguous create/upload/promotion must be inspected before retrying.
Do not delete evidence, replace assets, assume publication succeeded, or blindly
repeat a command. Published-release replacement is refused. Retain exact object,
commit/run receipt and local signed bytes for review.

## Compatibility and consumer verification

`release-download` now aliases exact CI-artifact staging, rather than draft
download. `release-upload-all` remains an ordered upload/promotion alias but still
requires the explicit promotion confirmation. `release-clean` refuses automatic
cleanup. Equal `RELEASE_TAG` remains a compatibility input; prefer the explicit
`CHANVOY_RELEASE_TAG`, and conflicting aliases fail closed.

Legacy asset names and consumer commands remain available in v0.3.2. They are
deprecated for future releases, with removal requiring a separate notice and cut.
See [verification and rotation guidance](docs/security/release-verification.md).
There is no crates.io publication in this binary release flow.

After publication, inspect the published release state and announce the exact
release URL and verification guidance. Install the actual released CLI only
under the separate installation cue, cycle the appropriate profile daemons, and
prove CLI/daemon identity with `version --extended`.
