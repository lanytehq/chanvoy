#!/usr/bin/env bash
# Receipt/direct-entry adaptation slice; all remotes and tag gates are stubs.
# Signature verification has a separate real-key disposable fixture corpus.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/chanvoy-stage-test.XXXXXX")"
scratch="$(cd "$scratch" && pwd -P)"
trap 'rm -rf "$scratch"' EXIT
repo="$scratch/repo"
mkdir -p "$repo/scripts" "$repo/config/release" "$scratch/bin"
for name in release-common.sh release-verify-staged.sh generate-checksums.sh verify-checksums.sh validate-release-assets.sh; do
	cp "$root/scripts/$name" "$repo/scripts/"
done
cp "$root/config/release/binary-platforms.txt" "$repo/config/release/"
printf '9.9.9\n' >"$repo/VERSION"
cat >"$repo/scripts/release-verify-published-tag.sh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
printf 'reverify %s/%s/%s\n' "$CHANVOY_RELEASE_TAG" "$CHANVOY_EXPECTED_TAG_OBJECT" "$CHANVOY_EXPECTED_COMMIT" >>"$FIXTURE_LOG"
[[ "${FIXTURE_TAG_REPLACED:-0}" == 0 && "$CHANVOY_RELEASE_TAG" == v1.2.3 &&
    "$CHANVOY_EXPECTED_TAG_OBJECT" == "$FIXTURE_OBJECT" && "$CHANVOY_EXPECTED_COMMIT" == "$FIXTURE_COMMIT" ]]
SH
cat >"$scratch/bin/gh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$FIXTURE_LOG"
[[ "$*" == 'api repos/lanytehq/chanvoy/actions/runs/11' ]] || exit 1
[[ "${FIXTURE_API_FAILURE:-0}" == 0 ]] || exit 1
printf '%s\n' "$FIXTURE_RUN_JSON"
SH
chmod +x "$scratch/bin/gh"
# This slice isolates receipt/run checks. Exact tagged data gets its own fixture.
printf '#!/usr/bin/env bash\nexit 0\n' >"$repo/scripts/release-verify-staged-data.sh"
export PATH="$scratch/bin:$PATH" FIXTURE_LOG="$scratch/calls"
export FIXTURE_OBJECT=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
export FIXTURE_COMMIT=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
export CHANVOY_RELEASE_TAG=v1.2.3
unset GH_REPO
good_json="$(jq -cn --arg commit "$FIXTURE_COMMIT" '{id:11, head_sha:$commit,
    head_branch:"v1.2.3", event:"push", status:"completed", conclusion:"success",
    path:".github/workflows/release.yml", repository:{full_name:"lanytehq/chanvoy"}}')"
export FIXTURE_RUN_JSON="$good_json"
directory="$scratch/assets"
mkdir "$directory"
receipt() {
	printf 'tag=v1.2.3\nobject=%s\ncommit=%s\nrun=11\n' "$FIXTURE_OBJECT" "$FIXTURE_COMMIT" >"$directory.anchor"
}
receipt
verify() { bash "$repo/scripts/release-verify-staged.sh" "$directory"; }
reject() {
	if "$@" >"$scratch/out" 2>&1; then
		echo 'error: expected complete staged receipt rejection' >&2
		exit 1
	fi
}
verify >/dev/null
grep -q "^reverify v1.2.3/$FIXTURE_OBJECT/$FIXTURE_COMMIT$" "$FIXTURE_LOG"
grep -q '^api repos/lanytehq/chanvoy/actions/runs/11$' "$FIXTURE_LOG"
cp "$directory.anchor" "$scratch/good.anchor"
for bad in \
	'tag=v1.2.4' 'object=zz' 'commit=zz' 'run=0' 'run=011' 'run=x'; do
	field="${bad%%=*}"
	grep -v "^$field=" "$scratch/good.anchor" >"$directory.anchor"
	printf '%s\n' "$bad" >>"$directory.anchor"
	reject verify
done
for field in tag object commit run; do
	grep -v "^$field=" "$scratch/good.anchor" >"$directory.anchor"
	reject verify
done
cp "$scratch/good.anchor" "$directory.anchor"
printf 'run=11\n' >>"$directory.anchor"
reject verify
cp "$scratch/good.anchor" "$directory.anchor"
printf 'extra=1\n' >>"$directory.anchor"
reject verify
rm "$directory.anchor"
reject verify
ln -s "$scratch/good.anchor" "$directory.anchor"
reject verify
rm "$directory.anchor"
receipt
ln -s "$directory" "$scratch/link"
cp "$directory.anchor" "$scratch/link.anchor"
reject bash "$repo/scripts/release-verify-staged.sh" "$scratch/link"
for filter in '.id=12' '.head_sha="0000000000000000000000000000000000000000"' \
	'.head_branch="v9.9.9"' '.event="pull_request"' '.status="in_progress"' \
	'.conclusion="failure"' '.path=".github/workflows/ci.yml"' '.repository.full_name="other/repo"'; do
	FIXTURE_RUN_JSON="$(jq -c "$filter" <<<"$good_json")" reject verify
done
FIXTURE_TAG_REPLACED=1 reject verify
FIXTURE_API_FAILURE=1 reject verify
GH_REPO=other/repo reject verify
verify >/dev/null
# Guarded checksum generation must fail closed on direct entry, not just Make.
assets=(LICENSE-APACHE LICENSE-MIT sbom-1.2.3.cdx.json
	chanvoy-v1.2.3-linux-x86_64 chanvoy-v1.2.3-linux-aarch64 chanvoy-v1.2.3-macos-aarch64
	release-notes-v1.2.3.md expected-fingerprints.txt expected-fingerprints.ndjson)
for asset in "${assets[@]}"; do printf 'synthetic\n' >"$directory/$asset"; done
rm "$directory.anchor"
reject bash "$repo/scripts/generate-checksums.sh" "$directory"
[[ ! -e "$directory/SHA256SUMS" && ! -e "$directory/SHA512SUMS" ]]
receipt
FIXTURE_TAG_REPLACED=1 reject bash "$repo/scripts/generate-checksums.sh" "$directory"
[[ ! -e "$directory/SHA256SUMS" && ! -e "$directory/SHA512SUMS" ]]
bash "$repo/scripts/generate-checksums.sh" "$directory" >/dev/null
bash "$repo/scripts/verify-checksums.sh" "$directory" >/dev/null
[[ -f "$directory.anchor" && ! -e "$directory/staging.anchor" ]]
echo '[ok] complete receipt/run identity and direct-entry checksum guards'

# Exact tagged-data staging and checksum gates after main advances.
tagged="$scratch/tagged-repo"
git init -q -b main "$tagged"
git -C "$tagged" config user.name Synthetic
git -C "$tagged" config user.email synthetic@example.invalid
mkdir -p "$tagged/scripts" "$tagged/config/release" "$tagged/keys" "$tagged/docs/security" "$tagged/docs/releases"
for name in release-common.sh release-verify-staged.sh release-verify-staged-data.sh stage-release-anchors.sh \
	generate-checksums.sh verify-checksums.sh validate-release-assets.sh; do
	cp "$root/scripts/$name" "$tagged/scripts/"
done
cp "$root/config/release/binary-platforms.txt" "$tagged/config/release/"
printf '1.2.3\n' >"$tagged/VERSION"
printf 'Tagged notes\n' >"$tagged/docs/releases/v1.2.3.md"
printf 'Tagged MIT license\n' >"$tagged/LICENSE-MIT"
printf 'Tagged Apache license\n' >"$tagged/LICENSE-APACHE"
printf 'Tagged text anchors\n' >"$tagged/keys/expected-fingerprints.txt"
printf 'Tagged NDJSON anchors\n' >"$tagged/keys/expected-fingerprints.ndjson"
printf 'Tagged public pin\n' >"$tagged/docs/security/release-signing-keys.asc"
# Deliberately hostile tagged helper: trusted checkout must never execute it.
cat >"$tagged/scripts/validate-release-anchors.sh" <<'SH'
#!/usr/bin/env bash
touch "$FIXTURE_UNTRUSTED_MARKER"
exit 99
SH
git -C "$tagged" add .
git -C "$tagged" commit -qm 'tagged inert fixture'
git -C "$tagged" tag -am synthetic v1.2.3
FIXTURE_COMMIT="$(git -C "$tagged" rev-parse HEAD)"
FIXTURE_OBJECT="$(git -C "$tagged" rev-parse refs/tags/v1.2.3)"
export FIXTURE_COMMIT FIXTURE_OBJECT
export FIXTURE_UNTRUSTED_MARKER="$scratch/tagged-code-executed"
cp "$repo/scripts/release-verify-published-tag.sh" "$tagged/scripts/"
# Authenticity and pair validation are deliberately stubbed only in this slice;
# verify-pinned-tag.test.sh covers their real Decernor/GPG implementation.
printf '#!/usr/bin/env bash\nexit 0\n' >"$tagged/scripts/validate-release-anchors.sh"
printf '9.9.9\n' >"$tagged/VERSION"
printf 'Current-main notes, not release notes\n' >"$tagged/docs/releases/v1.2.3.md"
printf 'Current-main anchors\n' >"$tagged/keys/expected-fingerprints.txt"
printf 'Current-main records\n' >"$tagged/keys/expected-fingerprints.ndjson"
git -C "$tagged" add .
git -C "$tagged" commit -qm 'advance trusted main'
FIXTURE_RUN_JSON="$(jq -cn --arg commit "$FIXTURE_COMMIT" '{id:11, head_sha:$commit,
    head_branch:"v1.2.3", event:"push", status:"completed", conclusion:"success",
    path:".github/workflows/release.yml", repository:{full_name:"lanytehq/chanvoy"}}')"
staged="$scratch/tagged-assets"
mkdir "$staged"
for asset in "${assets[@]:0:6}"; do printf 'synthetic\n' >"$staged/$asset"; done
git -C "$tagged" show "$FIXTURE_COMMIT:LICENSE-MIT" >"$staged/LICENSE-MIT"
git -C "$tagged" show "$FIXTURE_COMMIT:LICENSE-APACHE" >"$staged/LICENSE-APACHE"
printf 'tag=v1.2.3\nobject=%s\ncommit=%s\nrun=11\n' "$FIXTURE_OBJECT" "$FIXTURE_COMMIT" >"$staged.anchor"
bash "$tagged/scripts/stage-release-anchors.sh" "$staged" >/dev/null
[[ ! -e "$FIXTURE_UNTRUSTED_MARKER" ]]
[[ "$(cat "$staged/release-notes-v1.2.3.md")" == 'Tagged notes' ]]
[[ "$(cat "$staged/expected-fingerprints.txt")" == 'Tagged text anchors' ]]
[[ "$(cat "$staged/expected-fingerprints.ndjson")" == 'Tagged NDJSON anchors' ]]
bash "$tagged/scripts/release-verify-staged-data.sh" "$staged" >/dev/null
for metadata in release-notes-v1.2.3.md expected-fingerprints.txt expected-fingerprints.ndjson LICENSE-MIT LICENSE-APACHE; do
	cp "$staged/$metadata" "$scratch/saved-data"
	printf 'tampered\n' >>"$staged/$metadata"
	reject bash "$tagged/scripts/generate-checksums.sh" "$staged"
	[[ ! -e "$staged/SHA256SUMS" && ! -e "$staged/SHA512SUMS" ]]
	mv "$scratch/saved-data" "$staged/$metadata"
done
bash "$tagged/scripts/generate-checksums.sh" "$staged" >/dev/null
bash "$tagged/scripts/verify-checksums.sh" "$staged" >/dev/null
[[ ! -e "$FIXTURE_UNTRUSTED_MARKER" ]]
echo '[ok] tagged notes/pair/license binding survives advancing main without tagged code execution'
