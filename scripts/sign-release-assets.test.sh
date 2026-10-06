#!/usr/bin/env bash
# Complete disposable ceremony: real short-lived test keys, stub remotes only.
# shellcheck disable=SC2016 # Child shell snippets intentionally expand there.
set -euo pipefail
trap 'echo "error: disposable ceremony failed at test line $LINENO" >&2' ERR
root="$(cd "$(dirname "$0")/.." && pwd -P)"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/pg-cut.XXXXXX")"
scratch="$(cd "$scratch" && pwd -P)"
cleanup() {
	gpgconf --homedir "$scratch/gpg" --kill all >/dev/null 2>&1 || true
	rm -rf "$scratch"
}
trap cleanup EXIT
export GNUPGHOME="$scratch/gpg"
mkdir -m 700 "$GNUPGHOME"
gpg --batch --pinentry-mode loopback --passphrase '' --quick-generate-key \
	'Synthetic ceremony <synthetic@example.invalid>' ed25519 cert,sign 1d >/dev/null 2>&1
primary="$(gpg --batch --with-colons --fingerprint --list-keys | awk -F: '$1=="fpr" {print $10;exit}')"
gpg --batch --pinentry-mode loopback --passphrase '' --quick-add-key "$primary" ed25519 sign 1d >/dev/null 2>&1
subkey="$(gpg --batch --with-colons --with-subkey-fingerprint --list-keys | awk -F: '$1=="sub" {s=1;next} s && $1=="fpr" {print $10;exit}')"
export CHANVOY_GPG_SIGNING_FINGERPRINT="$primary" CHANVOY_PGP_KEY_ID="$subkey!" CHANVOY_GPG_HOMEDIR="$GNUPGHOME"
export CHANVOY_MINISIGN_KEY="$scratch/minisign.key" CHANVOY_MINISIGN_PUB="$scratch/minisign.pub"
minisign -G -W -s "$CHANVOY_MINISIGN_KEY" -p "$CHANVOY_MINISIGN_PUB" >/dev/null 2>&1
CHANVOY_DECERNOR_BIN="${CHANVOY_DECERNOR_BIN:-$(command -v decernor)}"
export CHANVOY_DECERNOR_BIN CHANVOY_RELEASE_TAG=v1.2.3
unset GH_REPO RELEASE_TAG CHANVOY_APPROVED_ENV_LOADER CHANVOY_ANCHOR_OUT \
	CHANVOY_EXPECTED_COMMIT CHANVOY_EXPECTED_TAG_OBJECT CHANVOY_REQUIRE_TAG
repo="$scratch/repo"
git init -q -b main "$repo"
git -C "$repo" config user.name '3 Leaps Infosec Team'
git -C "$repo" config user.email infosec@3leaps.net
mkdir -p "$repo/scripts" "$repo/config/release" "$repo/keys" "$repo/docs/security" "$repo/docs/releases" "$repo/schemas/release/v0"
for name in release-common.sh release-decernor.sh release-verify-staged.sh release-verify-staged-data.sh \
	stage-release-anchors.sh validate-release-assets.sh validate-release-anchors.sh generate-checksums.sh \
	verify-checksums.sh verify-pinned-tag.sh verify-public-keys.sh verify-signatures.sh sign-release-assets.sh \
	export-release-keys.sh release-fetch-ci-artifacts.sh check-draft-release.sh release-create-draft.sh \
	upload-release-assets.sh release-verify-draft.sh release-publish.sh release-export-pin.sh \
	release-validate-pin.sh release-insert-anchors.sh install-release-anchors.sh release-tag-common.sh \
	release-tag-operator.sh release-prepare-tag-message.py release-guard-tag-version.sh \
	release-tag-body.sh release-inspect-tag-ruleset.sh release-tag.sh release-verify-tag.sh \
	release-push-tag.sh release-verify-remote-tag.sh verify-release-binary-identity.sh; do
	cp "$root/scripts/$name" "$repo/scripts/"
done
cp "$root/config/release/"{binary-platforms.txt,tagger-identity.txt} "$repo/config/release/"
cp "$root/schemas/release/v0/fingerprint-record.schema.json" "$repo/schemas/release/v0/"
mkdir -p "$repo/scripts/lib"
cp "$root/scripts/lib/verify-release-identity.sh" "$repo/scripts/lib/"
printf '1.2.3\n' >"$repo/VERSION"
printf 'Tagged release notes\n' >"$repo/docs/releases/v1.2.3.md"
printf 'Synthetic MIT\n' >"$repo/LICENSE-MIT"
printf 'Synthetic Apache\n' >"$repo/LICENSE-APACHE"
echo '[test] approved synthetic public export and paired-anchor precursors'
for input in "$scratch/missing.pub" "$CHANVOY_MINISIGN_KEY"; do
	if CHANVOY_MINISIGN_PUB="$input" bash "$repo/scripts/release-export-pin.sh" >"$scratch/rejected" 2>&1; then
		echo 'error: unsafe public input passed export preflight' >&2; exit 1;
	fi
	[[ ! -e "$repo/docs/security/release-signing-keys.asc" ]]
done
bash "$repo/scripts/release-export-pin.sh" >/dev/null
bash "$repo/scripts/release-validate-pin.sh" >/dev/null
bash "$repo/scripts/release-insert-anchors.sh" >/dev/null
if bash "$repo/scripts/release-export-pin.sh" >"$scratch/rejected" 2>&1; then
	echo 'error: existing public pin was overwritten' >&2; exit 1;
fi
CHANVOY_PGP_KEY_ID="$primary!" bash "$repo/scripts/release-validate-pin.sh" >"$scratch/rejected" 2>&1 && {
	echo 'error: primary selected as signing subkey' >&2; exit 1;
}
cp -R "$repo/keys" "$scratch/old-pair"
old_digest="$(shasum -a 256 "$repo/keys/expected-fingerprints.txt" | awk '{print $1}')"
if bash "$repo/scripts/release-insert-anchors.sh" >"$scratch/rejected" 2>&1; then
	echo 'error: implicit public rotation accepted' >&2; exit 1
fi
if bash "$repo/scripts/release-insert-anchors.sh" --rotate-from "$(printf '%064d' 0)" >"$scratch/rejected" 2>&1; then
	echo 'error: wrong prior-anchor identity accepted' >&2; exit 1
fi
diff -r "$scratch/old-pair" "$repo/keys"
bash "$repo/scripts/release-insert-anchors.sh" --rotate-from "$old_digest" >/dev/null
diff -r "$scratch/old-pair" "$repo/keys"
# Exercise a real changed public identity, then explicitly rotate back for the cut.
minisign -G -W -s "$scratch/rotated.key" -p "$scratch/rotated.pub" >/dev/null 2>&1
CHANVOY_MINISIGN_PUB="$scratch/rotated.pub" bash "$repo/scripts/release-insert-anchors.sh" --rotate-from "$old_digest" >/dev/null
if cmp -s "$scratch/old-pair/expected-fingerprints.txt" "$repo/keys/expected-fingerprints.txt"; then
	echo 'error: explicit rotation did not change the public identity' >&2; exit 1;
fi
rotated_digest="$(shasum -a 256 "$repo/keys/expected-fingerprints.txt" | awk '{print $1}')"
bash "$repo/scripts/release-insert-anchors.sh" --rotate-from "$rotated_digest" >/dev/null
diff -r "$scratch/old-pair" "$repo/keys"
# Migrate the legacy TXT-only layout under the same explicit prior-byte guard.
rm "$repo/keys/expected-fingerprints.ndjson"
bash "$repo/scripts/release-insert-anchors.sh" --rotate-from "$old_digest" >/dev/null
diff -r "$scratch/old-pair" "$repo/keys"
cp "$repo/docs/security/release-signing-keys.asc" "$scratch/old-pin"
old_pin_digest="$(shasum -a 256 "$scratch/old-pin" | awk '{print $1}')"
bash "$repo/scripts/release-export-pin.sh" --replace-from "$old_pin_digest" >/dev/null
cmp -s "$scratch/old-pin" "$repo/docs/security/release-signing-keys.asc"
pin_time="$(python3 -c 'import os,sys; s=os.stat(sys.argv[1]); print(s.st_mtime_ns,s.st_ctime_ns)' "$repo/docs/security/release-signing-keys.asc")"
bash "$repo/scripts/release-validate-pin.sh" >/dev/null
cmp -s "$scratch/old-pin" "$repo/docs/security/release-signing-keys.asc"
[[ "$pin_time" == "$(python3 -c 'import os,sys; s=os.stat(sys.argv[1]); print(s.st_mtime_ns,s.st_ctime_ns)' "$repo/docs/security/release-signing-keys.asc")" ]]
mkdir "$scratch/new-pair"
printf 'changed text\n' >"$scratch/new-pair/expected-fingerprints.txt"
printf 'changed records\n' >"$scratch/new-pair/expected-fingerprints.ndjson"
if CHANVOY_TEST_FAIL_ANCHOR_INSTALL=1 bash "$repo/scripts/install-release-anchors.sh" \
	"$scratch/new-pair" "$repo/keys" >"$scratch/rejected" 2>&1; then
	echo 'error: interrupted anchor pair installation passed' >&2; exit 1;
fi
diff -r "$scratch/old-pair" "$repo/keys"
if bash "$repo/scripts/install-release-anchors.sh" "$scratch/new-pair" "$repo/keys" false; then
	echo 'error: failed post-install validator passed' >&2; exit 1;
fi
diff -r "$scratch/old-pair" "$repo/keys"
# Stub only the remote binding. The local signed-tag and anchor gate stays real.
cat >"$repo/scripts/release-verify-published-tag.sh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
[[ "${FIXTURE_TAG_REPLACED:-0}" == 0 && "$CHANVOY_RELEASE_TAG" == v1.2.3 ]]
[[ "${CHANVOY_EXPECTED_TAG_OBJECT:-$FIXTURE_OBJECT}" == "$FIXTURE_OBJECT" &&
    "${CHANVOY_EXPECTED_COMMIT:-$FIXTURE_COMMIT}" == "$FIXTURE_COMMIT" ]]
bash "$(dirname "$0")/verify-pinned-tag.sh" --published >/dev/null
if [[ -n "${CHANVOY_ANCHOR_OUT:-}" ]]; then
    printf 'tag=v1.2.3\nobject=%s\ncommit=%s\n' "$FIXTURE_OBJECT" "$FIXTURE_COMMIT" >"$CHANVOY_ANCHOR_OUT"
fi
SH
git -C "$repo" add .
git -C "$repo" commit -qm 'synthetic ceremony'
FIXTURE_COMMIT="$(git -C "$repo" rev-parse HEAD)"
FIXTURE_OBJECT=''
export FIXTURE_COMMIT FIXTURE_OBJECT FIXTURE_LOG="$scratch/calls" FIXTURE_REMOTE="$scratch/remote"
export FIXTURE_BUILD="$scratch/build" FIXTURE_STATE="$scratch/state"
mkdir "$FIXTURE_BUILD" "$FIXTURE_REMOTE" "$scratch/bin"
cp "$repo/LICENSE-APACHE" "$repo/LICENSE-MIT" "$FIXTURE_BUILD/"
for asset in chanvoy-v1.2.3-macos-aarch64 chanvoy-v1.2.3-linux-x86_64 chanvoy-v1.2.3-linux-aarch64 sbom-1.2.3.cdx.json; do
	printf 'synthetic payload\n' >"$FIXTURE_BUILD/$asset"
done
for platform in macos-aarch64 linux-x86_64 linux-aarch64; do
	cat >"$FIXTURE_BUILD/chanvoy-v1.2.3-$platform" <<'SH'
#!/usr/bin/env bash
printf 'host-executed\n' >>"$FIXTURE_LOG"
version=1.2.3; commit="${FIXTURE_COMMIT:0:7}"; dirty=false
case "${FIXTURE_HOST_STATE:-good}" in
wrong-version) version=9.9.9 ;;
wrong-commit) commit=deadbee ;;
dirty) dirty=true ;;
esac
printf 'chanvoy %s\nCommit: %s\nDirty: %s\n' "$version" "$commit" "$dirty"
SH
done
cat >"$scratch/bin/gh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$FIXTURE_LOG"
[[ "${FIXTURE_API_FAILURE:-0}" == 0 ]] || exit 1
case "$1 $2" in
"api repos/lanytehq/chanvoy/git/ref/tags/v1.2.3")
    jq -cn --arg object "$FIXTURE_OBJECT" '{object:{type:"tag",sha:$object}}' ;;
"api repos/lanytehq/chanvoy/actions/runs/11")
    jq -cn --arg commit "$FIXTURE_COMMIT" '{id:11,head_sha:$commit,head_branch:"v1.2.3",event:"push",
      status:"completed",conclusion:"success",path:".github/workflows/release.yml",repository:{full_name:"lanytehq/chanvoy"}}' ;;
"api --paginate")
    if [[ -f "$FIXTURE_STATE" ]]; then printf '[[{"tag_name":"v1.2.3"}]]\n'; else printf '[[]]\n'; fi ;;
"repo view") printf 'lanytehq/chanvoy\n' ;;
"run list")
    if [[ -n "${FIXTURE_RUNS+x}" ]]; then printf '%s\n' "$FIXTURE_RUNS"; else
      jq -cn --arg commit "$FIXTURE_COMMIT" '[{databaseId:11,headSha:$commit,conclusion:"success",event:"push"}]'
    fi ;;
"run download" | "release download")
    destination=''
    while [[ $# -gt 0 ]]; do
      if [[ "$1" == --dir ]]; then destination="$2"; shift; fi
      shift
    done
    [[ -n "$destination" ]]
    if [[ "$FIXTURE_OPERATION" == fetch ]]; then source="$FIXTURE_BUILD"; else source="$FIXTURE_REMOTE"; fi
    for asset in "$source"/*; do cp "$asset" "$destination/"; done ;;
"release view")
    [[ -f "$FIXTURE_STATE" ]]
    jq -cn --arg tag v1.2.3 --arg commit "$FIXTURE_COMMIT" --arg draft "$(cat "$FIXTURE_STATE")" \
      --argjson assets "$(find "$FIXTURE_REMOTE" -type f -maxdepth 1 -exec basename {} \; | jq -Rsc 'split("\n")[:-1] | map({name:.})')" \
      '{tagName:$tag,targetCommitish:$commit,isDraft:($draft=="true"),assets:$assets}' ;;
"release create" | "release upload")
    operation="$2"; shift 3
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --repo | --target | --title | --notes-file) shift 2 ;;
        --verify-tag | --draft) shift ;;
        --clobber) exit 99 ;;
        *) [[ -f "$1" && ! -e "$FIXTURE_REMOTE/$(basename "$1")" ]]; cp "$1" "$FIXTURE_REMOTE/"; shift ;;
      esac
    done
    [[ "$operation" != create ]] || printf 'true\n' >"$FIXTURE_STATE" ;;
"release edit")
    [[ "$*" == *'--draft=false'* && "$(cat "$FIXTURE_STATE")" == true ]]
    printf 'false\n' >"$FIXTURE_STATE" ;;
*) exit 1 ;;
esac
SH
chmod +x "$scratch/bin/gh"
export PATH="$scratch/bin:$PATH" FIXTURE_OPERATION=fetch
reject() {
	if "$@" >"$scratch/rejected" 2>&1; then
		echo "error: expected ceremony rejection: ${2##*/}" >&2; exit 1;
	fi
}
git init -q --bare -b main "$scratch/remote.git"
git -C "$repo" remote add origin "$scratch/remote.git"
git -C "$repo" push -q origin main
git -C "$repo" remote set-url origin https://github.com/lanytehq/chanvoy.git
git -C "$repo" config url."$scratch/remote.git".insteadOf https://github.com/lanytehq/chanvoy.git
export CHANVOY_TAGGER_NAME='3 Leaps Infosec Team' CHANVOY_TAGGER_EMAIL=infosec@3leaps.net
export CHANVOY_TAG_MESSAGE_DIR="$scratch/v1.2.3"
bash "$repo/scripts/release-tag-operator.sh" prepare-message >/dev/null
before_tags="$(git -C "$repo" show-ref --tags || true)"
bash "$repo/scripts/release-tag-operator.sh" preflight >/dev/null
[[ "$(git -C "$repo" show-ref --tags || true)" == "$before_tags" ]]
echo '[test] actual local tag, local verify, bare-remote push and remote verify entrypoints'
CHANVOY_TAGGER_NAME=unapproved reject bash "$repo/scripts/release-tag.sh"
CHANVOY_PGP_KEY_ID="$primary!" reject bash "$repo/scripts/release-tag.sh"
bash "$repo/scripts/release-tag-operator.sh" local-tag >/dev/null
FIXTURE_OBJECT="$(git -C "$repo" rev-parse refs/tags/v1.2.3)"
export FIXTURE_OBJECT
[[ "$(git -C "$repo" cat-file -t "$FIXTURE_OBJECT")" == tag ]]
[[ "$(git -C "$repo" rev-parse "$FIXTURE_OBJECT^{}")" == "$FIXTURE_COMMIT" ]]
(cd "$repo"; bash scripts/release-tag-body.sh "$FIXTURE_OBJECT") >"$scratch/actual-tag-message"
cmp -s "$scratch/actual-tag-message" "$CHANVOY_TAG_MESSAGE_DIR/message.txt"
bash "$repo/scripts/release-verify-tag.sh" >/dev/null
cp "$CHANVOY_TAG_MESSAGE_DIR/message.txt" "$scratch/approved-message"
printf 'Unreviewed message\n' >"$CHANVOY_TAG_MESSAGE_DIR/message.txt"
reject bash "$repo/scripts/release-verify-tag.sh"
mv "$scratch/approved-message" "$CHANVOY_TAG_MESSAGE_DIR/message.txt"
reject bash "$repo/scripts/release-tag.sh"
[[ "$(git -C "$repo" rev-parse refs/tags/v1.2.3)" == "$FIXTURE_OBJECT" ]]
bash "$repo/scripts/release-push-tag.sh" >/dev/null
[[ "$(git -C "$scratch/remote.git" rev-parse refs/tags/v1.2.3)" == "$FIXTURE_OBJECT" ]]
bash "$repo/scripts/release-verify-remote-tag.sh" >/dev/null
FIXTURE_API_FAILURE=1 reject bash "$repo/scripts/release-verify-remote-tag.sh"
reject bash "$repo/scripts/release-push-tag.sh"
reject bash "$repo/scripts/release-tag.sh"
[[ "$(git -C "$scratch/remote.git" rev-parse refs/tags/v1.2.3)" == "$FIXTURE_OBJECT" ]]
[[ "$(git -C "$repo" rev-parse refs/tags/v1.2.3)" == "$FIXTURE_OBJECT" ]]
printf '2.0.0\n' >"$repo/VERSION"
printf 'Later main notes\n' >"$repo/docs/releases/v1.2.3.md"
printf 'Later main text anchors\n' >"$repo/keys/expected-fingerprints.txt"
printf 'Later main NDJSON anchors\n' >"$repo/keys/expected-fingerprints.ndjson"
git -C "$repo" add VERSION docs/releases/v1.2.3.md keys
git -C "$repo" commit -qm 'advance main after tag'
no_remote_mutation() {
	if grep -Eq '^release (create|upload|edit) ' "$FIXTURE_LOG"; then
		echo 'error: remote mutation happened despite rejection' >&2; exit 1;
	fi
}
directory="$scratch/staged"
echo '[test] exact workflow artifact staging'
FIXTURE_RUNS='[]' reject bash "$repo/scripts/release-fetch-ci-artifacts.sh" v1.2.3 "$directory"
FIXTURE_TAG_REPLACED=1 reject bash "$repo/scripts/release-fetch-ci-artifacts.sh" v1.2.3 "$directory"
bash "$repo/scripts/release-fetch-ci-artifacts.sh" v1.2.3 "$directory" >/dev/null
bash "$repo/scripts/stage-release-anchors.sh" "$directory" >/dev/null
bash "$repo/scripts/generate-checksums.sh" "$directory" >/dev/null
echo '[test] draft creation and mandatory dual-format signing'
FIXTURE_API_FAILURE=1 reject bash "$repo/scripts/release-create-draft.sh" v1.2.3 "$directory"
bash "$repo/scripts/release-create-draft.sh" v1.2.3 "$directory" >/dev/null
reject bash "$repo/scripts/release-create-draft.sh" v1.2.3 "$directory"
# Create draft before signing, then mandate all four signatures for later stages.
bash "$repo/scripts/sign-release-assets.sh" "$directory" >/dev/null
bash "$repo/scripts/export-release-keys.sh" "$directory" >/dev/null
bash "$repo/scripts/verify-signatures.sh" "$directory" >/dev/null
echo '[test] legacy single-platform consumer verification commands'
consumer="$scratch/single-platform"
mkdir "$consumer"
for asset in chanvoy-v1.2.3-linux-x86_64 chanvoy-v1.2.3-linux-x86_64.minisig checksums.txt checksums.txt.asc chanvoy.pub chanvoy.gpg.asc; do
	cp "$directory/$asset" "$consumer/"
done
minisign -Vm "$consumer/chanvoy-v1.2.3-linux-x86_64" -p "$consumer/chanvoy.pub" >/dev/null 2>&1
gpg --homedir "$GNUPGHOME" --batch --verify "$consumer/checksums.txt.asc" "$consumer/checksums.txt" >/dev/null 2>&1
# GNU sha256sum is the documented consumer command (gsha256sum on macOS).
checksum_bin="$(command -v sha256sum || command -v gsha256sum)"
(cd "$consumer"; "$checksum_bin" --ignore-missing -c checksums.txt) >/dev/null
echo '[test] reject valid manifest signatures made by the pinned primary'
for manifest in SHA256SUMS SHA512SUMS; do
	mv "$directory/$manifest.asc" "$scratch/permitted-subkey-signature"
	gpg --homedir "$GNUPGHOME" --batch --armor --detach-sign --local-user "$primary!" \
		--output "$directory/$manifest.asc" "$directory/$manifest" >/dev/null 2>&1
	# Prove this is a cryptographically valid signature by the approved primary,
	# not merely a corrupt signature or an unusable cert-only fixture key.
	gpg --homedir "$GNUPGHOME" --batch --status-fd 1 --verify \
		"$directory/$manifest.asc" "$directory/$manifest" >"$scratch/primary-status" 2>/dev/null
	awk -v primary="$primary" '
      $1=="[GNUPG:]" && $2=="VALIDSIG" {valid++; signer=$3}
      $1=="[GNUPG:]" && $2=="GOODSIG" {good++}
      END {if (good!=1 || valid!=1 || signer!=primary) exit 1}
    ' "$scratch/primary-status"
	reject bash "$repo/scripts/verify-signatures.sh" "$directory"
	grep -q 'manifest signer, expiry or revocation check failed' "$scratch/rejected"
	mv "$scratch/permitted-subkey-signature" "$directory/$manifest.asc"
done
bash "$repo/scripts/verify-signatures.sh" "$directory" >/dev/null
echo '[test] missing/invalid signature and public-key negatives'
export FIXTURE_OPERATION=remote CHANVOY_CONFIRM_PUBLISH=v1.2.3
for signature in SHA256SUMS.minisig SHA512SUMS.minisig SHA256SUMS.asc SHA512SUMS.asc checksums.txt.asc \
	chanvoy-v1.2.3-linux-x86_64.minisig chanvoy-v1.2.3-linux-aarch64.minisig chanvoy-v1.2.3-macos-aarch64.minisig; do
	mv "$directory/$signature" "$scratch/saved-signature"
	: >"$FIXTURE_LOG"
	reject bash "$repo/scripts/verify-signatures.sh" "$directory"
	reject bash "$repo/scripts/upload-release-assets.sh" "$directory"
	reject bash "$repo/scripts/release-verify-draft.sh" "$directory"
	reject bash "$repo/scripts/release-publish.sh" "$directory"
	no_remote_mutation
	mv "$scratch/saved-signature" "$directory/$signature"
done
for signature in SHA256SUMS.asc SHA512SUMS.minisig; do
	cp "$directory/$signature" "$scratch/valid-signature"
	printf 'bad signature\n' >"$directory/$signature"
	: >"$FIXTURE_LOG"
	reject bash "$repo/scripts/upload-release-assets.sh" "$directory"
	reject bash "$repo/scripts/release-publish.sh" "$directory"
	no_remote_mutation
	mv "$scratch/valid-signature" "$directory/$signature"
done
minisign -G -W -s "$scratch/other.key" -p "$scratch/other.pub" >/dev/null 2>&1
cp "$directory/chanvoy.pub" "$scratch/good.pub"
cp "$scratch/other.pub" "$directory/chanvoy.pub"
reject bash "$repo/scripts/verify-public-keys.sh" "$directory"
mv "$scratch/good.pub" "$directory/chanvoy.pub"
: >"$FIXTURE_LOG"
FIXTURE_TAG_REPLACED=1 reject bash "$repo/scripts/upload-release-assets.sh" "$directory"
no_remote_mutation
bash "$repo/scripts/upload-release-assets.sh" "$directory" >/dev/null
echo '[test] fresh-download and promotion negatives'
# Every publication consumer must retain the complete receipt and metadata gates.
consumers=(upload-release-assets.sh release-verify-draft.sh release-publish.sh verify-signatures.sh)
mv "$directory.anchor" "$scratch/saved-receipt"
for consumer in "${consumers[@]}"; do
	: >"$FIXTURE_LOG"
	reject bash "$repo/scripts/$consumer" "$directory"
	no_remote_mutation
done
mv "$scratch/saved-receipt" "$directory.anchor"
for asset in chanvoy-v1.2.3-linux-x86_64 release-notes-v1.2.3.md expected-fingerprints.txt expected-fingerprints.ndjson; do
	cp "$directory/$asset" "$scratch/saved-asset"
	printf 'changed asset\n' >>"$directory/$asset"
	for consumer in "${consumers[@]}"; do
		: >"$FIXTURE_LOG"
		reject bash "$repo/scripts/$consumer" "$directory"
		no_remote_mutation
	done
	mv "$scratch/saved-asset" "$directory/$asset"
done
printf 'unapproved\n' >"$directory/extra-asset"
for consumer in "${consumers[@]}"; do reject bash "$repo/scripts/$consumer" "$directory"; done
rm "$directory/extra-asset"
# An already uploaded or published set is never clobbered on a retry.
: >"$FIXTURE_LOG"
reject bash "$repo/scripts/upload-release-assets.sh" "$directory"
no_remote_mutation
cp "$FIXTURE_REMOTE/chanvoy-v1.2.3-linux-x86_64" "$scratch/good-binary"
printf 'remote tamper\n' >>"$FIXTURE_REMOTE/chanvoy-v1.2.3-linux-x86_64"
: >"$FIXTURE_LOG"
reject bash "$repo/scripts/release-publish.sh" "$directory"
no_remote_mutation
if grep -q '^host-executed$' "$FIXTURE_LOG"; then
	echo 'error: unauthenticated remote binary was executed' >&2; exit 1;
fi
mv "$scratch/good-binary" "$FIXTURE_REMOTE/chanvoy-v1.2.3-linux-x86_64"
bash "$repo/scripts/release-verify-draft.sh" "$directory" >/dev/null
: >"$FIXTURE_LOG"
CHANVOY_CONFIRM_PUBLISH='' reject bash "$repo/scripts/release-publish.sh" "$directory"
no_remote_mutation
for state in wrong-version wrong-commit dirty; do
	: >"$FIXTURE_LOG"
	FIXTURE_HOST_STATE="$state" reject bash "$repo/scripts/release-publish.sh" "$directory"
	grep -q '^host-executed$' "$FIXTURE_LOG"
	no_remote_mutation
done
bash "$repo/scripts/release-publish.sh" "$directory" >/dev/null
[[ "$(cat "$FIXTURE_STATE")" == false ]]
reject bash "$repo/scripts/upload-release-assets.sh" "$directory"
echo '[ok] disposable fetch/draft/dual-signature/upload/fresh-download/promotion ceremony'
