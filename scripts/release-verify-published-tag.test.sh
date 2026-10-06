#!/usr/bin/env bash
# Real signed local bare remote; GitHub REST is stubbed. No live network/keyring.
set -euo pipefail
trap 'echo "error: disposable published-tag test failed at line $LINENO" >&2' ERR
root="$(cd "$(dirname "$0")/.." && pwd -P)"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/pg-pub.XXXXXX")"
scratch="$(cd "$scratch" && pwd -P)"
cleanup() {
	gpgconf --homedir "$scratch/gpg" --kill all >/dev/null 2>&1 || true
	rm -rf "$scratch"
}
trap cleanup EXIT
export GNUPGHOME="$scratch/gpg"
mkdir -m 700 "$GNUPGHOME"
export CHANVOY_DECERNOR_BIN="${CHANVOY_DECERNOR_BIN:-$(command -v decernor)}"
gpg --batch --pinentry-mode loopback --passphrase '' --quick-generate-key \
	'Synthetic published gate <synthetic@example.invalid>' ed25519 cert 1d >/dev/null 2>&1
primary="$(gpg --batch --with-colons --fingerprint --list-keys | awk -F: '$1=="fpr" {print $10;exit}')"
gpg --batch --pinentry-mode loopback --passphrase '' --quick-add-key "$primary" ed25519 sign 1d >/dev/null 2>&1
subkey="$(gpg --batch --with-colons --with-subkey-fingerprint --list-keys | awk -F: '$1=="sub" {s=1;next} s && $1=="fpr" {print $10;exit}')"
export CHANVOY_RELEASE_TAG=v1.2.3 CHANVOY_GPG_SIGNING_FINGERPRINT="$primary"
unset GH_REPO CHANVOY_PGP_KEY_ID CHANVOY_EXPECTED_COMMIT CHANVOY_EXPECTED_TAG_OBJECT
unset CHANVOY_ANCHOR_OUT CHANVOY_APPROVED_ENV_LOADER RELEASE_TAG CHANVOY_REQUIRE_TAG
git init -q --bare -b main "$scratch/remote.git"
src="$scratch/source"
git init -q -b main "$src"
git -C "$src" config user.name '3 Leaps Infosec Team'
git -C "$src" config user.email infosec@3leaps.net
mkdir -p "$src/scripts" "$src/config/release" "$src/keys" "$src/docs/security" "$src/schemas/release/v0"
cp "$root/scripts/"{release-verify-published-tag.sh,verify-pinned-tag.sh,validate-release-anchors.sh,release-decernor.sh} "$src/scripts/"
cp "$root/config/release/tagger-identity.txt" "$src/config/release/"
cp "$root/schemas/release/v0/fingerprint-record.schema.json" "$src/schemas/release/v0/"
printf '1.2.3\n' >"$src/VERSION"
gpg --batch --armor --export "$primary" >"$src/docs/security/release-signing-keys.asc"
printf 'gpg %s\nminisign %064d\n' "$primary" 0 >"$src/keys/expected-fingerprints.txt"
"$CHANVOY_DECERNOR_BIN" fingerprint "$src/docs/security/release-signing-keys.asc" --class public --kind gpg \
	--gpg-role primary --path-mode none --format ndjson >"$src/keys/expected-fingerprints.ndjson"
python3 - "$src/keys/expected-fingerprints.ndjson" <<'PY'
import json
import pathlib
import sys
with pathlib.Path(sys.argv[1]).open('a') as f:
    f.write(json.dumps(dict(schema_version='v0', kind='minisign', **{'class': 'public'},
        algorithm='sha256', fingerprint='0' * 64,
        fingerprint_scheme='minisign-public-blob-sha256-v1', confidence='high')) + '\n')
PY
git -C "$src" add .
git -C "$src" commit -qm 'synthetic published fixture'
commit="$(git -C "$src" rev-parse HEAD)"
sign_tag() {
	printf '%s\n' "$1" >"$scratch/message"
	GIT_COMMITTER_NAME='3 Leaps Infosec Team' GIT_COMMITTER_EMAIL=infosec@3leaps.net \
		git -C "$src" tag -fs -a --cleanup=verbatim -u "$subkey!" -F "$scratch/message" v1.2.3 >/dev/null
}
sign_tag 'Synthetic release'
git -C "$src" remote add origin "$scratch/remote.git"
git -C "$src" push -q origin main refs/tags/v1.2.3
op="$scratch/operator"
git clone -q "$scratch/remote.git" "$op"
git -C "$op" config user.name fixture
git -C "$op" config user.email fixture@example.invalid
printf '2.0.0\n' >"$op/VERSION"
git -C "$op" add VERSION
git -C "$op" commit -qm 'advance main'
git -C "$op" remote set-url origin https://github.com/lanytehq/chanvoy.git
git -C "$op" config url."$scratch/remote.git".insteadOf https://github.com/lanytehq/chanvoy.git
mkdir "$scratch/bin"
cat >"$scratch/bin/gh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
[[ "${FIXTURE_API_FAILURE:-0}" == 0 ]]
object="${2##*/}"
commit="$(git -C "$FIXTURE_REMOTE" rev-parse "$object^{}")"
jq -cn --arg commit "$commit" --arg tag "${FIXTURE_GH_TAG:-v1.2.3}" \
    --argjson verified "${FIXTURE_GH_VERIFIED:-true}" --arg reason "${FIXTURE_GH_REASON:-valid}" \
    '{tag:$tag,object:{type:"commit",sha:$commit},verification:{verified:$verified,reason:$reason}}'
SH
chmod +x "$scratch/bin/gh"
export PATH="$scratch/bin:$PATH" FIXTURE_REMOTE="$scratch/remote.git"
gate() { bash "$op/scripts/release-verify-published-tag.sh"; }
reject() {
	if "$@" >"$scratch/rejected" 2>&1; then echo 'error: expected published-tag rejection' >&2; exit 1; fi
}
echo '[test] published gate from advanced main'
gate >/dev/null
[[ "$(git -C "$op" worktree list --porcelain | grep -c '^worktree ')" == 1 ]]
FIXTURE_GH_VERIFIED=false reject gate
FIXTURE_GH_REASON=unsigned reject gate
FIXTURE_GH_TAG=v9.9.9 reject gate
FIXTURE_API_FAILURE=1 reject gate
GH_REPO=other/repository reject gate
RELEASE_TAG=v9.9.9 reject gate
CHANVOY_GPG_SIGNING_FINGERPRINT='' reject gate
CHANVOY_GPG_SIGNING_FINGERPRINT="$(printf '%040d' 0)" reject gate
# A host SSH alias is accepted only when its effective hostname is GitHub.
# Git traffic remains redirected to the same disposable bare remote.
cat >"$scratch/bin/ssh" <<'SH'
#!/usr/bin/env bash
[[ "$*" == '-G synthetic-github' ]] || exit 1
printf 'hostname %s\n' "${FIXTURE_SSH_HOST:-github.com}"
SH
chmod +x "$scratch/bin/ssh"
git -C "$op" config url."$scratch/remote.git".insteadOf git@synthetic-github:lanytehq/chanvoy.git
git -C "$op" remote set-url origin git@synthetic-github:lanytehq/chanvoy.git
gate >/dev/null
FIXTURE_SSH_HOST=other.invalid reject gate
git -C "$op" remote set-url origin https://github.com/lanytehq/chanvoy.git
git -C "$op" config url."$scratch/remote.git".insteadOf https://github.com/lanytehq/chanvoy.git
# Same commit replacement must bind the actual annotated object, not just SHA^{}.
echo '[test] immutable object anchor and signed replacement'
CHANVOY_ANCHOR_OUT="$scratch/anchor" gate >/dev/null
anchor_object="$(awk -F= '$1=="object" {print $2}' "$scratch/anchor")"
anchor_commit="$(awk -F= '$1=="commit" {print $2}' "$scratch/anchor")"
anchored() { CHANVOY_EXPECTED_TAG_OBJECT="$anchor_object" CHANVOY_EXPECTED_COMMIT="$anchor_commit" gate; }
anchored >/dev/null
sign_tag 'Different reviewed bytes, same commit'
git -C "$src" push -q --force origin refs/tags/v1.2.3
before="$(git -C "$op" rev-parse refs/tags/v1.2.3)"
reject anchored
[[ "$(git -C "$op" rev-parse refs/tags/v1.2.3)" == "$before" ]]
CHANVOY_EXPECTED_TAG_OBJECT="$anchor_object" reject gate
gate >/dev/null
# Lightweight and unsigned replacements never pass the unanchored gate either.
echo '[test] lightweight/unsigned replacements'
git -C "$scratch/remote.git" update-ref refs/tags/v1.2.3 "$commit"
reject gate
git -C "$src" tag -fa -m unsigned v1.2.3 >/dev/null
git -C "$src" push -q --force origin refs/tags/v1.2.3
reject gate
sign_tag 'Synthetic restored'
git -C "$src" push -q --force origin refs/tags/v1.2.3
gate >/dev/null
# Even a correctly signed target cannot execute its own verifier/helper code.
echo '[test] inert hostile tagged scripts'
marker="$scratch/target-code-executed"
for script in release-verify-published-tag.sh verify-pinned-tag.sh validate-release-anchors.sh release-decernor.sh; do
	printf '#!/usr/bin/env bash\ntouch %q\nexit 0\n' "$marker" >"$src/scripts/$script"
done
git -C "$src" add .
git -C "$src" commit -qm 'synthetic hostile tagged helpers'
sign_tag 'Signed inert target'
git -C "$src" push -q origin main
git -C "$src" push -q --force origin refs/tags/v1.2.3
reject anchored
gate >/dev/null
[[ ! -e "$marker" ]]
# A conflicting target VERSION cannot be accepted just because the signature is valid.
printf '9.9.9\n' >"$src/VERSION"
git -C "$src" add VERSION
git -C "$src" commit -qm 'synthetic conflicting target version'
sign_tag 'Wrong cut'
git -C "$src" push -q --force origin refs/tags/v1.2.3
reject gate
git -C "$scratch/remote.git" update-ref refs/tags/v1.2.3 "$anchor_object"
anchored >/dev/null
git -C "$scratch/remote.git" update-ref -d refs/tags/v1.2.3
reject gate
echo '[ok] REST status, independent primary, replaced object/target and inert-tag controls'
