#!/usr/bin/env bash
# Synthetic short-lived fixture. No operator keyring or real ceremony keys.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/chanvoy-pinned-tag.XXXXXX")"
cleanup() {
	local home
	for home in "$scratch/gpg" "$scratch/expired-ring"; do
		[[ -d "$home" ]] && gpgconf --homedir "$home" --kill all >/dev/null 2>&1 || true
	done
	rm -rf "$scratch"
}
trap cleanup EXIT
export GNUPGHOME="$scratch/gpg"
export CHANVOY_DECERNOR_BIN="${CHANVOY_DECERNOR_BIN:-$(command -v decernor)}"
unset CHANVOY_PGP_KEY_ID CHANVOY_GPG_SIGNING_FINGERPRINT
mkdir -m 700 "$GNUPGHOME"
gpg --batch --pinentry-mode loopback --passphrase '' --quick-generate-key \
	'Synthetic release <synthetic@example.invalid>' ed25519 sign 1d >/dev/null 2>&1
primary="$(gpg --batch --with-colons --fingerprint --list-keys | awk -F: '$1=="fpr" {print $10;exit}')"
gpg --batch --pinentry-mode loopback --passphrase '' --quick-add-key "$primary" ed25519 sign 1d >/dev/null 2>&1
subkey="$(gpg --batch --with-colons --with-subkey-fingerprint --list-keys "$primary" | awk -F: '$1=="sub" {s=1;next} s && $1=="fpr" {print $10;exit}')"
fixture="$scratch/repo"
git init -q -b main "$fixture"
mkdir -p "$fixture/docs/security" "$fixture/keys" "$fixture/scripts" "$fixture/config/release" "$fixture/schemas/release/v0"
cp "$root/scripts/"{verify-pinned-tag.sh,validate-release-anchors.sh,release-decernor.sh} "$fixture/scripts/"
cp "$root/config/release/tagger-identity.txt" "$fixture/config/release/"
cp "$root/schemas/release/v0/fingerprint-record.schema.json" "$fixture/schemas/release/v0/"
gpg --batch --armor --export "$primary" >"$fixture/docs/security/release-signing-keys.asc"
printf 'gpg %s\nminisign %064d\n' "$primary" 0 >"$fixture/keys/expected-fingerprints.txt"
"$CHANVOY_DECERNOR_BIN" fingerprint "$fixture/docs/security/release-signing-keys.asc" \
	--kind gpg --class public --gpg-role primary --path-mode none --format ndjson >"$fixture/keys/expected-fingerprints.ndjson"
python3 - "$fixture/keys/expected-fingerprints.ndjson" <<'PY'
import json
import pathlib
import sys
p = pathlib.Path(sys.argv[1])
with p.open('a') as f:
    f.write(json.dumps(dict(schema_version='v0', kind='minisign', **{'class': 'public'},
        algorithm='sha256', fingerprint='0' * 64,
        fingerprint_scheme='minisign-public-blob-sha256-v1', confidence='high')) + '\n')
PY
printf '1.2.3\n' >"$fixture/VERSION"
printf 'fixture\n' >"$fixture/file"
printf 'Fixture release\n' >"$scratch/message.txt"
git -C "$fixture" config user.name '3 Leaps Infosec Team'
git -C "$fixture" config user.email infosec@3leaps.net
git -C "$fixture" add .
git -C "$fixture" commit -qm fixture
good="$(git -C "$fixture" rev-parse HEAD)"
export CHANVOY_RELEASE_TAG=v1.2.3
expect_fail() {
	if "$@" >"$scratch/output" 2>&1; then
		echo 'error: expected signature control rejection' >&2
		exit 1
	fi
}
verify() { bash "$fixture/scripts/verify-pinned-tag.sh" "$@"; }
retag() {
	GIT_COMMITTER_NAME='3 Leaps Infosec Team' GIT_COMMITTER_EMAIL=infosec@3leaps.net \
		git -C "$fixture" tag -fs -a --cleanup=verbatim -u "$1" -F "$scratch/message.txt" "$CHANVOY_RELEASE_TAG" >/dev/null
}
commit_and_expect_fail() {
	git -C "$fixture" add -A
	git -C "$fixture" commit -qm mutated --allow-empty
	retag "$subkey!"
	expect_fail verify
	# Only committed disposable fixture state; no reset or discard operation.
	git -C "$fixture" switch --quiet --detach "$good"
	retag "$subkey!"
}
retag "$subkey!"
verify >/dev/null
CHANVOY_PGP_KEY_ID="$subkey!" verify >/dev/null
CHANVOY_GPG_SIGNING_FINGERPRINT="$primary" verify --published >/dev/null
expect_fail verify --published
expect_fail env CHANVOY_GPG_SIGNING_FINGERPRINT="$(printf '%040d' 0)" \
	bash "$fixture/scripts/verify-pinned-tag.sh" --published
cp "$fixture/keys/expected-fingerprints.txt" "$scratch/anchors.good"
cp "$fixture/keys/expected-fingerprints.ndjson" "$scratch/records.good"
cp "$fixture/docs/security/release-signing-keys.asc" "$scratch/approved.asc"
printf 'gpg %s\nminisign %064d\n' "$(printf '%040d' 0)" 0 >"$fixture/keys/expected-fingerprints.txt"
commit_and_expect_fail
printf '{}\n' >"$fixture/keys/expected-fingerprints.ndjson"
commit_and_expect_fail
rm "$fixture/keys/expected-fingerprints.ndjson"
commit_and_expect_fail
rm "$fixture/docs/security/release-signing-keys.asc"
commit_and_expect_fail
printf '9.9.9\n' >"$fixture/VERSION"
commit_and_expect_fail
printf 'Other <other@example.invalid>\n' >"$fixture/config/release/tagger-identity.txt"
commit_and_expect_fail
# Working tree tampering is irrelevant: only tagged inert trust data is read.
printf 'tampered\n' >"$fixture/keys/expected-fingerprints.txt"
rm "$fixture/docs/security/release-signing-keys.asc"
verify >/dev/null
cp "$scratch/anchors.good" "$fixture/keys/expected-fingerprints.txt"
cp "$scratch/approved.asc" "$fixture/docs/security/release-signing-keys.asc"
# Main may advance and even contain a malicious tagged-helper replacement.
printf '#!/usr/bin/env bash\nexit 99\n' >"$fixture/scripts/validate-release-anchors.sh"
printf '2.0.0\n' >"$fixture/VERSION"
git -C "$fixture" add .
git -C "$fixture" commit -qm advance
expect_fail verify
# Trusted verifier must remain trusted: preserve the earlier reviewed scripts.
cp "$root/scripts/validate-release-anchors.sh" "$fixture/scripts/"
CHANVOY_GPG_SIGNING_FINGERPRINT="$primary" verify --published >/dev/null
cp "$fixture/scripts/validate-release-anchors.sh" "$scratch/trusted-validator"
git -C "$fixture" add .
git -C "$fixture" commit -qm 'restore trusted verifier'
git -C "$fixture" switch --quiet --detach "$good"
retag "$subkey!"
gpg --batch --pinentry-mode loopback --passphrase '' --quick-generate-key \
	'Extra synthetic <extra@example.invalid>' ed25519 cert 1d >/dev/null 2>&1
extra="$(gpg --batch --with-colons --fingerprint --list-keys 'Extra synthetic' | awk -F: '$1=="fpr" {print $10;exit}')"
gpg --batch --armor --export "$extra" >>"$fixture/docs/security/release-signing-keys.asc"
commit_and_expect_fail
gpg --batch --armor --export "$extra" >"$fixture/docs/security/release-signing-keys.asc"
commit_and_expect_fail
gpg --batch --pinentry-mode loopback --passphrase '' --quick-add-key "$primary" ed25519 sign 1d >/dev/null 2>&1
second="$(gpg --batch --with-colons --with-subkey-fingerprint --list-keys "$primary" |
	awk -F: -v first="$subkey" '$1=="sub" {s=1;cap=$12;next} s && $1=="fpr" {if (cap ~ /s/ && $10!=first) print $10; s=0}' | tail -1)"
gpg --batch --armor --export "$primary" >"$fixture/docs/security/release-signing-keys.asc"
commit_and_expect_fail
retag "$second!"
expect_fail verify
retag "$primary!"
expect_fail verify
retag "$subkey!"
verify >/dev/null
real_gpg="$(command -v gpg)"
mkdir "$scratch/bin"
cat >"$scratch/bin/gpg" <<'SH'
#!/usr/bin/env bash
if [[ -n "${FIXTURE_FUTURE_TIME:-}" ]]; then
	exec "$FIXTURE_REAL_GPG" --faked-system-time "$FIXTURE_FUTURE_TIME" "$@"
fi
exec "$FIXTURE_REAL_GPG" "$@"
SH
chmod +x "$scratch/bin/gpg"
export FIXTURE_REAL_GPG="$real_gpg" PATH="$scratch/bin:$PATH"
export FIXTURE_FUTURE_TIME="$(($(date +%s) + 172800))"
expect_fail verify
unset FIXTURE_FUTURE_TIME
verify >/dev/null
git -C "$fixture" tag -d "$CHANVOY_RELEASE_TAG" >/dev/null
git -C "$fixture" tag "$CHANVOY_RELEASE_TAG"
expect_fail verify
git -C "$fixture" tag -d "$CHANVOY_RELEASE_TAG" >/dev/null
git -C "$fixture" tag -a -m unsigned "$CHANVOY_RELEASE_TAG"
expect_fail verify
echo '[ok] isolated pinned tag, pair, signer, expiry and advancing-main controls'
