#!/usr/bin/env bash
# Signed-tag ceremony invariants; no signing material belongs in this checkout.
set -euo pipefail
tag_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
tag_die() { echo "error: $*" >&2; return 1; }
tag_identity() {
	[[ "${CHANVOY_TAGGER_NAME:-} <${CHANVOY_TAGGER_EMAIL:-}>" == "$(cat "$tag_root/config/release/tagger-identity.txt")" ]] || {
		tag_die 'approved public tagger identity required'; return 1;
	}
}
tag_version() {
	[[ -n "${CHANVOY_RELEASE_TAG:-}" ]] || { tag_die 'CHANVOY_RELEASE_TAG required'; return 1; }
	bash "$tag_root/scripts/release-guard-tag-version.sh" >/dev/null
}
tag_message_file() {
	local file
	file="$(python3 - "$tag_root" "${CHANVOY_TAG_MESSAGE_DIR:-}" "$CHANVOY_RELEASE_TAG" <<'PY'
import pathlib
import sys
root = pathlib.Path(sys.argv[1])
raw, tag = sys.argv[2:]
p = pathlib.Path(raw)
if not raw or not p.is_absolute() or '..' in p.parts or p.name != tag:
    raise SystemExit('error: absolute external per-cut message directory required')
if any(a.is_symlink() for a in (p, *p.parents)) or not p.is_dir() or p.resolve().is_relative_to(root):
    raise SystemExit('error: unsafe message directory')
file = p / 'message.txt'
if not file.is_file() or file.is_symlink() or not file.stat().st_size:
    raise SystemExit('error: regular nonempty message.txt required')
print(file)
PY
	)" || return 1
	python3 "$tag_root/scripts/release-prepare-tag-message.py" --validate-message "$file" || return 1
	printf '%s\n' "$file"
}
tag_expected_message() {
	local file
	file="$(tag_message_file)" || return 1
	cat "$file"
}
tag_origin() {
	local url host
	url="$(git config --get remote.origin.url)" || return 1
	case "$url" in
		https://github.com/lanytehq/chanvoy | https://github.com/lanytehq/chanvoy.git | git@github.com:lanytehq/chanvoy | git@github.com:lanytehq/chanvoy.git) ;;
		git@*:lanytehq/chanvoy | git@*:lanytehq/chanvoy.git)
			host="${url#git@}"; host="${host%%:*}"
			[[ "$(ssh -G "$host" 2>/dev/null | awk '$1=="hostname" {print $2;exit}')" == github.com ]] || { tag_die 'origin SSH alias must resolve to github.com'; return 1; } ;;
		*) tag_die 'origin must be lanytehq/chanvoy on GitHub'; return 1 ;;
	esac
}
tag_checkout() {
	cd "$tag_root" || return 1
	tag_origin || return 1
	git fetch --quiet origin '+refs/heads/main:refs/remotes/origin/main' || return 1
	[[ "$(git symbolic-ref --quiet --short HEAD)" == main &&
		-z "$(git status --porcelain --untracked-files=all)" &&
		"$(git rev-parse HEAD)" == "$(git rev-parse refs/remotes/origin/main)" ]] || {
		tag_die 'clean main matching fetched origin/main required for tag creation'; return 1;
	}
}
tag_selector_shape() {
	[[ "${CHANVOY_PGP_KEY_ID:-}" =~ ^[0-9A-F]{40}!$ &&
		"${CHANVOY_GPG_SIGNING_FINGERPRINT:-}" =~ ^[0-9A-F]{40}$ ]] || {
		tag_die 'approved primary and exact uppercase signing subkey with ! required'; return 1;
	}
}
tag_key_selector() {
	tag_selector_shape || return 1
	bash "$tag_root/scripts/release-validate-pin.sh" >/dev/null || return 1
	bash "$tag_root/scripts/validate-release-anchors.sh" >/dev/null || return 1
	[[ "$(awk '$1=="gpg" {print $2}' "$tag_root/keys/expected-fingerprints.txt")" == "$CHANVOY_GPG_SIGNING_FINGERPRINT" ]] || {
		tag_die 'operator primary differs from reviewed anchor'; return 1;
	}
	python3 - "$tag_root" "${CHANVOY_GPG_HOMEDIR:-}" <<'PY'
import pathlib
import sys
root, p = map(pathlib.Path, sys.argv[1:])
if not p.is_absolute() or not p.is_dir() or any(a.is_symlink() for a in (p, *p.parents)) or p.resolve().is_relative_to(root):
    raise SystemExit('error: external absolute nonsymlink GPG home required')
PY
	local listing primary subkey
	listing="$(gpg --homedir "$CHANVOY_GPG_HOMEDIR" --batch --with-colons --fingerprint \
		--with-subkey-fingerprint --list-keys "${CHANVOY_PGP_KEY_ID%!}" 2>/dev/null)" || { tag_die 'signing subkey unavailable'; return 1; }
	primary="$(awk -F: '$1=="pub" {p=1;next} p && $1=="fpr" {print $10;exit}' <<<"$listing")"
	subkey="$(awk -F: -v f="${CHANVOY_PGP_KEY_ID%!}" '$1=="sub" {s=1;cap=$12;valid=$2;next} s && $1=="fpr" {if ($10==f && cap ~ /s/ && valid !~ /[erd]/) print $10; s=0}' <<<"$listing")"
	[[ "$primary" == "$CHANVOY_GPG_SIGNING_FINGERPRINT" && "$subkey!" == "$CHANVOY_PGP_KEY_ID" ]] || {
		tag_die 'live signing subkey on approved primary required'; return 1;
	}
	export GNUPGHOME="$CHANVOY_GPG_HOMEDIR"
}
tag_verify_object() {
	local object="$1" expected="$2" tagger actual
	[[ "$(git cat-file -t "$object" 2>/dev/null)" == tag &&
		"$(git cat-file tag "$object" | sed -n 's/^tag //p' | head -1)" == "$CHANVOY_RELEASE_TAG" &&
		"$(git rev-parse "$object^{}")" == "$(git rev-parse HEAD)" &&
		"$(git rev-parse HEAD)" == "$(git rev-parse refs/remotes/origin/main)" ]] || {
		tag_die 'annotated tag must match name, HEAD and fetched main'; return 1;
	}
	tagger="$(git cat-file tag "$object" | sed -n 's/^tagger \(.*\) [0-9][0-9]* [+-][0-9][0-9][0-9][0-9]$/\1/p' | head -1)"
	[[ "$tagger" == "$CHANVOY_TAGGER_NAME <$CHANVOY_TAGGER_EMAIL>" ]] || { tag_die 'tagger identity mismatch'; return 1; }
	actual="$(mktemp "${TMPDIR:-/tmp}/chanvoy-tag-body.XXXXXX")"
	if ! bash "$tag_root/scripts/release-tag-body.sh" "$object" >"$actual" || ! cmp -s "$expected" "$actual"; then
		rm -f "$actual"; tag_die 'signed tag message mismatch'; return 1;
	fi
	rm -f "$actual"
}
