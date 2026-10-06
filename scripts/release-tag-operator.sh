#!/usr/bin/env bash
# Explicit maintainer entrypoint: preparation, local tag and remote push separate.
set -euo pipefail
mode="${1:-}"
case "$mode" in prepare-message | preflight | local-tag | remote-push) ;;
*) echo 'error: expected prepare-message, preflight, local-tag or remote-push' >&2; exit 1 ;;
esac
root="$(cd "$(dirname "$0")/.." && pwd -P)"
cd "$root"
original_tag="${CHANVOY_RELEASE_TAG:-}"
[[ "$original_tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ &&
	"$original_tag" == "v$(cat VERSION)" ]] || { echo 'error: explicit canonical release tag matching VERSION required' >&2; exit 1; }
export CHANVOY_RELEASE_TAG
if [[ -n "${CHANVOY_APPROVED_ENV_LOADER+x}" ]]; then
	loader="$CHANVOY_APPROVED_ENV_LOADER"
	python3 - "$root" "$loader" <<'PY'
import pathlib
import sys
root, path = map(pathlib.Path, sys.argv[1:])
if not path.is_absolute() or not path.is_file() or any(p.is_symlink() for p in (path, *path.parents)) or path.resolve().is_relative_to(root):
    raise SystemExit('error: approved loader must be an external absolute nonsymlink regular file')
PY
	# shellcheck disable=SC1090
	if source "$loader" >/dev/null 2>&1; then loader_ok=1; else loader_ok=0; fi
	set -euo pipefail
	[[ "$loader_ok" == 1 ]] || { echo 'error: approved loader failed' >&2; exit 1; }
fi
cd "$root"
[[ "${CHANVOY_RELEASE_TAG:-}" == "$original_tag" && "$original_tag" == "v$(cat VERSION)" ]] || {
	echo 'error: loaded release tag differs from intended cut' >&2; exit 1;
}
if [[ "$mode" == prepare-message ]]; then
	export CHANVOY_TAG_MESSAGE_DIR
	exec python3 "$root/scripts/release-prepare-tag-message.py"
fi
export CHANVOY_TAG_MESSAGE_DIR CHANVOY_TAGGER_NAME CHANVOY_TAGGER_EMAIL \
	CHANVOY_GPG_SIGNING_FINGERPRINT CHANVOY_PGP_KEY_ID CHANVOY_GPG_HOMEDIR
# shellcheck source=release-tag-common.sh
# shellcheck disable=SC1091
source "$root/scripts/release-tag-common.sh"
tag_version
tag_identity
tag_checkout
tag_key_selector
tag_expected_message >/dev/null
bash "$root/scripts/release-inspect-tag-ruleset.sh" "$CHANVOY_RELEASE_TAG"
case "$mode" in
preflight) echo '[ok] maintainer preflight passed; no tag created' ;;
local-tag) bash "$root/scripts/release-tag.sh" ;;
remote-push) bash "$root/scripts/release-push-tag.sh" ;;
esac
