#!/usr/bin/env bash
# Isolated message/operator/version/ref/rules tests; no production key or remote.
# shellcheck disable=SC2016
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/pg-tag-test.XXXXXX")"
scratch="$(cd "$scratch" && pwd -P)"
trap 'rm -rf "$scratch"' EXIT
repo="$scratch/repo"
git init -q -b main "$repo"
git -C "$repo" config user.name fixture
git -C "$repo" config user.email fixture@example.invalid
mkdir -p "$repo/scripts" "$repo/config/release" "$scratch/bin"
cp "$root/scripts/"{release-tag-common.sh,release-prepare-tag-message.py,release-tag-body.sh,release-tag-operator.sh,release-guard-tag-version.sh,release-restore-tag-ref.sh,release-inspect-tag-ruleset.sh} "$repo/scripts/"
cp "$root/config/release/tagger-identity.txt" "$repo/config/release/"
printf '1.2.3\n' >"$repo/VERSION"
export CHANVOY_RELEASE_TAG=v1.2.3 CHANVOY_TAG_MESSAGE_DIR="$scratch/v1.2.3"
export CHANVOY_TAGGER_NAME='3 Leaps Infosec Team' CHANVOY_TAGGER_EMAIL=infosec@3leaps.net
unset RELEASE_TAG CHANVOY_APPROVED_ENV_LOADER
reject() { if "$@" >"$scratch/rejected" 2>&1; then echo 'error: expected tag-control rejection' >&2; exit 1; fi; }
python3 "$repo/scripts/release-prepare-tag-message.py" >/dev/null
cp "$scratch/v1.2.3/message.txt" "$scratch/expected"
printf 'Reviewed custom message\n' >"$scratch/v1.2.3/message.txt"
python3 "$repo/scripts/release-prepare-tag-message.py" >/dev/null
grep -q '^Reviewed custom message$' "$scratch/v1.2.3/message.txt"
# shellcheck source=release-tag-common.sh
# shellcheck disable=SC1091
source "$repo/scripts/release-tag-common.sh"
tag_identity
tag_message_file >/dev/null
for message in 'missing newline' $'bad trailing space \n' $'double newline\n\n' \
	$'bad\r\n' $'<REPLACE-model>\n' $'<model>\n'; do
	printf '%s' "$message" >"$scratch/v1.2.3/message.txt"
	reject tag_message_file
	reject python3 "$repo/scripts/release-prepare-tag-message.py"
done
cp "$scratch/expected" "$scratch/v1.2.3/message.txt"
ln -s "$scratch" "$scratch/linked"
CHANVOY_TAG_MESSAGE_DIR="$scratch/linked/v1.2.3" reject tag_message_file
CHANVOY_TAG_MESSAGE_DIR="$repo/v1.2.3" reject python3 "$repo/scripts/release-prepare-tag-message.py"
CHANVOY_TAGGER_NAME=$'bad\nname' reject tag_identity
CHANVOY_TAGGER_EMAIL='a<b>' reject tag_identity
CHANVOY_RELEASE_TAG=v01.2.3 reject bash "$repo/scripts/release-guard-tag-version.sh"
CHANVOY_RELEASE_TAG=v9.9.9 reject bash "$repo/scripts/release-guard-tag-version.sh"
RELEASE_TAG=v1.2.4 reject bash "$repo/scripts/release-guard-tag-version.sh"
RELEASE_TAG=v1.2.3 bash "$repo/scripts/release-guard-tag-version.sh" >/dev/null
CHANVOY_PGP_KEY_ID=0123456789ABCDEF reject tag_selector_shape
CHANVOY_PGP_KEY_ID=0123456789ABCDEF0123456789ABCDEF01234567! \
	CHANVOY_GPG_SIGNING_FINGERPRINT=0123456789ABCDEF0123456789ABCDEF01234567 tag_selector_shape
# Loader errors cannot replace the requested cut or leak configured paths.
printf 'export CHANVOY_RELEASE_TAG=v9.9.9\nprintf "private loader output\\n"\n' >"$scratch/loader.sh"
CHANVOY_APPROVED_ENV_LOADER="$scratch/loader.sh" reject bash "$repo/scripts/release-tag-operator.sh" prepare-message
if grep -q 'private loader output' "$scratch/rejected"; then
	echo 'error: approved-loader output leaked' >&2; exit 1;
fi
ln -s "$scratch/loader.sh" "$scratch/loader-link"
CHANVOY_APPROVED_ENV_LOADER="$scratch/loader-link" reject bash "$repo/scripts/release-tag-operator.sh" prepare-message
bash "$repo/scripts/release-tag-operator.sh" prepare-message >/dev/null
git -C "$repo" add .
git -C "$repo" commit -qm fixture
git -C "$repo" update-ref refs/remotes/origin/main HEAD
git -C "$repo" tag v1.2.3
(cd "$repo"; reject tag_verify_object refs/tags/v1.2.3 "$scratch/expected")
git -C "$repo" tag -d v1.2.3 >/dev/null
GIT_COMMITTER_NAME="$CHANVOY_TAGGER_NAME" GIT_COMMITTER_EMAIL="$CHANVOY_TAGGER_EMAIL" \
	git -C "$repo" tag -a --cleanup=verbatim v1.2.3 -F "$scratch/expected"
(cd "$repo"; tag_verify_object refs/tags/v1.2.3 "$scratch/expected")
printf 'wrong body\n' >"$scratch/wrong"
(cd "$repo"; reject tag_verify_object refs/tags/v1.2.3 "$scratch/wrong")
git init -q --bare -b main "$scratch/remote.git"
git -C "$repo" remote add origin "$scratch/remote.git"
git -C "$repo" push -q origin main refs/tags/v1.2.3
git -C "$repo" remote set-url origin https://github.com/lanytehq/chanvoy.git
git -C "$repo" config url."$scratch/remote.git".insteadOf https://github.com/lanytehq/chanvoy.git
tag_checkout
# Restore a peeled runner-local ref to the exact annotated remote object.
object="$(git -C "$repo" rev-parse refs/tags/v1.2.3)"
git -C "$repo" switch --quiet --detach HEAD
git -C "$repo" update-ref refs/tags/v1.2.3 HEAD
(cd "$repo"; bash scripts/release-restore-tag-ref.sh >/dev/null)
[[ "$(git -C "$repo" rev-parse refs/tags/v1.2.3)" == "$object" ]]
(cd "$repo"; CHANVOY_REQUIRE_TAG=1 bash scripts/release-guard-tag-version.sh >/dev/null)
(cd "$repo"; CHANVOY_EXPECTED_TAG_OBJECT="$(printf '%040d' 0)" reject bash scripts/release-restore-tag-ref.sh)
printf 'dirty\n' >"$repo/dirty"
(cd "$repo"; CHANVOY_REQUIRE_TAG=1 reject bash scripts/release-guard-tag-version.sh)
rm "$repo/dirty"
# Effective rules report uncertainty; never infer protection from display names.
cat >"$scratch/bin/gh" <<'SH'
#!/usr/bin/env bash
[[ "${FIXTURE_API_FAILURE:-0}" == 0 ]] || exit 1
if [[ "$*" == *'rulesets?includes_parents=true&per_page=100'* ]]; then
    cat "$FIXTURE_LIST"
else cat "$FIXTURE_RULE"; fi
SH
chmod +x "$scratch/bin/gh"
export PATH="$scratch/bin:$PATH" FIXTURE_LIST="$scratch/list" FIXTURE_RULE="$scratch/rule"
printf '[[{"id":7,"target":"tag","enforcement":"active"}]]\n' >"$FIXTURE_LIST"
cat >"$FIXTURE_RULE" <<'JSON'
{"source_type":"Organization","source":"lanytehq","target":"tag","enforcement":"active","conditions":{"ref_name":{"include":["refs/tags/v*"],"exclude":[]}},"rules":[{"type":"creation"},{"type":"update"},{"type":"deletion"},{"type":"non_fast_forward"}],"bypass_actors":[{"actor_type":"Team","bypass_mode":"always"}]}
JSON
report="$repo/scripts/release-inspect-tag-ruleset.sh"
[[ "$(bash "$report" v1.2.3)" == *'bypass=Team:always'* ]]
for mutation in 'del(.bypass_actors)' '.conditions.ref_name.exclude=["~UNKNOWN"]' 'del(.target)'; do
	jq "$mutation" "$scratch/rule" >"$scratch/changed"
	[[ "$(FIXTURE_RULE="$scratch/changed" bash "$report" v1.2.3)" == UNKNOWN:* ]]
done
for mutation in '.rules=[]' '.conditions.ref_name.exclude=["refs/tags/v1.2.3"]' '.enforcement="disabled"'; do
	jq "$mutation" "$scratch/rule" >"$scratch/changed"
	[[ "$(FIXTURE_RULE="$scratch/changed" bash "$report" v1.2.3)" == ABSENT:* ]]
done
[[ "$(FIXTURE_API_FAILURE=1 bash "$report" v1.2.3)" == UNKNOWN:* ]]
printf 'bad json\n' >"$FIXTURE_LIST"
[[ "$(bash "$report" v1.2.3)" == UNKNOWN:* ]]
while IFS= read -r target; do
	grep -Eq "^${target}:" "$root/Makefile" || { echo 'error: checklist target absent' >&2; exit 1; }
done < <(grep -oE 'make [a-z][a-z0-9-]+' "$root/RELEASE_CHECKLIST.md" | awk '{print $2}' | sort -u)
echo '[ok] external messages, operator, tag body, version, restored ref and rules controls'
