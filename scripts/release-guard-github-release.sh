#!/usr/bin/env bash
# Require authoritative proof that a GitHub Release object does not exist.

set -euo pipefail

fail() {
	echo "error: $*" >&2
	exit 1
}

tag="${1:-}"
[ -n "$tag" ] || fail "usage: release-guard-github-release.sh <tag>"

set +e
repo_response="$(gh api --include --silent "repos/lanytehq/chanvoy" 2>&1)"
repo_query_status=$?
set -e

repo_http_status="$(
	printf '%s\n' "$repo_response" |
		awk '$1 ~ /^HTTP\// && $2 ~ /^[0-9][0-9][0-9]$/ { status = $2 } END { print status }'
)"
case "$repo_http_status" in
2??) ;;
"")
	printf '%s\n' "$repo_response" >&2
	fail "could not prove GitHub repository access (gh exit ${repo_query_status})"
	;;
*)
	printf '%s\n' "$repo_response" >&2
	fail "GitHub repository access probe failed (HTTP ${repo_http_status}, gh exit ${repo_query_status})"
	;;
esac

set +e
response="$(gh api --include --silent "repos/lanytehq/chanvoy/releases/tags/${tag}" 2>&1)"
query_status=$?
set -e

http_status="$(
	printf '%s\n' "$response" |
		awk '$1 ~ /^HTTP\// && $2 ~ /^[0-9][0-9][0-9]$/ { status = $2 } END { print status }'
)"

case "$http_status" in
404)
	echo "[ok] GitHub Release object absent for ${tag}"
	;;
2??)
	fail "GitHub Release object already exists for ${tag}"
	;;
"")
	printf '%s\n' "$response" >&2
	fail "could not determine GitHub Release state for ${tag} (gh exit ${query_status})"
	;;
*)
	printf '%s\n' "$response" >&2
	fail "GitHub Release query failed for ${tag} (HTTP ${http_status}, gh exit ${query_status})"
	;;
esac
