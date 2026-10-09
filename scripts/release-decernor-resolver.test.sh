#!/usr/bin/env bash
# Resolver slice of the release anchor corpus; synthetic executables, no keys.
# shellcheck disable=SC2016 # Child shell, not parent, expands positional args.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/chanvoy-decernor-resolver.XXXXXX")"
trap 'rm -rf "$fixture"' EXIT
source "$root/scripts/release-decernor.sh"
unset CHANVOY_DECERNOR_BIN DECERNOR_BIN DECERNOR

make_stub() {
	local name="$1" version="$2" extended_version="$3" identity="${4:-decernor}"
	cat >"$fixture/$name" <<EOF
#!/usr/bin/env bash
case "\$*" in
  version) printf '%s\n' '$identity $version' ;;
  'version -e') printf '%s\n' 'Version: $extended_version' 'Commit: synthetic' 'Build Date: synthetic' 'Go Version: synthetic' 'Gofulmen: synthetic' 'Crucible: synthetic' ;;
  *) exit 1 ;;
esac
EOF
	chmod +x "$fixture/$name"
}

reject() {
	if ( "$@" ) >"$fixture/output" 2>&1; then
		echo 'error: expected resolver rejection' >&2
		exit 1
	fi
	# Diagnostics must not leak even these synthetic configured paths.
	if grep -Fq "$fixture" "$fixture/output"; then
		echo 'error: resolver diagnostic included configured path' >&2
		exit 1
	fi
}

make_stub valid 0.1.8 0.1.8
make_stub newer 0.2.0 0.2.0
make_stub old 0.1.7 0.1.7
make_stub mismatch 0.1.8 0.1.7
make_stub wrong 0.1.8 0.1.8 other
cat >"$fixture/noisy" <<'SH'
#!/usr/bin/env bash
echo "$0: synthetic probe failure" >&2
exit 1
SH
chmod +x "$fixture/noisy"
CHANVOY_DECERNOR_BIN="$fixture/valid" resolve_release_decernor ceremony
[[ "$RELEASE_DECERNOR_BIN" == "$fixture/valid" ]]
CHANVOY_DECERNOR_BIN="$fixture/newer" resolve_release_decernor ceremony
[[ "$RELEASE_DECERNOR_BIN" == "$fixture/newer" ]]

(
	CHANVOY_DECERNOR_BIN="$fixture/valid"
	DECERNOR_BIN="$fixture/old"
	resolve_release_decernor general
	[[ "$RELEASE_DECERNOR_BIN" == "$fixture/valid" ]]
	unset CHANVOY_DECERNOR_BIN
	DECERNOR_BIN="$fixture/valid" resolve_release_decernor general
	[[ "$RELEASE_DECERNOR_BIN" == "$fixture/valid" ]]
	unset DECERNOR_BIN
	mkdir "$fixture/path"
	cp "$fixture/valid" "$fixture/path/decernor"
	PATH="$fixture/path:$PATH"
	resolve_release_decernor general
	[[ "$RELEASE_DECERNOR_BIN" == "$fixture/path/decernor" ]]
)

# Ceremony requires an explicit absolute regular executable, never PATH fallback.
reject resolve_release_decernor ceremony
reject env DECERNOR_BIN="$fixture/valid" bash -c 'source "$1"; resolve_release_decernor ceremony' _ "$root/scripts/release-decernor.sh"
for stub in old mismatch wrong missing noisy; do
	reject env CHANVOY_DECERNOR_BIN="$fixture/$stub" bash -c 'source "$1"; resolve_release_decernor ceremony' _ "$root/scripts/release-decernor.sh"
done
reject env CHANVOY_DECERNOR_BIN=decernor bash -c 'source "$1"; resolve_release_decernor ceremony' _ "$root/scripts/release-decernor.sh"
ln -s "$fixture/valid" "$fixture/link"
cp "$fixture/valid" "$fixture/nonexec"
chmod -x "$fixture/nonexec"
for stub in link nonexec; do
	reject env CHANVOY_DECERNOR_BIN="$fixture/$stub" bash -c 'source "$1"; resolve_release_decernor ceremony' _ "$root/scripts/release-decernor.sh"
done
echo '[ok] Decernor resolver identity, precedence, explicit ceremony binding and redaction'
