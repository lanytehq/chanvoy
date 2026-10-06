#!/usr/bin/env bash
# Install the checksum-pinned public Linux CI helper, never a ceremony key.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
[[ -n "${RUNNER_TEMP:-}" && -n "${GITHUB_ENV:-}" ]] || { echo 'error: CI helper installation requires runner paths' >&2; exit 1; }
tool_dir="$(mktemp -d "$RUNNER_TEMP/decernor.XXXXXX")"
curl --proto '=https' --tlsv1.2 --fail --silent --show-error --location \
	'https://github.com/3leaps/decernor/releases/download/v0.1.8/decernor_0.1.8_linux_amd64.tar.gz' \
	--output "$tool_dir/decernor_0.1.8_linux_amd64.tar.gz"
(cd "$tool_dir" && sha256sum --check "$root/config/release/decernor-ci-sha256.txt")
tar -xzf "$tool_dir/decernor_0.1.8_linux_amd64.tar.gz" -C "$tool_dir" decernor
[[ -f "$tool_dir/decernor" && -x "$tool_dir/decernor" && ! -L "$tool_dir/decernor" ]] || { echo 'error: unsafe CI helper binary' >&2; exit 1; }
printf 'CHANVOY_DECERNOR_BIN=%s\n' "$tool_dir/decernor" >>"$GITHUB_ENV"
