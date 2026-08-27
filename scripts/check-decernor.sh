#!/usr/bin/env bash
# Preflight helper: require decernor >= 0.1.4 (strict X.Y.Z).
set -euo pipefail
script_dir="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib/fingerprint-contract.sh
source "${script_dir}/lib/fingerprint-contract.sh"
chanvoy_preflight_decernor
