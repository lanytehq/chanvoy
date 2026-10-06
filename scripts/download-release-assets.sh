#!/usr/bin/env bash
# Compatibility spelling: exact CI artifacts, not an unverified draft download.
set -euo pipefail
if [[ "${1:-}" == --help || "${1:-}" == -h ]]; then
    echo 'Usage: download-release-assets.sh <release-tag> <output-dir>'
    echo 'Stages exact verified CI artifacts and an external receipt; output must be empty.'
    exit 0
fi
[[ $# == 2 ]] || { echo 'error: release tag and output directory required' >&2; exit 2; }
exec bash "$(dirname "$0")/release-fetch-ci-artifacts.sh" "$@"
