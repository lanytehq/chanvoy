#!/usr/bin/env bash
# Hermetic reporting proofs: all process/CLI surfaces are fixture executables.
set -euo pipefail
report_test_repo=$(cd "$(dirname "$0")/.." && pwd -P)
report_test_helper=${1:-$report_test_repo/scripts/report-installed-daemons.sh}
report_test_root=$(mktemp -d)
trap 'rm -rf "$report_test_root"' EXIT
report_test_path=$PATH
report_test_cmp=$(command -v cmp)
report_test_tools="$report_test_root/fake tools"
report_test_bin_dir="$report_test_root/bin [a]. space"
mkdir -p "$report_test_tools" "$report_test_bin_dir"
report_test_bin="$report_test_bin_dir/chanvoy"
report_test_reference="$report_test_root/qualified artifact"
report_test_calls="$report_test_root/cli.calls"
report_test_ps_calls="$report_test_root/ps.calls"
report_test_ps_input="$report_test_root/ps.input"
report_test_output="$report_test_root/output"

cat > "$report_test_bin" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$REPORT_TEST_CLI_LOG"
printf '%s\n' '{"stopped":true,"profile":"synthetic-installer"}'
exit "${REPORT_TEST_CLI_EXIT:-0}"
EOF
cat > "$report_test_tools/ps" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$REPORT_TEST_PS_LOG"
if [[ $REPORT_TEST_PS_EXIT != 0 ]]; then exit "$REPORT_TEST_PS_EXIT"; fi
cat "$REPORT_TEST_PS_INPUT"
EOF
cat > "$report_test_tools/cmp" <<'EOF'
#!/usr/bin/env bash
if [[ $REPORT_TEST_CMP_EXIT != 0 ]]; then exit "$REPORT_TEST_CMP_EXIT"; fi
exec "$REPORT_TEST_REAL_CMP" "$@"
EOF
chmod +x "$report_test_bin" "$report_test_tools/ps" "$report_test_tools/cmp"
/bin/bash -n "$report_test_helper"

fail() {
    printf 'FAIL: %s: %s\n' "$report_test_case" "$1" >&2
    cat "$report_test_output" >&2
    exit 1
}
has() { grep -Fq -- "$1" "$report_test_output" || fail "missing receipt: $1"; }
lacks() { if grep -Fq -- "$1" "$report_test_output"; then fail "false receipt: $1"; fi; }
reset_case() {
    report_test_case=$1
    report_test_selected_bin=$report_test_bin
    report_test_selected_reference=$report_test_reference
    report_test_inspection_exit=0
    report_test_compare_exit=0
    report_test_cli_exit=0
    report_test_skip=''
    chmod +x "$report_test_bin"
    cp "$report_test_bin" "$report_test_reference"
    : > "$report_test_ps_input"
    : > "$report_test_calls"
    : > "$report_test_ps_calls"
    : > "$report_test_output"
}
run_report() {
    env -i PATH="$report_test_tools:$report_test_path" \
        CHANVOY_INSTALL_QUALIFIED_ARTIFACT="$report_test_selected_reference" \
        CHANVOY_INSTALL_SKIP_DAEMON_RESTART="$report_test_skip" \
        REPORT_TEST_CLI_LOG="$report_test_calls" REPORT_TEST_CLI_EXIT="$report_test_cli_exit" \
        REPORT_TEST_PS_LOG="$report_test_ps_calls" REPORT_TEST_PS_INPUT="$report_test_ps_input" \
        REPORT_TEST_PS_EXIT="$report_test_inspection_exit" REPORT_TEST_CMP_EXIT="$report_test_compare_exit" \
        REPORT_TEST_REAL_CMP="$report_test_cmp" \
        /bin/bash "$report_test_helper" "$report_test_selected_bin" > "$report_test_output" 2>&1 \
        || fail 'reporting must preserve fail-soft exit'
    [[ ! -s "$report_test_calls" ]] || fail 'ANY CLI/ownable/stop/start invocation is forbidden'
    IFS= read -r report_test_first_line < "$report_test_output"
    [[ "$report_test_first_line" == *'reports installed-daemon candidates; no automatic restart in this release.'* ]] \
        || fail 'reporting alias notice must be first'
    lacks '[ok] restarted'
    lacks 'DOWN'
    lacks 'foreign'
    lacks 'no live daemons'
    lacks 'the stop did land'
}
no_inspection() { [[ ! -s "$report_test_ps_calls" ]] || fail 'inspection preceded its prerequisite'; }
one_inspection() {
    [[ $(wc -l < "$report_test_ps_calls") -eq 1 ]] || fail 'expected one owned discovery snapshot'
}

reset_case matching-candidates
printf '%s\n' \
    "$report_test_bin --profile synthetic-alpha daemon serve" \
    "$report_test_bin --profile synthetic-alpha daemon serve" \
    "$report_test_bin --profile synthetic-beta daemon serve --json" > "$report_test_ps_input"
run_report
one_inspection
has 'synthetic-alpha: daemon candidate observed'
has 'synthetic-beta: daemon candidate observed'
has '[!!] daemon report: candidates=2 withheld=2 unresolved=0'
lacks '[ok]'
has 'whole-process exit and guarded runtime cleanup'
has 'listener absence or refused connection are not death proof'
has 'unknown ownership, death or cleanup withholds startup'
has 'only after confirmation:'
has 'verify dual pin:'

reset_case old-no-runtime-stop-receipt
printf '%s\n' "$report_test_bin --profile synthetic-installer daemon serve" > "$report_test_ps_input"
run_report
has 'candidates=1 withheld=1 unresolved=0'
# The fixture would emit old stopped:true if called. Its zero invocation count
# proves that neither an old no-op receipt nor candidate uncertainty admits start.

reset_case refused-or-unlaunchable-cli
report_test_cli_exit=2
printf '%s\n' "$report_test_bin --profile synthetic-installer daemon serve" > "$report_test_ps_input"
run_report
has 'automatic restart withheld'

reset_case missing-reference
report_test_selected_reference=''
run_report
no_inspection
has 'unqualified:'
has '[!!] daemon report: candidates=0 withheld=0 unresolved=1'

reset_case mismatched-reference
printf '%s\n' 'different synthetic bytes' > "$report_test_reference"
run_report
no_inspection
has 'reference bytes differ or cannot be compared'

reset_case missing-reference-path
report_test_selected_reference="$report_test_root/not-present"
run_report
no_inspection
has 'unqualified:'

reset_case unreadable-comparison
report_test_compare_exit=2
run_report
no_inspection
has 'reference bytes differ or cannot be compared'

reset_case nonregular-reference
report_test_selected_reference=$report_test_root
run_report
no_inspection
has 'unqualified:'

reset_case fifo-reference
report_test_selected_reference="$report_test_root/reference.fifo"
mkfifo "$report_test_selected_reference"
run_report
no_inspection
has 'unqualified:'

reset_case symlink-reference
report_test_selected_reference="$report_test_root/reference.link"
ln -s "$report_test_reference" "$report_test_selected_reference"
run_report
no_inspection
has 'unqualified:'

reset_case installed-self-reference
report_test_selected_reference=$report_test_bin
run_report
no_inspection
has 'reference must be independent'

reset_case installed-hardlink-reference
report_test_selected_reference="$report_test_root/reference.hardlink"
ln "$report_test_bin" "$report_test_selected_reference"
run_report
no_inspection
has 'reference must be independent'

reset_case unavailable-installed-executable
chmod -x "$report_test_bin"
run_report
no_inspection
has 'installed executable unavailable'

reset_case opt-out
report_test_skip=1
report_test_selected_reference=''
run_report
no_inspection
has 'reporting skipped'

reset_case denied-discovery
report_test_inspection_exit=1
run_report
one_inspection
has 'discovery-unconfirmed'
lacks 'no matching candidates'
has 'unresolved=1'

reset_case zero-candidates
run_report
one_inspection
has 'no matching candidates observed'
has 'candidates=0 withheld=0 unresolved=0'

reset_case literal-path-and-argument-boundaries
printf '%s\n' \
    "$report_test_bin-lookalike --profile excluded daemon serve" \
    "prefix$report_test_bin --profile excluded daemon serve" \
    "$report_test_root/worktree/chanvoy --profile excluded daemon serve" \
    "$report_test_bin --profile excluded daemon serving" \
    "$report_test_bin --profile excluded daemon serveExtra" \
    "$report_test_bin --profile excluded other-daemon serve" \
    "$report_test_bin --profile excluded daemon start" \
    "$report_test_bin --profile synthetic-safe daemon serve" > "$report_test_ps_input"
run_report
one_inspection
has 'synthetic-safe: daemon candidate observed'
lacks 'excluded: daemon candidate observed'
has 'candidates=1 withheld=1 unresolved=0'

reset_case actual-make-reporting-alias
printf '%s\n' "$report_test_bin --profile synthetic-installer daemon serve" > "$report_test_ps_input"
env -i PATH="$report_test_tools:$report_test_path" \
    CHANVOY_INSTALL_QUALIFIED_ARTIFACT="$report_test_reference" \
    REPORT_TEST_CLI_LOG="$report_test_calls" REPORT_TEST_CLI_EXIT=0 \
    REPORT_TEST_PS_LOG="$report_test_ps_calls" REPORT_TEST_PS_INPUT="$report_test_ps_input" \
    REPORT_TEST_PS_EXIT=0 REPORT_TEST_CMP_EXIT=0 REPORT_TEST_REAL_CMP="$report_test_cmp" \
    make --no-print-directory -C "$report_test_repo" install-restart-daemons "LOCAL_BIN=$report_test_bin_dir" \
    > "$report_test_output" 2>&1 || fail 'compatibility alias failed'
[[ ! -s "$report_test_calls" ]] || fail 'Make alias invoked a CLI/lifecycle command'
one_inspection
has 'reports installed-daemon candidates; no automatic restart'
has 'candidates=1 withheld=1 unresolved=0'

reset_case literal-profile-punctuation
printf '%s\n' "$report_test_bin --profile _synthetic.profile daemon serve" > "$report_test_ps_input"
run_report
one_inspection
has '_synthetic.profile: daemon candidate observed'
has 'candidates=1 withheld=1 unresolved=0'

reset_case unconfirmed-profile-formatting
printf '%s\n' "$report_test_bin --profile synthetic!unconfirmed daemon serve" > "$report_test_ps_input"
run_report
one_inspection
has 'candidate formatting is unconfirmed'
has 'unresolved=1'
lacks 'no matching candidates'
lacks 'synthetic!unconfirmed'

reset_case unix-only-alias
env -i PATH="$report_test_tools:$report_test_path" \
    REPORT_TEST_CLI_LOG="$report_test_calls" REPORT_TEST_CLI_EXIT=0 \
    REPORT_TEST_PS_LOG="$report_test_ps_calls" REPORT_TEST_PS_INPUT="$report_test_ps_input" \
    REPORT_TEST_PS_EXIT=0 REPORT_TEST_CMP_EXIT=0 REPORT_TEST_REAL_CMP="$report_test_cmp" \
    make --no-print-directory -C "$report_test_repo" install-restart-daemons OS=Windows_NT "LOCAL_BIN=$report_test_bin_dir" \
    > "$report_test_output" 2>&1 || fail 'Unix-only alias failed'
[[ ! -s "$report_test_calls" ]] || fail 'unsupported platform invoked CLI/lifecycle'
no_inspection
has 'reports installed-daemon candidates; no automatic restart'
has 'process discovery is Unix-only'
has 'unresolved=1'

printf '%s\n' '[ok] installer reporting: 21 owned cases, zero CLI/lifecycle calls'
