#!/usr/bin/env bash
# Reporting only: process discovery cannot attest a candidate's native death.
set -uo pipefail

printf '%s\n' '[..] install-restart-daemons reports installed-daemon candidates; no automatic restart in this release.'

report_bin=${1:-}
report_reference=${CHANVOY_INSTALL_QUALIFIED_ARTIFACT:-}

unresolved() {
    printf '[!!] %s; no cycle attempted\n' "$1"
    printf '%s\n' '[!!] daemon report: candidates=0 withheld=0 unresolved=1'
}

if [[ ${CHANVOY_INSTALL_SKIP_DAEMON_RESTART:-} == 1 ]]; then
    printf '%s\n' '[..] reporting skipped (CHANVOY_INSTALL_SKIP_DAEMON_RESTART=1); no cycle attempted'
    exit 0
fi
if [[ ! -f "$report_bin" || -L "$report_bin" || ! -x "$report_bin" ]]; then
    unresolved 'installed executable unavailable'
    exit 0
fi
if [[ -z "$report_reference" || ! -f "$report_reference" || -L "$report_reference" ]]; then
    unresolved 'unqualified: designate an independent artifact with CHANVOY_INSTALL_QUALIFIED_ARTIFACT'
    exit 0
fi
if [[ "$report_bin" -ef "$report_reference" ]]; then
    unresolved 'unqualified: the reference must be independent of the installed copy'
    exit 0
fi
if ! cmp -s -- "$report_bin" "$report_reference"; then
    unresolved 'unqualified: reference bytes differ or cannot be compared'
    exit 0
fi

# Equality identifies the explicitly designated reference; it does not confer
# release qualification or prove any daemon's generation, ownership or death.
if report_args=$(ps -ww -axo args= 2>/dev/null); then
    :
else
    unresolved 'discovery-unconfirmed: process inspection did not complete'
    exit 0
fi

report_profiles=''
report_seen=$'\n'
report_count=0
report_unconfirmed=0
while IFS= read -r report_line; do
    case "$report_line" in
        "$report_bin --profile "*) ;;
        *) continue ;;
    esac
    report_rest=${report_line#"$report_bin --profile "}
    case "$report_rest" in
        *' daemon serve'|*' daemon serve '*) ;;
        *) continue ;;
    esac
    # ps joins argv with spaces. Inspect the complete prefix before the verb,
    # so an ambiguous multiword/empty profile cannot become a false absence.
    report_profile=${report_rest%%" daemon serve"*}
    if [[ ! "$report_profile" =~ ^[A-Za-z0-9_.-]+$ ]]; then
        report_unconfirmed=$((report_unconfirmed + 1))
        continue
    fi
    case "$report_seen" in
        *$'\n'"$report_profile"$'\n'*) continue ;;
    esac
    report_seen+="$report_profile"$'\n'
    report_profiles+="$report_profile"$'\n'
    report_count=$((report_count + 1))
done <<< "$report_args"

if [[ "$report_count" == 0 && "$report_unconfirmed" == 0 ]]; then
    printf '%s\n' '[ok] no matching candidates observed; no cycle attempted'
    printf '%s\n' '[ok] daemon report: candidates=0 withheld=0 unresolved=0'
    exit 0
fi

while IFS= read -r report_profile; do
    [[ -n "$report_profile" ]] || continue
    printf '     [!!] %s: daemon candidate observed; automatic restart withheld (candidate death is not established)\n' "$report_profile"
    printf '         source the owning identity for %s; resolve candidate ownership and liveness\n' "$report_profile"
    printf '         observe: %q --profile %q daemon status\n' "$report_bin" "$report_profile"
    printf '         diagnose: %q --profile %q doctor\n' "$report_bin" "$report_profile"
    printf '         only for the independently owned predecessor: %q --profile %q daemon stop\n' "$report_bin" "$report_profile"
    printf '%s\n' '         independently confirm the same candidate whole-process exit and guarded runtime cleanup'
    printf '%s\n' '         stop exit 0, missing PID/socket, listener absence or refused connection are not death proof'
    printf '%s\n' '         unknown ownership, death or cleanup withholds startup'
    printf '         only after confirmation: %q --profile %q daemon start\n' "$report_bin" "$report_profile"
    printf '         verify dual pin: %q --profile %q version --extended\n' "$report_bin" "$report_profile"
    printf '         verify observation readiness: %q --profile %q doctor\n' "$report_bin" "$report_profile"
done <<< "$report_profiles"
if [[ "$report_unconfirmed" -gt 0 ]]; then
    printf '%s\n' '[!!] installed-path candidate formatting is unconfirmed; no cycle attempted'
fi
printf '[!!] daemon report: candidates=%d withheld=%d unresolved=%d\n' "$report_count" "$report_count" "$report_unconfirmed"
