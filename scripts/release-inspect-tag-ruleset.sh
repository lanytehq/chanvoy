#!/usr/bin/env bash
# Read-only rules advisory. UNKNOWN or FOUND never authorizes signing/pushing.
set -euo pipefail
tag="${1:-}"
if [[ ! "$tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
	echo 'UNKNOWN: canonical release tag required'; exit 0;
fi
if ! command -v gh >/dev/null || ! command -v jq >/dev/null; then
	echo 'UNKNOWN: gh and jq required for rules inspection'; exit 0;
fi
repository=lanytehq/chanvoy
if ! pages="$(gh api --paginate --slurp "repos/$repository/rulesets?includes_parents=true&per_page=100" 2>/dev/null)"; then
	echo 'UNKNOWN: effective tag rules could not be read'; exit 0;
fi
if ! jq -e 'type == "array" and all(.[]; type == "array" and all(.[]; type == "object" and (.id | type == "number") and (.target | type == "string") and (.enforcement | type == "string")))' <<<"$pages" >/dev/null 2>&1; then
	echo 'UNKNOWN: incomplete effective rule listing'; exit 0;
fi
ids="$(jq -r '[.[][] | select(.target == "tag" and .enforcement == "active") | .id] | unique | .[]' <<<"$pages")"
uncertain=0
for id in $ids; do
	if ! rule="$(gh api "repos/$repository/rulesets/$id" 2>/dev/null)"; then uncertain=1; continue; fi
	if ! result="$(jq -er --arg ref "refs/tags/$tag" '
      def matches:
        if type != "string" then null
        elif (startswith("~") or contains("?") or contains("[") or contains("]") or contains("\\")) then null
        elif (endswith("*") and (.[0:-1] | contains("*") | not)) then .[0:-1] as $prefix | $ref | startswith($prefix)
        elif contains("*") then null
        else . == $ref end;
      if type != "object" or (.target | type) != "string" or (.enforcement | type) != "string" then "unknown"
      elif .target != "tag" or .enforcement != "active" then "absent"
      elif (.conditions | type) != "object" or (.conditions | keys) != ["ref_name"] or
        (.conditions.ref_name | type) != "object" or (.conditions.ref_name.include | type) != "array" or
        (.conditions.ref_name.exclude | type) != "array" or any(.conditions.ref_name.include[]; type != "string") or
        any(.conditions.ref_name.exclude[]; type != "string") or (.rules | type) != "array" or
        any(.rules[]; type != "object" or (.type | type) != "string") or (.bypass_actors | type) != "array" then "unknown"
      else . as $rule |
        ([.conditions.ref_name.include[] | matches] | if any(.[]; . == true) then "yes" elif any(.[]; . == null) then "unknown" else "no" end) as $included |
        ([.conditions.ref_name.exclude[] | matches] | if any(.[]; . == true) then "yes" elif any(.[]; . == null) then "unknown" else "no" end) as $excluded |
        if $included == "unknown" or $excluded == "unknown" then "unknown"
        elif $included == "no" or $excluded == "yes" then "absent"
        elif (["creation", "update", "deletion", "non_fast_forward"] - [$rule.rules[].type] | length) != 0 then "absent"
        elif any($rule.bypass_actors[]; type != "object" or (.actor_type | type) != "string" or (.bypass_mode | type) != "string") then "unknown"
        elif ($rule.source_type != "Repository" and $rule.source_type != "Organization") or ($rule.source | type) != "string" then "unknown"
        else "found " + $rule.source_type + " bypass=" + (if ($rule.bypass_actors | length) == 0 then "none" else ($rule.bypass_actors | map(.actor_type + ":" + .bypass_mode) | unique | join(",")) end) end
      end' <<<"$rule" 2>/dev/null)"; then uncertain=1; continue; fi
	case "$result" in
		found\ *) echo "FOUND: applicable tag ruleset id=$id $result controls=creation,update,deletion,non_fast_forward"; exit 0 ;;
		unknown) uncertain=1 ;;
	esac
done
if [[ "$uncertain" == 1 ]]; then echo 'UNKNOWN: effective rules or conditions unverified';
else echo 'ABSENT: no visible active rule provides required controls'; fi
