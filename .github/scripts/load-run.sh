#!/usr/bin/env bash
# Run plbench scenarios for one profile and provider, then compare each result
# with its baseline. Used by CI directly; local use goes through the Makefile.
#
#   load-run.sh <profile> <provider> [scenario ...]
#
# Environment:
#   PLBENCH_OUT   results directory (default: results)
#   PLBENCH_BIN   plbench binary (default: builds bin/plbench)
set -euo pipefail

profile="${1:?profile}"
provider="${2:?provider}"
shift 2
scenarios=("$@")
if [ "${#scenarios[@]}" -eq 0 ] || [ "${scenarios[0]}" = all ]; then
  scenarios=(catalog_isolated catalog_history)
fi

out="${PLBENCH_OUT:-results}"
bin="${PLBENCH_BIN:-bin/plbench}"
if [ ! -x "$bin" ]; then
  mkdir -p "$(dirname "$bin")"
  go build -o "$bin" ./partitionlog/internal/cmd/plbench
fi
mkdir -p "$out"

status=0
for scenario in "${scenarios[@]}"; do
  echo "::group::plbench run $scenario $profile $provider"
  "$bin" run -scenario "$scenario" -profile "$profile" -provider "$provider" -out "$out" || status=$?
  echo "::endgroup::"
  result=$(ls -t "$out/$scenario-$profile-$provider-"*.json 2>/dev/null | head -1 || true)
  if [ -z "$result" ]; then
    echo "no result written for $scenario"; status=1; continue
  fi
  echo "::group::plbench compare $result"
  "$bin" compare "$result" -format text -only-changed || status=$?
  echo "::endgroup::"
done
exit $status
