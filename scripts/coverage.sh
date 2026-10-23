#!/usr/bin/env bash
# Line-coverage report and gate for the shipped packages: ESE.MessageQueue, ESE.MessageQueue.RabbitMQ and
# ESE.MessageQueue.AzureServiceBus (samples, tests and benchmarks are excluded).
#
#   scripts/coverage.sh                 # run the full suite with coverage, report, enforce the gate
#   scripts/coverage.sh --skip-tests    # only merge existing TestResults/**/coverage.cobertura.xml (CI)
#
# The gate applies to the three assemblies together (all their lines in one ratio), which is the same measure the
# single assembly had before the Phase 7 package split. Each assembly's own figure is printed, and each must also stay
# at or above ASSEMBLY_FLOOR, so a provider cannot hide behind a well-covered core.
# Measured on the merged unit + integration run (Docker on, no MQ_ASB_CONNECTION_STRING); a unit-only run covers
# less. Ratchet the thresholds up, never down (OPTIMIZATION-PLAN.md section 5).
set -euo pipefail

THRESHOLD="${COVERAGE_THRESHOLD:-88}"
ASSEMBLY_FLOOR="${COVERAGE_ASSEMBLY_FLOOR:-86}"
RESULTS_DIR="${RESULTS_DIR:-TestResults}"
REPORT_DIR="$RESULTS_DIR/coverage-report"
ASSEMBLIES=(ESE.MessageQueue ESE.MessageQueue.RabbitMQ ESE.MessageQueue.AzureServiceBus)

cd "$(dirname "$0")/.."

if [[ "${1:-}" != "--skip-tests" ]]; then
  rm -rf "$RESULTS_DIR"
  dotnet test ESE.MessageQueue.sln --blame-hang-timeout 2m --collect:"XPlat Code Coverage" --results-directory "$RESULTS_DIR"
fi

filters="$(printf '+%s;' "${ASSEMBLIES[@]}")"
dotnet tool restore > /dev/null
dotnet tool run reportgenerator \
  "-reports:$RESULTS_DIR/**/coverage.cobertura.xml" \
  "-targetdir:$REPORT_DIR" \
  "-reporttypes:Html;TextSummary;MarkdownSummaryGithub;Cobertura" \
  "-assemblyfilters:${filters%;}" \
  "-verbosity:Warning"

summary="$REPORT_DIR/Summary.txt"
line_coverage="$(awk -F': *' '/^ *Line coverage:/ { sub(/%/, "", $2); print $2; exit }' "$summary")"
failed=0

for assembly in "${ASSEMBLIES[@]}"; do
  # Summary.txt lists "<assembly>  <line%>" per assembly, then its classes indented below it.
  value="$(awk -v name="$assembly" '$1 == name { sub(/%/, "", $2); print $2; exit }' "$summary")"
  if [[ -z "$value" ]]; then
    echo "::error::No coverage recorded for $assembly"
    failed=1
    continue
  fi
  echo "Line coverage ($assembly): ${value}% (floor: ${ASSEMBLY_FLOOR}%)"
  if awk -v actual="$value" -v gate="$ASSEMBLY_FLOOR" 'BEGIN { exit !(actual + 0 < gate + 0) }'; then
    echo "::error::Line coverage of $assembly ${value}% is below the ${ASSEMBLY_FLOOR}% per-assembly floor"
    failed=1
  fi
done

echo "Line coverage (all packages): ${line_coverage}% (gate: ${THRESHOLD}%)"
echo "HTML report: $REPORT_DIR/index.html"

if awk -v actual="$line_coverage" -v gate="$THRESHOLD" 'BEGIN { exit !(actual + 0 < gate + 0) }'; then
  echo "::error::Line coverage ${line_coverage}% is below the ${THRESHOLD}% gate"
  failed=1
fi
exit "$failed"
