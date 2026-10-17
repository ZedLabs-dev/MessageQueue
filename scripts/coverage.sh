#!/usr/bin/env bash
# Line-coverage report and gate for the ESE.MessageQueue assembly.
#
#   scripts/coverage.sh                 # run the full suite with coverage, report, enforce the gate
#   scripts/coverage.sh --skip-tests    # only merge existing TestResults/**/coverage.cobertura.xml (CI)
#
# The gate is measured on the merged unit + integration run (Docker on, no MQ_ASB_CONNECTION_STRING).
# A unit-only run covers less and will fall below it. Ratchet COVERAGE_THRESHOLD up, never down
# (target: 80, OPTIMIZATION-PLAN.md section 5).
set -euo pipefail

THRESHOLD="${COVERAGE_THRESHOLD:-84}"
RESULTS_DIR="${RESULTS_DIR:-TestResults}"
REPORT_DIR="$RESULTS_DIR/coverage-report"

cd "$(dirname "$0")/.."

if [[ "${1:-}" != "--skip-tests" ]]; then
  rm -rf "$RESULTS_DIR"
  dotnet test ESE.MessageQueue.sln --blame-hang-timeout 2m --collect:"XPlat Code Coverage" --results-directory "$RESULTS_DIR"
fi

dotnet tool restore > /dev/null
dotnet tool run reportgenerator \
  "-reports:$RESULTS_DIR/**/coverage.cobertura.xml" \
  "-targetdir:$REPORT_DIR" \
  "-reporttypes:Html;TextSummary;MarkdownSummaryGithub;Cobertura" \
  "-assemblyfilters:+ESE.MessageQueue" \
  "-verbosity:Warning"

LINE_COVERAGE="$(awk -F': *' '/^ *Line coverage:/ { sub(/%/, "", $2); print $2; exit }' "$REPORT_DIR/Summary.txt")"
echo "Line coverage (ESE.MessageQueue): ${LINE_COVERAGE}% (gate: ${THRESHOLD}%)"
echo "HTML report: $REPORT_DIR/index.html"

if awk -v actual="$LINE_COVERAGE" -v gate="$THRESHOLD" 'BEGIN { exit !(actual + 0 < gate + 0) }'; then
  echo "::error::Line coverage ${LINE_COVERAGE}% is below the ${THRESHOLD}% gate"
  exit 1
fi
