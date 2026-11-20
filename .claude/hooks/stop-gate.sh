#!/usr/bin/env bash
# Stop hook: Claude cannot finish while the build or the unit tests are red.
#
# Runs `dotnet build -warnaserror` and `dotnet test --filter Category=Unit` (no Docker, no broker). Red: exit 2 with a
# short failure summary on stderr, which Claude Code feeds back so the agent keeps working. Green, or nothing to check:
# exit 0.
#
# Guards against loops and hangs:
#   - stop_hook_active=true (Claude is already continuing because of this hook): exit 0, so a failure the agent
#     cannot fix ends the turn instead of looping. Claude Code also caps consecutive stop-hook blocks on its own.
#     Without jq the field is read with grep.
#   - runs only when C#, project or build files differ from HEAD (staged, unstaged or untracked); otherwise exit 0
#     without building, since committed code already passed this gate.
#   - each step has a timeout (MQ_STOP_HOOK_BUILD_TIMEOUT / MQ_STOP_HOOK_TEST_TIMEOUT seconds; settings.json gives the
#     whole hook 600 s). A timeout counts as red.
set -uo pipefail

project_dir="${CLAUDE_PROJECT_DIR:-$(cd "$(dirname "$0")/../.." && pwd)}"
build_timeout="${MQ_STOP_HOOK_BUILD_TIMEOUT:-300}"
test_timeout="${MQ_STOP_HOOK_TEST_TIMEOUT:-240}"
input="$(cat)"

# stop_hook_active: jq when installed, else a grep for `"stop_hook_active": true`.
if command -v jq >/dev/null 2>&1; then
  active="$(printf '%s' "$input" | jq -r '.stop_hook_active // false' 2>/dev/null)"
else
  active="$(printf '%s' "$input" | grep -Eq '"stop_hook_active"[[:space:]]*:[[:space:]]*true' && echo true || echo false)"
fi
if [[ "$active" == "true" ]]; then
  exit 0
fi

cd "$project_dir" || exit 0
if [[ -z "$(git status --porcelain --untracked-files=all -- '*.cs' '*.csproj' '*.props' '*.targets' '*.sln' 'PublicAPI.*.txt' '.editorconfig' '*/packages.lock.json' 2>/dev/null)" ]]; then
  exit 0
fi

with_timeout() { perl -e 'alarm shift; exec @ARGV' "$@"; }
log="$(mktemp -t mq-stop-hook.XXXXXX)"
trap 'rm -f "$log"' EXIT

with_timeout "$build_timeout" dotnet build ZedLabs.MessageQueue.sln -warnaserror -nologo -v q -clp:NoSummary > "$log" 2>&1
status=$?
if [[ $status -ne 0 ]]; then
  {
    [[ $status -eq 142 ]] && echo "Stop gate: dotnet build timed out after ${build_timeout}s." \
      || echo "Stop gate: dotnet build -warnaserror failed. Fix these before finishing:"
    grep -E ': (error|warning) ' "$log" | sed -E "s#^$project_dir/##; s# \[[^]]*\]\$##" | sort -u | head -n 30
  } >&2
  exit 2
fi

with_timeout "$test_timeout" dotnet test ZedLabs.MessageQueue.sln --no-build --filter Category=Unit --blame-hang-timeout 2m -nologo > "$log" 2>&1
status=$?
if [[ $status -ne 0 ]]; then
  {
    [[ $status -eq 142 ]] && echo "Stop gate: unit tests timed out after ${test_timeout}s." \
      || echo "Stop gate: unit tests failed (dotnet test --filter Category=Unit). Fix these before finishing:"
    grep -E '^[[:space:]]+Failed |^(Passed|Failed)!|error' "$log" | sed -E 's/^[[:space:]]+//' | head -n 30
  } >&2
  exit 2
fi
exit 0
