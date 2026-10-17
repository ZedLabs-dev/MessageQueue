#!/usr/bin/env bash
# PostToolUse hook (Edit|Write|MultiEdit): formats the edited C# file with the repo's .editorconfig, so style drift is
# fixed as it happens instead of failing `dotnet format --verify-no-changes` later.
#
# Reads the hook JSON on stdin (tool_input.file_path; jq, or a grep fallback when jq is missing) and formats only that
# file through its owning project: `dotnet format <csproj> --include <file>` (whitespace, code style and analyzer
# fixes; 2-5 s measured; it does not apply the RS0016 fix, so PublicAPI files are never edited behind the agent's
# back). A file outside any project gets a whitespace-only pass. Files outside the repository (after resolving
# symlinks and `..`), non-.cs files and bin/obj are ignored.
#
# Exit codes (Claude Code hooks): 0 = done or nothing to do. 2 = formatting failed; stderr is shown to Claude (the
# edit itself already happened and stays). Never 1: that would only show the first stderr line.
set -uo pipefail

resolve() { python3 -c 'import os, sys; print(os.path.realpath(sys.argv[1]))' "$1"; }
field() {
  if command -v jq >/dev/null 2>&1; then
    printf '%s' "$input" | jq -r "$1 // empty" 2>/dev/null
  else
    # Fallback without jq: the first "file_path": "..." string (JSON escapes other than \" are not expected in paths).
    printf '%s' "$input" | grep -o '"file_path"[[:space:]]*:[[:space:]]*"[^"]*"' | head -n 1 | sed -E 's/.*:[[:space:]]*"(.*)"/\1/'
  fi
}

project_dir="$(resolve "${CLAUDE_PROJECT_DIR:-$(dirname "$0")/../..}")"
input="$(cat)"
file="$(field '.tool_input.file_path')"

[[ -n "$file" && "$file" == *.cs ]] || exit 0
[[ "$file" == /* ]] || file="$project_dir/$file"
[[ -f "$file" ]] || exit 0
file="$(resolve "$file")"
case "$file" in
  "$project_dir"/*) ;;
  *) exit 0 ;;                       # outside this repository
esac
case "$file" in */bin/*|*/obj/*) exit 0 ;; esac

# Owning project: the nearest *.csproj walking up from the file's folder, inside the repository.
dir="$(dirname "$file")"
project=""
while [[ "$dir" == "$project_dir" || "$dir" == "$project_dir"/* ]]; do
  candidate="$(find "$dir" -maxdepth 1 -name '*.csproj' -print -quit)"
  if [[ -n "$candidate" ]]; then project="$candidate"; break; fi
  [[ "$dir" == "$project_dir" ]] && break
  dir="$(dirname "$dir")"
done

relative="${file#"$project_dir"/}"
cd "$project_dir" || exit 0
if [[ -n "$project" ]]; then
  output="$(perl -e 'alarm shift; exec @ARGV' 60 dotnet format "$project" --include "$relative" --no-restore 2>&1)"
else
  output="$(perl -e 'alarm shift; exec @ARGV' 30 dotnet format whitespace --folder --include "$relative" 2>&1)"
fi
status=$?
if [[ $status -ne 0 ]]; then
  {
    [[ $status -eq 142 ]] && echo "dotnet format timed out on $relative." || echo "dotnet format could not format $relative (exit $status):"
    printf '%s\n' "$output" | tail -n 20
  } >&2
  exit 2
fi
exit 0
