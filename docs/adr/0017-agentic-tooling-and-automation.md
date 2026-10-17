# 0017. Claude Code tooling and unattended automation

- Status: Accepted (2026-10-17)
- Context: OPTIMIZATION-PLAN §6.3, §6.4, Phase 7

## Decision

- **Hooks** (`.claude/settings.json`, scripts in `.claude/hooks/`):
  - `PostToolUse` on `Edit|Write|MultiEdit`: `format-cs.sh` runs `dotnet format <owning csproj> --include <file>` on
    the edited `.cs` file only (measured 2-5 s, under the 15 s budget, so the full format rather than whitespace only;
    it does not apply the RS0016 fix, so PublicAPI files are never edited behind the agent's back, checked with a
    probe); a file outside any project gets a whitespace pass. Paths are resolved (symlinks, `..`) before the
    repository check. A format failure exits 2, so Claude sees stderr; the edit itself stands.
  - `Stop`: `stop-gate.sh` runs `dotnet build -warnaserror` and the Unit tests; red exits 2 with a short summary on
    stderr, so the agent keeps working. It exits 0 when `stop_hook_active` is set (no loop on an unfixable failure),
    runs only when C# or build files differ from HEAD, reads the input with jq or a grep fallback, and bounds each
    step with a timeout (perl `alarm`; macOS has no `timeout`) inside the hook's 600 s limit.
- **Agents** (`.claude/agents/`): `test-writer` (spec first, red shown), `security-reviewer` (§2 checklist, scan,
  secrets grep, read-only), `package-updater` (CPM, release notes for majors, full gate), `adversarial-reviewer`.
- **Adversarial review before commit:** the method used since Phase 4 is codified: list the rules a change promises,
  write one mutant per rule, run `scripts/mutate.py` (a scratch copy, never the working tree; definitions in
  `scripts/mutants/`), probe concurrency, shutdown, hostile input and secret leaks there, and turn every survivor into
  a spec. Skills `update-packages`, `add-tests`, `release`, `mutation-test` give the step lists.
- **GitHub:** `claude.yml` (optional; skipped without `ANTHROPIC_API_KEY`): claude-code-action, pinned by SHA, answers
  `@claude` from owners, members and collaborators only and runs `/code-review` + `/security-review` on their pull
  requests when opened or ready (not on every push); nothing runs for Dependabot or forks, and checkouts keep no
  credentials. `update-packages.yml` runs the skill weekly in a read-only job with narrowed tools (named dotnet
  commands, WebFetch to nuget.org and github.com) and hands the diff to a second job that opens the pull request, the
  review vehicle while the owner works on `main`. A GitHub App token is preferred: `GITHUB_TOKEN` pull requests run CI
  only after a maintainer approves.

## Consequences

- An agent cannot end a turn with a red build or unit suite, but the Stop gate does not run integration tests,
  coverage or the vulnerable scan: the definition of done in CLAUDE.md still applies.
