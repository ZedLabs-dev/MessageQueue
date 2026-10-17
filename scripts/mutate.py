#!/usr/bin/env python3
"""Mutation testing in a scratch copy of the repository. The working tree is never modified.

    scripts/mutate.py scripts/mutants/dispatcher.json                 # every mutant in the file
    scripts/mutate.py scripts/mutants/dispatcher.json D01 D03         # only these ids
    scripts/mutate.py FILE --keep --workdir /tmp/mq-mut               # reuse / keep the scratch copy

A mutant definition file is JSON:

    {
      "description": "what this set probes",
      "defaults": { "filter": "Category=Unit", "project": "tests/ESE.MessageQueue.Tests" },
      "mutants": [
        { "id": "D01", "name": "retry counter not incremented",
          "file": "src/ESE.MessageQueue/Providers/MessageDispatcher.cs",
          "find": "exact text that must occur exactly once", "replace": "the mutated text",
          "filter": "optional dotnet test --filter, overrides the default" }
      ]
    }

Each mutant is applied to the copy, the test project is built (warnings allowed), the filtered tests are run, and
the file is restored. Results: KILLED (a test failed), SURVIVED (all passed: a gap in the specs), COMPILE-ERROR
(the mutant does not build: rewrite it), INVALID (find text not found exactly once). A baseline run must pass first.
Integration filters need Docker (MQ_REQUIRE_DOCKER=true is set). Exit code: 0 when every valid mutant was killed,
1 when any survived, 2 on a setup error. A JSON report is written next to the definition file's name in the workdir.
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

ROOT = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
MARKER = ".mq-mutate-workdir"
EXCLUDE = {"bin", "obj", "TestResults", ".git", ".idea", ".vs", "artifacts", "BenchmarkDotNet.Artifacts"}
# A copy without .git: SourceLink and deterministic CI settings would otherwise warn or fail.
BUILD_PROPS = ["-p:TreatWarningsAsErrors=false", "-p:EnableSourceLink=false", "-p:EnableSourceControlManagerQueries=false",
               "-p:ContinuousIntegrationBuild=false", "-p:EnablePackageValidation=false", "-nologo", "-v", "q"]


def inside(path, root):
    """True when `path`, with symlinks and `..` resolved, is `root` or below it."""
    path, root = os.path.realpath(path), os.path.realpath(root)
    return os.path.commonpath([path, root]) == root


def copy_repo(dest):
    def ignore(_, names):
        return [n for n in names if n in EXCLUDE]
    shutil.copytree(ROOT, dest, ignore=ignore, dirs_exist_ok=True)


def run(cmd, cwd, env=None, timeout=1800):
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, env=env, timeout=timeout)


def build(work, project):
    return run(["dotnet", "build", project, *BUILD_PROPS], work)


def test(work, project, test_filter):
    env = dict(os.environ, MQ_REQUIRE_DOCKER="true")
    result = run(["dotnet", "test", project, "--no-build", "--blame-hang-timeout", "2m", "--filter", test_filter], work, env)
    failed = [line.strip().split(" [")[0].removeprefix("Failed ESE.MessageQueue.Tests.")
              for line in result.stdout.splitlines() if line.strip().startswith("Failed ESE")]
    ran = any("Total:" in line for line in result.stdout.splitlines())
    return result.returncode, failed, ran, result.stdout[-1500:]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("definitions")
    parser.add_argument("ids", nargs="*", help="run only these mutant ids")
    parser.add_argument("--workdir", help="scratch copy location (default: a new temp directory)")
    parser.add_argument("--keep", action="store_true", help="keep the scratch copy afterwards")
    parser.add_argument("--no-copy", action="store_true", help="reuse an existing --workdir copy as is (skips the fresh copy)")
    args = parser.parse_args()

    with open(args.definitions) as f:
        spec = json.load(f)
    defaults = {"filter": "Category=Unit", "project": "tests/ESE.MessageQueue.Tests", **spec.get("defaults", {})}
    mutants = [m for m in spec["mutants"] if not args.ids or m["id"] in args.ids]
    if not mutants:
        print("No mutants selected", file=sys.stderr)
        return 2

    for m in mutants:
        if os.path.isabs(m["file"]) or not inside(os.path.join(ROOT, m["file"]), ROOT):
            print(f'Mutant {m["id"]}: file {m["file"]} is not a path inside the repository', file=sys.stderr)
            return 2

    work = os.path.realpath(args.workdir or tempfile.mkdtemp(prefix="mq-mutate-"))
    if inside(work, ROOT) or inside(ROOT, work):
        print(f"Refusing a work directory inside (or containing) the repository: {work}", file=sys.stderr)
        return 2
    if not args.no_copy:
        # A reused --workdir is emptied first, so no mutant from an earlier run can survive in it. Only
        # directories this script created (they carry MARKER) are ever deleted; any other non-empty one is refused.
        if os.path.isdir(work) and os.listdir(work):
            if not os.path.isfile(os.path.join(work, MARKER)):
                print(f"Refusing to empty {work}: it is not empty and was not created by this script", file=sys.stderr)
                return 2
            shutil.rmtree(work)
        print(f"Copying {ROOT} -> {work}", flush=True)
        copy_repo(work)
        with open(os.path.join(work, MARKER), "w") as marker:
            marker.write("Created by scripts/mutate.py; safe to delete.\n")

    try:
        project = defaults["project"]
        print("Baseline build and tests...", flush=True)
        if (b := build(work, project)).returncode != 0:
            print("Baseline does not build:\n" + b.stdout[-3000:], file=sys.stderr)
            return 2
        for test_filter in sorted({m.get("filter", defaults["filter"]) for m in mutants}):
            code, failed, ran, tail = test(work, project, test_filter)
            if code != 0 or not ran:
                print(f"Baseline tests fail for filter {test_filter}: {failed or tail}", file=sys.stderr)
                return 2

        results = []
        for m in mutants:
            path = os.path.realpath(os.path.join(work, m["file"]))
            if not inside(path, work):
                print(f'{m["id"]}: {m["file"]} resolves outside the work directory', file=sys.stderr)
                return 2
            original = open(path, encoding="utf-8").read()
            count = original.count(m["find"])
            label = f'{m["id"]} {m.get("name", "")}'.strip()
            if count != 1:
                results.append({"id": m["id"], "name": m.get("name"), "status": "INVALID", "detail": f"find text occurs {count} times"})
                print(f"{label}: INVALID ({count} matches)", flush=True)
                continue
            started = time.time()
            open(path, "w", encoding="utf-8").write(original.replace(m["find"], m["replace"]))
            try:
                b = build(work, m.get("project", project))
                if b.returncode != 0:
                    status, detail = "COMPILE-ERROR", b.stdout[-600:]
                else:
                    code, failed, ran, tail = test(work, m.get("project", project), m.get("filter", defaults["filter"]))
                    if code != 0 and failed:
                        status, detail = "KILLED", failed[:3]
                    elif code == 0 and ran:
                        status, detail = "SURVIVED", []
                    else:
                        status, detail = "ERROR", tail
            finally:
                open(path, "w", encoding="utf-8").write(original)
            results.append({"id": m["id"], "name": m.get("name"), "status": status, "detail": detail,
                            "seconds": round(time.time() - started)})
            print(f"{label}: {status} {detail if status == 'KILLED' else ''}", flush=True)

        report = os.path.join(work, os.path.splitext(os.path.basename(args.definitions))[0] + ".results.json")
        with open(report, "w") as f:
            json.dump(results, f, indent=1)
        counts = {s: sum(r["status"] == s for r in results) for s in ("KILLED", "SURVIVED", "COMPILE-ERROR", "INVALID", "ERROR")}
        print(f"Summary: {counts} (report: {report})")
        return 1 if counts["SURVIVED"] or counts["ERROR"] else 0
    finally:
        if not args.keep and not args.workdir:
            shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
