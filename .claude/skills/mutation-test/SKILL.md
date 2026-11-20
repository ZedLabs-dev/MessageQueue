---
name: mutation-test
description: Run mutation testing on ZedLabs.MessageQueue in a scratch copy with scripts/mutate.py - write or pick a mutant definition file, run it, and turn every surviving mutant into a missing spec.
argument-hint: "[definition file or area, e.g. scripts/mutants/dispatcher.json | src/ZedLabs.MessageQueue.RabbitMQ]"
---

Mutation-test: $ARGUMENTS

`scripts/mutate.py` copies the repository to a temporary directory (without bin, obj, TestResults and .git), checks
that the baseline builds and the selected tests pass, then applies one mutant at a time, builds, runs the filtered
tests and restores the file. The working tree is never touched. Run `scripts/mutate.py --help` for the file format.

1. **Pick or write definitions.** Existing sets: `scripts/mutants/provider-registry.json`,
   `scripts/mutants/dispatcher.json`, `scripts/mutants/azure-service-bus.json`. For a new area write
   `$TMPDIR/<area>.json` (or a new file under `scripts/mutants/` if it is worth keeping): one mutant per rule the
   code promises - drop a guard, flip a comparison or boolean, reorder awaits (send before complete), remove a
   `finally` / dispose, change a limit or constant, swallow an exception. `find` must occur exactly once in `file`.
2. **Choose filters.** Default `Category=Unit`; narrow with `&FullyQualifiedName~Given_x` for speed. Broker behavior
   needs `Category=Integration` (Docker; the script sets `MQ_REQUIRE_DOCKER=true`).
3. **Run.**
   ```bash
   scripts/mutate.py scripts/mutants/dispatcher.json            # all
   scripts/mutate.py scripts/mutants/dispatcher.json D02 D06    # some
   ```
   Exit 0: every valid mutant killed. Exit 1: something survived. Rewrite COMPILE-ERROR / INVALID mutants.
4. **Act on survivors.** Each SURVIVED mutant is a behavior no spec pins down: write the spec (it must kill that
   mutant; rerun with its id), or explain why the mutant is equivalent.
5. **Report** the score per file (killed / valid) and the specs added. Remove any containers you started.
