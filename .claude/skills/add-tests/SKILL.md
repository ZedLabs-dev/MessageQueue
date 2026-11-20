---
name: add-tests
description: Find coverage gaps in one source file of ZedLabs.MessageQueue and close them with behavior-driven specs, each observed failing (or killing a mutant) before it counts.
argument-hint: "<path/to/File.cs>"
---

Add specs for: $ARGUMENTS

1. **Measure.** Run the suite with coverage (`MQ_REQUIRE_DOCKER=true scripts/coverage.sh`; a unit-only run is
   `dotnet test ZedLabs.MessageQueue.sln --filter Category=Unit --collect:"XPlat Code Coverage" --results-directory TestResults`
   then `scripts/coverage.sh --skip-tests`). Open `TestResults/coverage-report/index.html` (or grep the
   `Cobertura.xml` for the file) and list the uncovered lines and branches of the target file.
2. **Classify each gap:** a behavior worth pinning (write a spec), unreachable or defensive code (say so; consider
   removing it), or broker-only behavior (an Integration spec, RabbitMQ via Testcontainers; ASB through
   `Support/FakeServiceBus`).
3. **Write the specs** the way the `test-writer` agent does (CLAUDE.md "Testing"): `Given_<context>` class in
   `tests/ZedLabs.MessageQueue.Tests/<Area>/`, `When_<Condition>` methods, a Unit/Integration trait, AwesomeAssertions
   with "it should ..." reasons.
4. **Prove each spec bites.** A spec for new behavior must be seen failing first. A spec for existing behavior must
   kill a mutant of the line it covers: put a one-entry definition in `$TMPDIR/add-tests.json` and run
   `scripts/mutate.py $TMPDIR/add-tests.json` (it works in a scratch copy). A spec that kills nothing is deleted or
   rewritten.
5. **Gate:** `dotnet build ZedLabs.MessageQueue.sln -warnaserror`, `dotnet format ZedLabs.MessageQueue.sln --verify-no-changes`,
   the coverage script (never lower `COVERAGE_THRESHOLD` / `COVERAGE_ASSEMBLY_FLOOR`; raise them if the measurement
   allows, one point below the measured value).
6. Report the lines now covered, the specs added, and the red / mutant evidence for each.
