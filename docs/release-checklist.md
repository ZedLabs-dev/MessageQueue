# Release checklist (regression suite)

The suite that must pass before any release, and the record to keep of it. It is PRODUCTION-READINESS-PLAN §7
"regression test suite to run before releases". The `/release <version>` skill
(`.claude/skills/release/SKILL.md`) runs steps 1-5, 7, 10 and 11 as its gate (`scripts/coverage.sh` runs the full
suite); step 6 runs only when `MQ_ASB_CONNECTION_STRING` is set in that shell, and steps 8 and 9 are done by hand or
by CI. Record all of them below. `.github/workflows/release.yml` reruns CI (`ci.yml`) on the tag before it publishes.

A **final** version (`1.0.0`, no suffix) also needs every [release blocker](#release-blockers) closed. A pre-release
(`1.0.0-rc.N`) may ship with blockers open; they are listed in its CHANGELOG section.

## The suite

Run from the repository root, on `main`, with a clean working tree.

| # | Step | Command | Pass when |
|---|---|---|---|
| 1 | Locked restore | `dotnet restore ZedLabs.MessageQueue.sln --locked-mode` | no lock file drift |
| 2 | Build, both configurations | `dotnet build ZedLabs.MessageQueue.sln -warnaserror` and `dotnet build ZedLabs.MessageQueue.sln -warnaserror -c Release` | 0 warnings, 0 errors |
| 3 | Format | `dotnet format ZedLabs.MessageQueue.sln --verify-no-changes` | no changes |
| 4 | Unit specs | `dotnet test ZedLabs.MessageQueue.sln --blame-hang-timeout 2m --filter Category=Unit` | 0 failed. Includes the documentation specs (`Packaging/Given_the_documentation_snippets`, `Given_the_configuration_reference`, `Given_the_monitoring_reference`) and the public API and package-split specs |
| 5 | Integration specs, RabbitMQ | `MQ_REQUIRE_DOCKER=true dotnet test ZedLabs.MessageQueue.sln --blame-hang-timeout 2m --filter Category=Integration` | 0 failed; the RabbitMQ specs ran (Docker is required: missing Docker fails instead of skipping) |
| 6 | Integration specs, Azure Service Bus | step 5 with `MQ_ASB_CONNECTION_STRING` set, see [below](#azure-service-bus-suite) | the 11 specs of `Given_a_running_azure_service_bus` **passed**, not skipped. **Mandatory for a final release** |
| 7 | Coverage gate | `MQ_REQUIRE_DOCKER=true scripts/coverage.sh` | combined line coverage of the three packages ≥ 89% and each package ≥ 87% (`COVERAGE_THRESHOLD`, `COVERAGE_ASSEMBLY_FLOOR` in the script; never lower them) |
| 8 | Mutation sets | `for set in scripts/mutants/*.json; do scripts/mutate.py "$set" || break; done` | exit 0 for every set: every valid mutant killed or documented equivalent (355 mutants in 12 sets today, see [below](#mutation-sets)) |
| 9 | Linux | CI (`ci.yml`, `ubuntu-latest`) green on the release commit, or the [Linux container run](#linux) | the Linux-only specs passed |
| 10 | Vulnerable packages | `dotnet list ZedLabs.MessageQueue.sln package --vulnerable --include-transitive` | "no vulnerable packages" for every project |
| 11 | Pack | `dotnet pack ZedLabs.MessageQueue.sln -c Release -o artifacts` | three `.nupkg` and three `.snupkg` with the release version; the provider packages depend on exactly that core version (`unzip -p artifacts/ZedLabs.MessageQueue.RabbitMQ.*.nupkg '*.nuspec'`) |

Optional for performance-sensitive changes: the benchmarks with the default job
(`dotnet run -c Release --project benchmarks/ZedLabs.MessageQueue.Benchmarks -- --filter '*'`), compared with
[benchmarks.md](benchmarks.md).

### Azure Service Bus suite

The Service Bus integration specs talk to a real namespace (or the local emulator). They skip without
`MQ_ASB_CONNECTION_STRING`, so a green run without it proves nothing about Service Bus: check the test report shows
them **passed**.

| Variable | Default | Meaning |
|---|---|---|
| `MQ_ASB_CONNECTION_STRING` | (unset: the specs skip) | a connection string with Send and Listen on the entities below (Manage is not needed) |
| `MQ_ASB_QUEUE_NAME` | `integration-test-queue` | queue for the queue-mode specs |
| `MQ_ASB_TOPIC_NAME` | `integration-test-topic` | topic for the subscription-mode specs |
| `MQ_ASB_SUBSCRIPTION_NAME` | `integration-test-subscription` | its subscription |
| `MQ_ASB_TRANSACTIONAL_RETRIES` | `true` | `false` for entities that refuse transactions (emulator, Basic tier, partitioned) |

Create the entities as in `docker/servicebus-emulator-config.json`: lock duration 1 minute, `MaxDeliveryCount` 5,
default TTL 1 hour, no sessions, no duplicate detection. Run against a **Standard or Premium namespace** for the
release (the emulator does not support transactions, so it cannot exercise `TransactionalRetries`):

```bash
export MQ_ASB_CONNECTION_STRING='Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<rule>;SharedAccessKey=<key>'
MQ_REQUIRE_DOCKER=true dotnet test ZedLabs.MessageQueue.sln --blame-hang-timeout 2m --filter Category=Integration
```

In CI, set the repository secret `MQ_ASB_CONNECTION_STRING`; `ci.yml` passes it to the integration step. The emulator
(`docker compose -f docker/docker-compose.dev.yml --profile servicebus up -d`) is for development runs; it needs
`ACCEPT_EULA=Y` in `docker/.env`, which only the person accepting the license may set.

### Mutation sets

`scripts/mutate.py` copies the repository to a scratch directory, checks the baseline, then applies one mutant at a
time and runs the set's filtered specs. It never modifies the working tree. Sets whose filters include
`Category=Integration` need Docker.

| Set | Mutants | Area |
|---|---:|---|
| `dispatcher.json` | 9 | shared pipeline |
| `provider-registry.json` | 10 | provider selection and registration |
| `azure-service-bus.json` | 6 | Service Bus provider |
| `rabbitmq-broker.json` | 3 | RabbitMQ on an unreliable broker (Integration) |
| `phase7-fixes.json` | 17 | Phase 7 review fixes |
| `deferred-items.json` | 55 | bounded values, certificates, JSON metadata, capability checks |
| `review-deferred-items.json` | 37 | review of the above (1 documented equivalent; 2 killed on Linux only) |
| `review-deferred-items-2.json` | 2 | Packaging conventions |
| `dead-letter-ops.json` | 73 | dead-letter operations (1 documented equivalent) |
| `review-dead-letter-ops.json` | 63 | review of the above |
| `code-gaps.json` | 38 | publish size limit, exchange types, RabbitMQ transport errors, shutdown budget, publish logs, concurrency under load (some Integration) |
| `review-code-gaps.json` | 42 | review of the above: settlement, SafeLogger, log forging, shutdown advice (1 documented equivalent; some Integration) |

A survivor is a behavior no spec pins down: write the spec (it must kill the mutant) before releasing.

### Linux

`ci.yml` runs on `ubuntu-latest`. Two specs in `RabbitMQ/Given_a_rabbitmq_client_certificate_in_the_store` run only on
Linux (they add a certificate to `CurrentUser/My`, which only Linux keeps as plain files) and report Skipped elsewhere;
mutants R-T17 and R-T18 of `review-deferred-items.json` are killed only there. Without CI, run the unit suite in the
.NET SDK container, on a copy so that the container's restore does not overwrite your `bin/` and `obj/`:

```bash
docker run --rm -v "$PWD":/src:ro mcr.microsoft.com/dotnet/sdk:10.0 bash -c '
  mkdir /work &&
  tar -C /src --exclude=bin --exclude=obj --exclude=TestResults --exclude=artifacts --exclude=.git -cf - . | tar -C /work -xf - &&
  cd /work && dotnet test ZedLabs.MessageQueue.sln --blame-hang-timeout 2m --filter Category=Unit'
```

It reports 0 skipped when the Linux-only specs ran (verified 2026-10-23: 907 passed, 0 skipped).

## Release blockers

| Blocker | Closed when | Status |
|---|---|---|
| Azure Service Bus integration specs have never run against a real broker | step 6 passed against a real namespace, recorded below and in the CHANGELOG | **open** |
| CI has never run on GitHub | `ci.yml` green on `main` | **open** (needs the first push) |

## Record

Copy into the release's pull request or tag message:

```text
Release <version>, commit <sha>, <date>
1-3 restore/build/format ........ ok
4 unit ......................... <passed>/<total>, 0 failed
5 integration (RabbitMQ) ........ <passed> passed, <skipped> skipped
6 integration (Service Bus) ..... <passed>/11 against <namespace tier>   (or: not run - pre-release only)
7 coverage ...................... <combined>% (core <x>%, RabbitMQ <y>%, Service Bus <z>%)
8 mutation ...................... <killed>/<valid>, equivalents: <ids>
9 Linux ......................... CI run <url>
10 vulnerable packages ........... none
11 pack .......................... 3 nupkg + 3 snupkg, version <version>
```
