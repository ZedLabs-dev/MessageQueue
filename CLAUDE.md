# ESE.MessageQueue

Provider-agnostic event messaging library for .NET 10. Applications code against `IMessageQueue<T>`;
RabbitMQ, Azure Service Bus and an in-memory queue are interchangeable implementations.
Roadmap: `OPTIMIZATION-PLAN.md` (phased work), `PRODUCTION-READINESS-PLAN.md` (backlog).

## Commands

```bash
dotnet restore ESE.MessageQueue.sln --locked-mode          # CI restore; lock files must not drift
dotnet build ESE.MessageQueue.sln -warnaserror              # warnings are errors (also via Directory.Build.props)
dotnet test ESE.MessageQueue.sln                            # full suite
dotnet test ESE.MessageQueue.sln --filter "FullyQualifiedName!~Integration"   # fast loop; becomes --filter Category=Unit in Phase 3
dotnet format ESE.MessageQueue.sln                          # apply .editorconfig style
dotnet format ESE.MessageQueue.sln --verify-no-changes      # what CI runs
dotnet list ESE.MessageQueue.sln package --vulnerable --include-transitive   # must report none

cp MessageQueue/.env.example MessageQueue/.env              # once; .env is git-ignored
docker compose -f MessageQueue/docker-compose.dev.yml up -d --wait                       # RabbitMQ 4.x
docker compose -f MessageQueue/docker-compose.dev.yml --profile servicebus up -d          # + ASB emulator (ACCEPT_EULA=Y in .env)
```

Integration tests need RabbitMQ on `localhost:5672` (`admin`/`password123` today) and
`AZURE_SERVICEBUS_CONNECTION_STRING` for ASB. Until Phase 3, tests without a broker pass vacuously.

## Architecture

| Layer | Folder | Key types |
|---|---|---|
| Abstractions | `Abstractions/`, `Processing/` | `IMessageQueue<T>`, `IMessagePublisher<T>`, `IMessageSubscriber<T>`, `MessageContext`, `MessageResult`, `IMessageProcessor<T>` |
| Configuration | `Configuration/`, `Extensions/` | `MessageQueueConfig` (base record), `MessageQueueProvider`, `ServiceCollectionExtensions.AddMessageQueue<T>` |
| Providers | `RabbitMQ/`, `AzureServiceBus/`, `Testing/` | `RabbitMqMessageQueue<T>` + `PublisherChannel` pool; `AzureServiceBusMessageQueue<T>` + `PublisherSender` pool; `InMemoryMessageQueue<T>` |
| Cross-cutting | `Serialization/`, `Retry/`, `Exceptions/` | `MessageSerializer` (JSON / MessagePack, `UntrustedData`), `RetryPolicy`, `RetryExecutor`, `MessageQueueException` |
| Hosting & ops | `Hosting/`, `HealthChecks/`, `Telemetry/` | `MessageQueueHostedService<T>`, health checks per broker, `ITelemetryProvider` (`System.Diagnostics` Activity/Meter) |

Abstractions never reference a broker SDK. Provider-specific settings live on the provider config
(`RabbitMqConfig`, `AzureServiceBusConfig`), not on the base.

## Code conventions (enforced by `.editorconfig` + analyzers)

- File-scoped namespaces; `using` directives outside the namespace.
- Omit default accessibility: no `private` on members, no `internal` on top-level types.
- Fields: `readonly Type name;` camelCase, **no underscore prefix**. `static readonly`/`const`: PascalCase.
- `var` for locals. `this.` only to disambiguate (constructor assignments).
- Options/config classes: no XML comments; `[Required]`/`[Range]` attributes; a separate
  `IValidateOptions<T>` validator; `const` section names and defaults; register with `ValidateOnStart()`.
- Make only the requested change. No speculative features. Validate at system boundaries, trust internal code.
  Reuse existing abstractions. Mock only external dependencies.

## Testing: behavior-driven, spec first

1. Write the spec first. Run it and **see it fail** for the right reason before writing implementation.
2. Test classes are named for the behavior or context under test: `Given_a_rabbitmq_queue`, `PublishingMessages`.
3. Test methods: `When_<Condition_Or_Action>`, e.g. `When_Message_Is_Valid`, `When_ConnectionString_Is_Null`.
4. Assertions use FluentAssertions (AwesomeAssertions from Phase 3) with a `because` that starts with
   `"it should"` / `"it should not"` and describes behavior, so output reads "When X, it should Y":
   ```csharp
   result.Should().BeTrue("it should acknowledge a valid message");
   await act.Should().ThrowAsync<MessageQueueException>("it should not leak the connection password");
   ```
5. No `// Arrange` / `// Act` / `// Assert` comments.
6. Unit tests must not need a broker. Broker tests are integration tests (`[Trait("Category","Integration")]` from Phase 3).

## Hard rules

- **Never** early-`return`, catch-and-ignore, or loosen an assertion to make a test pass. A test that cannot
  run must be a real skip that shows up as skipped (`Assert.Skip*` from Phase 3), never a vacuous pass.
- **Never** add or bump a package outside Central Package Management: versions live only in
  `Directory.Packages.props`; `PackageReference` has no `Version`. Commit the updated `packages.lock.json`.
- **Never** suppress NU19xx (NuGet audit) warnings. Fix the vulnerable dependency instead.
- **Never** blanket-disable analyzers. Deferring a rule needs an `.editorconfig` entry with a comment naming the phase.
- Public API changes must update `PublicAPI.Unshipped.txt`. Those files and PublicApiAnalyzers arrive in Phase 7;
  until then, record every public-surface change in `CHANGELOG.md` under Unreleased.
- Work on `main` only (owner's directive): phases land as commits on `main`. No feature branches, worktrees or PRs
  unless the owner asks.
- Do not edit `.env` files or hand-edit lock files (regenerate them with `dotnet restore`).
- Do not modify `PRODUCTION-READINESS-PLAN.md` or `MessageQueue/docs/azure-service-bus-implementation-guide.md`.

## Definition of done

- [ ] Specs written first and observed failing; now green.
- [ ] `dotnet build ESE.MessageQueue.sln -warnaserror` is clean.
- [ ] `dotnet format ESE.MessageQueue.sln --verify-no-changes` passes.
- [ ] `dotnet test` reports 0 failed and **0 vacuous passes**; skips appear as skips.
- [ ] Coverage is at or above the gate (≥ 80% line on `MessageQueue/`, enforced from Phase 3).
- [ ] Vulnerable-package scan reports none.
- [ ] `CHANGELOG.md` Unreleased updated for any behavior or public-API change.
- [ ] Ticked the item in `OPTIMIZATION-PLAN.md` if the change completes one.
