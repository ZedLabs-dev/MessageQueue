# Getting started: in-memory provider

The in-memory provider runs the full pipeline (serialization, size limit, retry with backoff, dead letters) inside
your process, with no broker. Use it for local development, tests and single-process tools. Nothing is durable:
queued messages, pending retries and dead letters are lost when the process stops
([ADR 0003](../adr/0003-in-memory-provider-guarantees.md)). Switching to RabbitMQ or Azure Service Bus later changes
configuration and one registration line, not your publisher or processor code
([RabbitMQ guide](rabbitmq.md), [Azure Service Bus guide](azure-service-bus.md)).

Every C# block on this page is compiled from `samples/ZedLabs.MessageQueue.Samples/Docs/` and the guide is executed end to
end by `Packaging/Given_the_documentation_snippets`.

## 1. Install

The provider is in the core package. .NET 10 is required.

```bash
dotnet add package ZedLabs.MessageQueue --prerelease
dotnet add package Microsoft.Extensions.Hosting
dotnet add package OpenTelemetry.Extensions.Hosting    # only for the telemetry step: AddOpenTelemetry()
```

The packages are versioned `1.0.0-rc.1` (pre-release). If your feed does not have them yet, build them with
`dotnet pack ZedLabs.MessageQueue.sln -c Release -o artifacts` and add `artifacts/` as a local package source.

## 2. A message type

Any class or record that System.Text.Json (or MessagePack, with `SerializationFormat = MessagePack`) can round-trip.
Properties are written in camelCase.

<!-- snippet: message -->
```csharp
public sealed record OrderPlaced(string OrderId, string CustomerId, decimal Amount);
```

## 3. A processor

The processor returns what should happen to the message: `Acknowledge`, `Retry` or `DeadLetter`. A thrown exception
counts as `Retry`. The hosted service creates a DI scope per message, so the processor can depend on scoped services
such as a `DbContext`.

<!-- snippet: processor -->
```csharp
using ZedLabs.MessageQueue.Abstractions;
using ZedLabs.MessageQueue.Processing;

public sealed class OrderPlacedProcessor : IMessageProcessor<OrderPlaced>
{
    readonly IOrderLedger ledger;

    public OrderPlacedProcessor(IOrderLedger ledger)   // scoped: a new DI scope, and processor, per message
    {
        this.ledger = ledger;
    }

    public async Task<MessageResult> ProcessAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken)
    {
        // Can never succeed: dead-letter it now, without retrying (reason code "Rejected").
        if (message.Amount <= 0)
            return MessageResult.DeadLetter;

        // Delivery is at-least-once, and a redelivered message keeps its MessageId: use it as the idempotency key,
        // recorded in the same transaction as the work. An id seen before is a duplicate: acknowledge it, do nothing.
        if (!await ledger.RecordOnceAsync(context.MessageId, message, cancellationToken))
            return MessageResult.Acknowledge;

        // Your work here. An exception thrown counts as MessageResult.Retry: redelivered after the retry policy's
        // backoff, and dead-lettered ("MaxRetriesExceeded") once Retry:MaxRetries retries have failed.
        return MessageResult.Acknowledge;
    }
}
```

## 4. Register

<!-- snippet: inmemory-registration -->
```csharp
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using ZedLabs.MessageQueue.Extensions;
using ZedLabs.MessageQueue.HealthChecks;
using ZedLabs.MessageQueue.Hosting;

public static class OrderMessaging
{
    public static IServiceCollection AddOrderMessaging(this IServiceCollection services, IConfiguration configuration)
    {
        // InMemory is built in: no provider to register. The section's Provider setting selects it.
        services.AddMessageQueue<OrderPlaced>(configuration);                  // binds and validates "MessageQueue"
        services.AddMessageProcessor<OrderPlaced, OrderPlacedProcessor>();     // scoped: one per message
        services.AddHostedService<MessageQueueHostedService<OrderPlaced>>();   // consumes while the host runs
        services.AddSingleton<IOrderLedger, InMemoryOrderLedger>();            // your own services
        services.AddSingleton<OrderService>();
        services.AddSingleton<OrderDeadLetters>();                             // operator tool (dead-letter peek and replay)

        // The in-memory queue has no connection to probe; report the host as able to publish.
        services.AddHealthChecks().AddMessageQueueHealthCheck("orders", _ => Task.FromResult(true));
        return services;
    }
}
```

`IOrderLedger` stands in for your own storage. `AddMessageQueue<T>` registers `IMessageQueue<T>`,
`IMessagePublisher<T>`, `IMessageSubscriber<T>` and `IDeadLetterQueue<T>` as one singleton.

## 5. Configure

`appsettings.json`:

```json
{
  "MessageQueue": {
    "Provider": "InMemory",
    "QueueName": "order-placed",
    "MaxConcurrentCalls": 4,
    "Retry": { "MaxRetries": 3, "InitialDelay": "00:00:01", "MaxDelay": "00:00:05" }
  }
}
```

The same settings as environment variables (`__` separates sections):

```bash
export MessageQueue__Provider=InMemory
export MessageQueue__QueueName=order-placed
export MessageQueue__MaxConcurrentCalls=4
export MessageQueue__Retry__MaxRetries=3
export MessageQueue__Retry__InitialDelay=00:00:01
export MessageQueue__Retry__MaxDelay=00:00:05
```

Every setting, default and startup error is in the [configuration reference](../configuration.md). Unknown keys and
out-of-range values stop the host at startup.

## 6. Program.cs

<!-- snippet: inmemory-program -->
```csharp
using Microsoft.Extensions.Hosting;
using ZedLabs.MessageQueue.Samples.Docs;
using ZedLabs.MessageQueue.Samples.Docs.InMemory;

var builder = Host.CreateApplicationBuilder(args);   // appsettings.json, environment variables, command line
builder.Services.AddOrderMessaging(builder.Configuration);
builder.Services.AddOrderTelemetry();
using var host = builder.Build();
await host.RunAsync();
```

## 7. Publish

Inject `IMessagePublisher<T>` wherever you publish:

<!-- snippet: publisher -->
```csharp
using ZedLabs.MessageQueue.Abstractions;

public sealed class OrderService
{
    readonly IMessagePublisher<OrderPlaced> publisher;

    public OrderService(IMessagePublisher<OrderPlaced> publisher)
    {
        this.publisher = publisher;
    }

    public Task PlaceAsync(OrderPlaced order, string checkoutId, CancellationToken cancellationToken) =>
        publisher.PublishAsync(order, new PublishOptions
        {
            MessageId = order.OrderId,                    // the idempotency key consumers see as MessageContext.MessageId
            CorrelationId = checkoutId,                   // at most 128 characters, no control characters
            Headers = new Dictionary<string, string> { ["tenant"] = "contoso" }   // names starting with "x-" are reserved
        }, cancellationToken);
}
```

`PublishAsync(order)` without options generates a version 7 GUID message id. `PublishAsync(IEnumerable<T>)` publishes
a batch.

## What happens to a message

1. The body is serialized on publish and queued; `PublishAsync` throws `MessageQueueException` instead for a body over
   `MaxMessageSizeBytes`. The consumer loop runs up to `MaxConcurrentCalls` handlers at once.
2. A body that does not deserialize is dead-lettered as `DeserializationFailed`.
3. Your processor runs. `Acknowledge` removes the message; `DeadLetter` dead-letters it as `Rejected`.
4. `Retry` (or an exception) waits for the backoff on a timer, then delivers it again with
   `MessageContext.DeliveryAttempt` + 1. After `Retry:MaxRetries` retries it is dead-lettered as `MaxRetriesExceeded`.

[Failure modes](../operations/failure-modes.md) covers the rest, including shutdown.

## Health checks

The in-memory queue has no connection to probe; the registration above uses the generic
`AddMessageQueueHealthCheck(name, check)`, which you can point at any condition. Expose the checks the usual way, for
example `app.MapHealthChecks("/health")` in ASP.NET Core.

## Telemetry

<!-- snippet: telemetry -->
```csharp
using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using ZedLabs.MessageQueue.Extensions;

public static class OrderTelemetry
{
    public const string SourceName = "MyApp.Messaging";

    public static IServiceCollection AddOrderTelemetry(this IServiceCollection services)
    {
        // The library's spans and metrics, reported through an ActivitySource and a Meter of this name.
        services.AddMessageQueueTelemetry(SourceName);

        // Your OpenTelemetry pipeline subscribes to the same name; add your exporter (OTLP, Prometheus, ...) here.
        services.AddOpenTelemetry()
            .WithTracing(tracing => tracing.AddSource(SourceName))
            .WithMetrics(metrics => metrics.AddMeter(SourceName));
        return services;
    }
}
```

Add an exporter to the OpenTelemetry builder (`.AddOtlpExporter()`, `.AddPrometheusExporter()`, ...; those packages
are yours to choose). Spans, metrics and log events are listed in [monitoring](../operations/monitoring.md).

## Dead letters

`IDeadLetterQueue<T>` peeks, replays and purges dead letters on every provider:

<!-- snippet: dead-letters -->
```csharp
using Microsoft.Extensions.Logging;
using ZedLabs.MessageQueue.Abstractions;

// Run from an operator tool, an admin endpoint or a one-off job, not from the message handler.
public sealed partial class OrderDeadLetters
{
    readonly IDeadLetterQueue<OrderPlaced> deadLetters;
    readonly ILogger<OrderDeadLetters> logger;

    public OrderDeadLetters(IDeadLetterQueue<OrderPlaced> deadLetters, ILogger<OrderDeadLetters> logger)
    {
        this.deadLetters = deadLetters;
        this.logger = logger;
    }

    // Look first: read-only snapshots with the reason code ("MaxRetriesExceeded", "Rejected", ...) and detail.
    public async Task<IReadOnlyList<DeadLetterMessage<OrderPlaced>>> PeekAsync(CancellationToken cancellationToken)
    {
        var letters = await deadLetters.PeekAsync(maxCount: 50, cancellationToken);
        foreach (var letter in letters)
            LogDeadLetter(logger, letter.MessageId, letter.ReasonCode, letter.ReasonDetail, letter.DeliveryAttempt);
        return letters;
    }

    // After fixing the cause: count with a dry run, then move them back as first deliveries with a full retry budget.
    public async Task<DeadLetterReplayResult> ReplayAsync(IReadOnlyCollection<string> reasonCodes, CancellationToken cancellationToken)
    {
        var preview = await deadLetters.ReplayAsync(
            new DeadLetterReplayOptions<OrderPlaced> { MaxCount = 500, ReasonCodes = reasonCodes, DryRun = true }, cancellationToken);
        LogPreview(logger, preview.Matched);

        var result = await deadLetters.ReplayAsync(
            new DeadLetterReplayOptions<OrderPlaced> { MaxCount = 500, ReasonCodes = reasonCodes }, cancellationToken);
        LogReplayed(logger, result.Replayed, result.Failed, result.Skipped);
        return result;
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Dead letter {MessageId}: {Reason} ({Detail}) on attempt {Attempt}")]
    static partial void LogDeadLetter(ILogger logger, string messageId, string? reason, string? detail, int? attempt);

    [LoggerMessage(Level = LogLevel.Information, Message = "A replay would move {Matched} dead letter(s) back to the queue")]
    static partial void LogPreview(ILogger logger, int matched);

    [LoggerMessage(Level = LogLevel.Information, Message = "Replayed {Replayed} dead letter(s): {Failed} failed, {Skipped} skipped")]
    static partial void LogReplayed(ILogger logger, int replayed, int failed, int skipped);
}
```

The registration above adds it as a singleton (`services.AddSingleton<OrderDeadLetters>()`); or inject
`IDeadLetterQueue<OrderPlaced>` directly. Call `ReplayAsync([DeadLetterReasons.MaxRetriesExceeded], token)` once the
cause is fixed. In tests, the queue's
`InMemoryMessageQueue<T>.DeadLetters` list holds the same entries. The full procedure is in
[dead-letter operations](../dead-letter-operations.md).

## Tests

Resolve `InMemoryMessageQueue<T>` and await `WaitForIdleAsync()` (nothing queued, running or waiting to retry) instead
of polling; then assert on `ProcessedMessages`, `DeadLetters` or your own state. Give a failing handler a millisecond
retry policy (`Retry:InitialDelay = 00:00:00.010`) so tests do not wait a minute for the default backoff.

## Next

- [RabbitMQ](rabbitmq.md) or [Azure Service Bus](azure-service-bus.md) for durable messaging
- [Configuration reference](../configuration.md), [failure modes](../operations/failure-modes.md),
  [monitoring](../operations/monitoring.md)
