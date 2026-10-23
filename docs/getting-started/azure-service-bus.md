# Getting started: Azure Service Bus

`ESE.MessageQueue.AzureServiceBus` runs the shared pipeline on Azure Service Bus (Azure.Messaging.ServiceBus 7): a
queue, or a topic with one subscription, authenticated with Entra ID (recommended) or a connection string
([ADRs 0009-0012](../adr/0009-azure-service-bus-backoff-and-attempt-counting.md)). The provider's integration specs
have **not yet run against a real namespace**; that is the release blocker for 1.0.0 (see the
[release checklist](../release-checklist.md)). Its behavior is covered by unit specs over the SDK seams.

Every C# block on this page is compiled from `samples/ESE.MessageQueue.Samples/Docs/`, and the configuration examples
are validated, by `Packaging/Given_the_documentation_snippets`.

## 1. Install

```bash
dotnet add package ESE.MessageQueue.AzureServiceBus --prerelease    # brings ESE.MessageQueue (same version)
dotnet add package Microsoft.Extensions.Hosting
dotnet add package OpenTelemetry.Extensions.Hosting    # only for the telemetry step: AddOpenTelemetry()
```

The packages are versioned `1.0.0-rc.1` (pre-release); upgrade the core and provider packages together. If your feed
does not have them yet, build them with `dotnet pack ESE.MessageQueue.sln -c Release -o artifacts`.

## 2. Entities and permissions

The library never creates or changes entities: declare them in your infrastructure code. For a queue:

```bash
az servicebus queue create --resource-group my-rg --namespace-name contoso --name order-placed \
  --max-delivery-count 10 --lock-duration PT1M
```

Grant the application's identity the two data roles on the entity (or the namespace), never Owner or a Manage role:

```bash
queue_id=$(az servicebus queue show --resource-group my-rg --namespace-name contoso --name order-placed --query id -o tsv)
az role assignment create --assignee "$APP_PRINCIPAL_ID" --role "Azure Service Bus Data Sender" --scope "$queue_id"
az role assignment create --assignee "$APP_PRINCIPAL_ID" --role "Azure Service Bus Data Receiver" --scope "$queue_id"
```

| Role | Needed for |
|---|---|
| Azure Service Bus Data Sender | publishing; queue retries (a retry sends a scheduled copy); dead-letter replay |
| Azure Service Bus Data Receiver | consuming; the default health check (a peek); dead-letter peek, replay and purge |

For a topic, create the topic and a subscription, and grant Sender on the topic and Receiver on the subscription.
Keep the subscription's `MaxDeliveryCount` above `Retry:MaxRetries + 1`: on a subscription each retry is a redelivery
of the same message, so a lower limit dead-letters it (`MaxDeliveryCountExceeded`) before the policy is used up.

## 3. A message type and a processor

<!-- snippet: message -->
```csharp
public sealed record OrderPlaced(string OrderId, string CustomerId, decimal Amount);
```

<!-- snippet: processor -->
```csharp
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Processing;

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

`IOrderLedger` stands in for your own storage. A thrown exception counts as `Retry`.

## 4. Register

<!-- snippet: azureservicebus-registration -->
```csharp
using Azure.Core;
using Azure.Identity;
using ESE.MessageQueue.Extensions;
using ESE.MessageQueue.HealthChecks;
using ESE.MessageQueue.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

public static class OrderMessaging
{
    public static IServiceCollection AddOrderMessaging(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddAzureServiceBusProvider();                                 // first: makes Provider = "AzureServiceBus" selectable
        services.AddMessageQueue<OrderPlaced>(configuration);                  // binds and validates "MessageQueue"
        services.AddMessageProcessor<OrderPlaced, OrderPlacedProcessor>();     // scoped: one per message
        services.AddHostedService<MessageQueueHostedService<OrderPlaced>>();   // consumes while the host runs
        services.AddSingleton<IOrderLedger, InMemoryOrderLedger>();            // your own services
        services.AddSingleton<OrderService>();
        services.AddSingleton<OrderDeadLetters>();                             // operator tool (dead-letter peek and replay)

        // Reuses the queue's client: Unhealthy while its processor is not running, else a peek (Listen rights only).
        services.AddHealthChecks().AddAzureServiceBusHealthCheck<OrderPlaced>();
        return services;
    }

    // Optional, with FullyQualifiedNamespace: pin the identity instead of DefaultAzureCredential's search order.
    public static IServiceCollection AddOrderMessagingIdentity(this IServiceCollection services, string managedIdentityClientId)
    {
        services.AddSingleton<TokenCredential>(
            new ManagedIdentityCredential(ManagedIdentityId.FromUserAssignedClientId(managedIdentityClientId)));
        return services;
    }
}
```

`AddAzureServiceBusProvider()` must come before `AddMessageQueue<T>`. With `FullyQualifiedNamespace` the queue uses the
`TokenCredential` registered in the container, else `DefaultAzureCredential`. `AddOrderMessagingIdentity` shows how
to pin a user-assigned managed identity; a credential is ignored when the configuration uses a `ConnectionString`.

## 5. Configure

`appsettings.json` with Entra ID (production):

```json
{
  "MessageQueue": {
    "Provider": "AzureServiceBus",
    "FullyQualifiedNamespace": "contoso.servicebus.windows.net",
    "QueueName": "order-placed",
    "MaxConcurrentCalls": 16,
    "MaxAutoLockRenewalDuration": "00:10:00",
    "RequireDelayedRedelivery": true,
    "Retry": { "MaxRetries": 5, "InitialDelay": "00:00:02", "MaxDelay": "00:00:30" }
  }
}
```

The same from environment variables (for example App Service or Container Apps settings):

```bash
export MessageQueue__Provider=AzureServiceBus
export MessageQueue__FullyQualifiedNamespace=contoso.servicebus.windows.net
export MessageQueue__QueueName=order-placed
export MessageQueue__MaxConcurrentCalls=16
export MessageQueue__MaxAutoLockRenewalDuration=00:10:00
export MessageQueue__RequireDelayedRedelivery=true
```

`DefaultAzureCredential` reads its own settings too, for example `AZURE_CLIENT_ID` to choose a user-assigned managed
identity, or `az login` on a developer machine.

A topic and subscription. The queue publishes to the topic and consumes from the subscription; `QueueName` then only
names it in logs and telemetry:

```json
{
  "MessageQueue": {
    "Provider": "AzureServiceBus",
    "FullyQualifiedNamespace": "contoso.servicebus.windows.net",
    "QueueName": "billing-orders",
    "TopicName": "order-events",
    "SubscriptionName": "billing",
    "PrefetchCount": 0
  }
}
```

A subscription cannot schedule a delayed copy (it would reach every subscription), so a retry waits under the
message lock or, when the delay exceeds the lock budget, is redelivered at once (`Capabilities.DelayedRedelivery =
false`, warning 1101). With `RequireDelayedRedelivery = true` that configuration fails at startup instead.

The local emulator (accept its EULA yourself in `docker/.env`, then
`docker compose -f docker/docker-compose.dev.yml --profile servicebus up -d`) needs a connection string, and refuses
transactions:

```json
{
  "MessageQueue": {
    "Provider": "AzureServiceBus",
    "ConnectionString": "Endpoint=sb://localhost:5673;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;",
    "QueueName": "integration-test-queue",
    "TransactionalRetries": false
  }
}
```

The emulator cannot create entities at runtime; `docker/servicebus-emulator-config.json` declares them. Every setting
and startup error is in the [configuration reference](../configuration.md#azure-service-bus-azureservicebusconfig).

## 6. Program.cs

<!-- snippet: azureservicebus-program -->
```csharp
using ESE.MessageQueue.Samples.Docs;
using ESE.MessageQueue.Samples.Docs.AzureServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);   // appsettings.json, environment variables, command line
builder.Services.AddOrderMessaging(builder.Configuration);
builder.Services.AddOrderTelemetry();
// Give running handlers time to finish: more than MessageQueue:ShutdownTimeout (30 s by default).
builder.Services.Configure<HostOptions>(options => options.ShutdownTimeout = TimeSpan.FromSeconds(45));
using var host = builder.Build();
await host.RunAsync();
```

Keep the host's `ShutdownTimeout` above the queue's `ShutdownTimeout` (both 30 s by default), so the host does not
cancel the drain of running handlers.

## 7. Publish

<!-- snippet: publisher -->
```csharp
using ESE.MessageQueue.Abstractions;

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

Every send is bounded by `SendTimeout`. `PublishAsync(IEnumerable<T>)` sends batches of at most `MaxBatchSize`
messages, split by the entity's byte limit. Published messages carry `Subject` = the message type's name,
`ContentType` and `TimeToLive` = `MessageTimeToLive`.

## What the provider does

- **Connects on first use.** The client is created with the queue; the hosted service starts the processor during
  host start. Authentication and entity errors then surface as processor errors (log event 3009, metric
  `ese.messagequeue.transport.errors`) while the SDK keeps retrying; the health check reports them. (This follows the
  SDK's processor model and the provider's unit specs; it has not yet been observed against a real namespace.)
- **Queue retries** schedule a copy (`ScheduledEnqueueTime = now + delay`) and complete the original in one
  transaction (`TransactionalRetries`). Entities that refuse transactions (partitioned, Basic tier, emulator) fall back
  to send-then-complete after warning 3014.
- **Dead-letters** with the broker's dead-letter operation, reason = the library's reason code and the properties
  `x-ese-dead-letter-reason`, `x-ese-dead-letter-detail` (an exception type, never its message), `x-ese-dead-lettered-at`
  and `x-ese-dead-letter-attempt`.
- **Lock renewal**: the processor renews each message's lock for up to `MaxAutoLockRenewalDuration`; a handler that
  runs longer loses the lock and the message is redelivered (warning 3008).

Details: [failure modes](../operations/failure-modes.md), [runbooks](../operations/runbooks.md#azure-service-bus).

## Health checks

`AddAzureServiceBusHealthCheck<OrderPlaced>()` reuses the queue's client and identity: Unhealthy while the queue's
processor is not running, otherwise a peek on the consumed queue or subscription (Data Receiver is enough). The opt-in
`AddAzureServiceBusMetricsHealthCheck(...)` adds active, dead-letter and scheduled counts but needs **Manage** rights.
Results carry no namespace and no exception text; the exception goes to the log as event 6001.

## Telemetry

<!-- snippet: telemetry -->
```csharp
using ESE.MessageQueue.Extensions;
using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

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

Service Bus spans and metrics use `messaging.system = servicebus`, the queue (or topic) as destination and
`messaging.operation.name = send` on publish. See [monitoring](../operations/monitoring.md) and
[alerts](../operations/alerts.md), which also covers the entity's dead-letter count.

## Dead letters

<!-- snippet: dead-letters -->
```csharp
using ESE.MessageQueue.Abstractions;
using Microsoft.Extensions.Logging;

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

The registration above adds it as a singleton; or inject `IDeadLetterQueue<OrderPlaced>` directly.

Peek needs Listen (Data Receiver); replay needs Send and Listen; purge needs Listen. Replays work on queues only: on a
topic subscription `ReplayAsync` throws `NotSupportedException` (peek and purge work). The full procedure is in
[dead-letter operations](../dead-letter-operations.md).

## Next

- [Configuration reference](../configuration.md), [failure modes](../operations/failure-modes.md),
  [monitoring](../operations/monitoring.md), [alerts](../operations/alerts.md), [runbooks](../operations/runbooks.md)
