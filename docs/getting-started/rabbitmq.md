# Getting started: RabbitMQ

`ESE.MessageQueue.RabbitMQ` runs the shared pipeline on RabbitMQ 4.x with RabbitMQ.Client 7: durable quorum queues,
publisher confirms, broker-side retry backoff and a dead-letter queue per queue
([ADRs 0006-0008](../adr/0006-rabbitmq-broker-side-backoff.md), [0022](../adr/0022-rabbitmq-client-certificates.md)).
The integration suite runs against RabbitMQ 4.3.6.

Every C# block on this page is compiled from `samples/ESE.MessageQueue.Samples/Docs/`, and the configuration examples
are validated, by `Packaging/Given_the_documentation_snippets`.

## 1. Install

```bash
dotnet add package ESE.MessageQueue.RabbitMQ --prerelease    # brings ESE.MessageQueue (same version)
dotnet add package Microsoft.Extensions.Hosting
dotnet add package OpenTelemetry.Extensions.Hosting    # only for the telemetry step: AddOpenTelemetry()
```

The packages are versioned `1.0.0-rc.1` (pre-release); upgrade the core and provider packages together. If your feed
does not have them yet, build them with `dotnet pack ESE.MessageQueue.sln -c Release -o artifacts`.

## 2. A broker

For development, the repository's compose file starts RabbitMQ 4.3.6 on `localhost` (choose a user and password in
`docker/.env` first):

```bash
cp docker/.env.example docker/.env
docker compose -f docker/docker-compose.dev.yml up -d --wait
```

For production, create a virtual host and a user that can touch only this application's queues. The library declares,
for a queue named `order-placed`: an exchange `order-placed` (unless `ExchangeName` is set), the queue
`order-placed`, the dead-letter queue `order-placed.deadletter`, and retry queues `order-placed.retry.<ms>` on first
use. It publishes retry and dead-letter copies through the default exchange. These permissions are enough for
publishing, consuming, retries, dead-lettering and the dead-letter operations (verified against RabbitMQ 4.3.6):

```bash
rabbitmqctl add_vhost orders
rabbitmqctl add_user orders-app '<password>'
rabbitmqctl set_permissions -p orders orders-app \
  '^order-placed(\..+)?$' \
  '^(order-placed(\..+)?|amq\.default)$' \
  '^order-placed(\..+)?$'
```

Configure, write and read, in that order. Write on `amq.default` is required: without it the queue declaration fails
with `ACCESS_REFUSED - write access to exchange 'amq.default'`. Widen the patterns if you set `ExchangeName` or
`DeadLetterQueueName` to names outside `order-placed.*`. The user needs no tags (no management access).

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

<!-- snippet: rabbitmq-registration -->
```csharp
using ESE.MessageQueue.Extensions;
using ESE.MessageQueue.HealthChecks;
using ESE.MessageQueue.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

public static class OrderMessaging
{
    public static IServiceCollection AddOrderMessaging(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddRabbitMqProvider();                                        // first: makes Provider = "RabbitMq" selectable
        services.AddMessageQueue<OrderPlaced>(configuration);                  // binds and validates "MessageQueue"
        services.AddMessageProcessor<OrderPlaced, OrderPlacedProcessor>();     // scoped: one per message
        services.AddHostedService<MessageQueueHostedService<OrderPlaced>>();   // consumes while the host runs
        services.AddSingleton<IOrderLedger, InMemoryOrderLedger>();            // your own services
        services.AddSingleton<OrderService>();
        services.AddSingleton<OrderDeadLetters>();                             // operator tool (dead-letter peek and replay)

        // Reuses the queue's connection: Unhealthy when it is closed or the consumer was cancelled by the broker.
        services.AddHealthChecks().AddRabbitMqHealthCheck<OrderPlaced>();
        return services;
    }
}
```

`AddRabbitMqProvider()` must come before `AddMessageQueue<T>`, which resolves the `Provider` setting at once.

## 5. Configure

`appsettings.json` for production (TLS through `Tls:Enabled`; the password comes from a secret, below):

```json
{
  "MessageQueue": {
    "Provider": "RabbitMq",
    "HostName": "rabbit.internal.example.com",
    "VirtualHost": "orders",
    "UserName": "orders-app",
    "Tls": { "Enabled": true },
    "QueueName": "order-placed",
    "PrefetchCount": 20,
    "MaxConcurrentCalls": 10,
    "RequireDelayedRedelivery": true,
    "Retry": { "MaxRetries": 5, "InitialDelay": "00:00:02", "MaxDelay": "00:00:30" }
  }
}
```

The same configuration from environment variables, with the password from your secret store:

```bash
export MessageQueue__Provider=RabbitMq
export MessageQueue__HostName=rabbit.internal.example.com
export MessageQueue__VirtualHost=orders
export MessageQueue__UserName=orders-app
export MessageQueue__Password="$RABBITMQ_PASSWORD"
export MessageQueue__Tls__Enabled=true
export MessageQueue__QueueName=order-placed
export MessageQueue__PrefetchCount=20
export MessageQueue__MaxConcurrentCalls=10
export MessageQueue__RequireDelayedRedelivery=true
```

Or as one URI (`amqps://` turns TLS on; percent-encode special characters in the password):

```bash
export MessageQueue__Provider=RabbitMq
export MessageQueue__ConnectionString="amqps://orders-app:s3cret@rabbit.internal.example.com/orders"
export MessageQueue__QueueName=order-placed
```

For the local compose broker, plain AMQP is allowed because the host is loopback:

```json
{
  "MessageQueue": {
    "Provider": "RabbitMq",
    "HostName": "localhost",
    "QueueName": "order-placed",
    "Retry": { "MaxRetries": 3, "InitialDelay": "00:00:01", "MaxDelay": "00:00:05" }
  }
}
```

with `MessageQueue__UserName` and `MessageQueue__Password` set to the user from `docker/.env`. TLS is required for
every other host unless `RequireTls` is explicitly `false`; client certificates (mutual TLS), the TLS settings and
every startup error are in the [configuration reference](../configuration.md#rabbitmq-rabbitmqconfig).

## 6. Program.cs

<!-- snippet: rabbitmq-program -->
```csharp
using ESE.MessageQueue.Samples.Docs;
using ESE.MessageQueue.Samples.Docs.RabbitMq;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);   // appsettings.json, environment variables, command line
builder.Services.AddOrderMessaging(builder.Configuration);
builder.Services.AddOrderTelemetry();
// Give running handlers time to finish: more than MessageQueue:ShutdownTimeout (25 s by default).
builder.Services.Configure<HostOptions>(options => options.ShutdownTimeout = TimeSpan.FromSeconds(45));
using var host = builder.Build();
await host.RunAsync();
```

The host's `ShutdownTimeout` (30 s by default in .NET 10) must exceed the queue's `ShutdownTimeout` (25 s by default),
so the host does not stop waiting while handlers drain; the hosted service logs warning 5005 at startup when it does
not. The host stops hosted services one after another, so with several queues it needs the sum of their timeouts:
45 s leaves room for one queue at its default and some headroom. Give your orchestrator's termination grace period a
few seconds more again.

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

Publishing connects on first use. With `EnablePublisherConfirms` (the default) `PublishAsync` returns once the broker
has confirmed the message; `PublishAsync(IEnumerable<T>)` publishes a batch and awaits the confirms together, about 34
times faster per message on a loopback broker ([benchmarks](../benchmarks.md)).

## What the provider does

- **Connects lazily.** Building the container does not connect. The hosted service subscribes during host start, so
  an unreachable broker or a topology conflict fails the start (`MessageQueueException`). Call
  `await queue.InitializeAsync()` to fail even earlier.
- **Declares its topology** on connect: a durable exchange (type `ExchangeType`) bound to the queue with routing key
  `QueueName`; quorum queues by default, the quorum dead-letter queue with `x-delivery-limit = -1`.
- **Retries broker-side.** A retry is a confirmed copy in `{QueueName}.retry.{ms}`, a queue whose TTL dead-letters it
  back to the main queue; the original is then acknowledged. No handler slot is held while it waits.
- **Dead-letters** a confirmed copy to `{QueueName}.deadletter` with the headers `x-ese-dead-letter-reason`,
  `x-ese-dead-letter-detail` (an exception type, never its message) and `x-ese-dead-lettered-at`.
- **Recovers connections** automatically (`NetworkRecoveryInterval`), redeclaring the topology.

Details: [failure modes](../operations/failure-modes.md), [runbooks](../operations/runbooks.md#rabbitmq).

## Health checks

`AddRabbitMqHealthCheck<OrderPlaced>()` reuses the queue's connection (connecting it if needed) and reports Unhealthy
while the connection is closed or, while subscribed, when the consumer channel is closed or the broker cancelled the
consumer. Results carry no host name and no exception text; the exception goes to the log as event 6001. Map the
endpoint as usual, for example `app.MapHealthChecks("/health/ready")`. A standalone check without a registered queue,
`AddRabbitMqHealthCheck(connectionString, tls)`, takes the same `RabbitMqTlsConfig` as the queue, so it can present a
client certificate to a broker that requires one.

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

RabbitMQ spans and metrics use `messaging.system = rabbitmq` and the destination `{exchange}:{queue}`
(`order-placed:order-placed` by default). See [monitoring](../operations/monitoring.md) and
[alerts](../operations/alerts.md), which also covers broker-side dead-letter queue depth.

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

Dead-letter operations need `EnableDeadLetterQueue = true` (the default). A peek on a quorum dead-letter queue moves
the messages it read to the end of that queue (order only). The full procedure, including the `delivery-limit` policy
for dead-letter queues created by earlier versions, is in [dead-letter operations](../dead-letter-operations.md).

## Next

- [Configuration reference](../configuration.md), [failure modes](../operations/failure-modes.md),
  [monitoring](../operations/monitoring.md), [alerts](../operations/alerts.md), [runbooks](../operations/runbooks.md)
