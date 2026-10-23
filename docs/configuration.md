# Configuration reference

Every setting of every provider, with its default, valid range and the error you get at startup when it is wrong.
The tables are checked against the code by `tests/ESE.MessageQueue.Tests/Packaging/Given_the_configuration_reference`
(every setting listed, defaults as constructed, error texts as produced), and every JSON and environment-variable
example on this page is bound and validated by `Given_the_documentation_snippets`.

Design records: [ADR 0002](adr/0002-provider-agnostic-config-and-options-validation.md) (portable base config,
validation), [ADR 0001](adr/0001-retry-semantics-and-default-policy.md) (retry), [ADR 0015](adr/0015-provider-spi-and-package-split.md)
(provider registry), [ADR 0020](adr/0020-required-delayed-redelivery.md) (`RequireDelayedRedelivery`).

## How configuration is read

```csharp
services.AddRabbitMqProvider();                           // providers first (InMemory is built in)
services.AddMessageQueue<OrderPlaced>(configuration);     // section "MessageQueue"
services.AddMessageQueue<PaymentReceived>(configuration, "Payments");   // another type, another section
```

- **One section per queue, one queue per message type.** `AddMessageQueue<T>(configuration, section = "MessageQueue")`
  reads one section. Its `Provider` setting picks the provider, and so the config class the rest of the section binds
  to (`InMemoryConfig`, `RabbitMqConfig`, `AzureServiceBusConfig`). A second `AddMessageQueue<T>` for the same `T`
  throws `ConfigurationException` ("A message queue for ... is already registered"). Different message types can use
  different sections and different providers in the same application.
- **`Provider` is required and resolved at registration.** There is no default broker. A missing or unknown name
  throws `ConfigurationException` from `AddMessageQueue<T>` itself, listing the registered providers. Names match
  case-insensitively. Register a broker's provider (`AddRabbitMqProvider()`, `AddAzureServiceBusProvider()`) **before**
  `AddMessageQueue<T>`.
- **Strict binding.** An unknown key in the section (a typo, or a removed setting such as `RetryAttempts`) fails at
  host start with `InvalidOperationException` instead of being ignored. So does a value that cannot be converted
  (`"SerializationFormat": "Xml"`).
- **Enum settings take names only.** `SerializationFormat`, `Retry:Strategy`, `Tls:AcceptablePolicyErrors` and the
  certificate store settings must be member names (case-insensitive; flags enums take comma-separated names). Numbers
  such as `"1"` fail at startup.
- **Validated at host start** (`ValidateOnStart`): DataAnnotations (`[Required]`, `[Range]`), the shared rules (retry
  ranges, serialization format) and the provider's own rules. All failures are reported together in one
  `OptionsValidationException`. A queue you construct yourself (`new RabbitMqMessageQueue<T>(config)`) runs the same
  validation in its constructor.
- **Read once.** A queue is a singleton created on first use from the validated settings; the consumer pipeline takes a
  snapshot. Changing `appsettings.json` or a secret while the process runs has **no effect** until the process restarts
  (see [runbooks](operations/runbooks.md#rolling-out-configuration-changes)).
- **Code-only settings.** `JsonTypeInfoResolver`, `Retry:CustomDelayCalculator` and `Retry:IsRetryable` are delegates or
  objects and cannot come from configuration. Set `JsonTypeInfoResolver` with
  `services.AddMessageQueueJsonMetadata<T>(MyJsonContext.Default)`
  ([ADR 0019](adr/0019-source-generated-json-metadata.md)); the retry delegates are for queues you construct in code.

### Value formats

| Kind | Write it as | Example |
|---|---|---|
| `TimeSpan` | `[d.]hh:mm:ss[.fffffff]` (invariant culture) | `"00:00:30"`, `"14.00:00:00"`. A bare number is **days**: `"30"` is 30 days |
| `bool` | `true` / `false` | `"RequireTls": false` |
| enum | a member name; flags: names separated by commas | `"MessagePack"`, `"RemoteCertificateNameMismatch, RemoteCertificateChainErrors"` |
| nested section | a JSON object, or `:` in keys | `"Retry": { "MaxRetries": 3 }` or `MessageQueue:Retry:MaxRetries` |

### Environment variables

The .NET host maps `__` (double underscore) to `:`. Every setting below can come from an environment variable, which
is how secrets and per-environment values usually arrive (Kubernetes secrets, App Service settings):

```bash
export MessageQueue__Provider=RabbitMq
export MessageQueue__QueueName=order-placed
export MessageQueue__HostName=rabbit.internal.example.com
export MessageQueue__Tls__Enabled=true
export MessageQueue__UserName=orders-app
export MessageQueue__Password="from-your-secret-store"
export MessageQueue__Retry__MaxRetries=5
export MessageQueue__Retry__InitialDelay=00:00:02
```

A section other than `MessageQueue` uses its own prefix (`Payments__Provider`, ...). Environment variables override
`appsettings.json` and `appsettings.{Environment}.json` with the default host builder.

### Several queues

```json
{
  "Orders": {
    "Provider": "RabbitMq",
    "ConnectionString": "amqps://rabbit.example.com/orders",
    "QueueName": "order-placed",
    "MaxConcurrentCalls": 20,
    "PrefetchCount": 40
  },
  "Payments": {
    "Provider": "AzureServiceBus",
    "FullyQualifiedNamespace": "contoso.servicebus.windows.net",
    "QueueName": "payment-received",
    "RequireDelayedRedelivery": true
  }
}
```

```csharp
services.AddRabbitMqProvider().AddAzureServiceBusProvider();
services.AddMessageQueue<OrderPlaced>(configuration, "Orders");
services.AddMessageQueue<PaymentReceived>(configuration, "Payments");
```

Each queue has its own connection (RabbitMQ) or client (Service Bus). Two message types should not share one broker
queue: each queue's consumer would receive the other type's messages and dead-letter them as `DeserializationFailed`.

## Base settings: `MessageQueueConfig` (every provider)

| Setting | Type | Default | Valid values | Required | Notes |
|---|---|---|---|---|---|
| `Provider` | string | (required) | a registered provider name: `InMemory`, `RabbitMq`, `AzureServiceBus` | yes | Resolved at registration; selects the config class below |
| `QueueName` | string | (empty) | non-empty; RabbitMQ: at most 240 UTF-8 bytes | yes | The queue published to and consumed from. Service Bus topic mode: only names the queue in logs and telemetry |
| `Retry` | section | (section) | see [`RetryPolicy`](#retry-section-retrypolicy) | yes (has defaults) | Retry and backoff |
| `SerializationFormat` | enum | `Json` | `Json`, `MessagePack` | no | Wire format; publishers and consumers of a queue must agree |
| `EnableDeadLetterQueue` | bool | `true` | | no | When `false`, a message that would be dead-lettered is **dropped** (acknowledged) with warning 1004; RabbitMQ then declares no dead-letter queue and has no dead-letter operations |
| `RequireDelayedRedelivery` | bool | `false` | | no | `true`: startup fails when the provider or entity cannot delay a retry (`Capabilities.DelayedRedelivery = false`, a Service Bus topic subscription) and `Retry` has a backoff. `false`: that case only logs warning 1101 |
| `MaxMessageSizeBytes` | int | `1048576` | 1 to 2147483647 | no | Larger bodies are dead-lettered as `MessageTooLarge` without being deserialized |
| `MaxConcurrentCalls` | int | `10` | 1 to 1000 | no | Handlers running at once. RabbitMQ: also capped by `PrefetchCount` (warning 2010) |
| `JsonTypeInfoResolver` | `IJsonTypeInfoResolver` | (unset) | code only | no | Source-generated JSON metadata (`AddMessageQueueJsonMetadata<T>`); requires `SerializationFormat` `Json` |

## Retry section: `RetryPolicy`

Keys are `Retry:<Setting>`. A message is handled at most `MaxRetries + 1` times, then dead-lettered as
`MaxRetriesExceeded`. The delay before retry `n` follows `Strategy` from `InitialDelay`, gets ±`JitterFactor` jitter
when `UseJitter` is set, and is capped at `MaxDelay`. The default waits about 2 + 4 + 8 + 16 + 30 s, about a minute,
before it gives up.

| Setting | Type | Default | Valid values | Required | Notes |
|---|---|---|---|---|---|
| `Retry:Strategy` | enum | `Exponential` | `Fixed`, `Linear`, `Exponential`, `Custom` | no | `Fixed`: `InitialDelay` each time; `Linear`: `InitialDelay × n`; `Exponential`: `InitialDelay × BackoffMultiplier^(n-1)`; `Custom` needs `CustomDelayCalculator` (code) |
| `Retry:MaxRetries` | int | `5` | 0 to 1000 | no | Redeliveries after the first attempt; `0` dead-letters on the first failure |
| `Retry:InitialDelay` | TimeSpan | `00:00:02` | ≥ 0 | no | Delay before the first retry; `0` with `Fixed` retries at once |
| `Retry:MaxDelay` | TimeSpan | `00:00:30` | `InitialDelay` to `1.00:00:00` | no | Cap on every delay |
| `Retry:BackoffMultiplier` | double | `2` | 1 to 100 | no | `Exponential` only |
| `Retry:UseJitter` | bool | `true` | | no | Spreads retries of many messages apart |
| `Retry:JitterFactor` | double | `0.1` | 0 to 1 | no | ±10% by default; never above `MaxDelay` |
| `Retry:CustomDelayCalculator` | `Func<int, TimeSpan>` | (unset) | code only | with `Custom` | A throwing calculator falls back to `InitialDelay` (warning 1006) |
| `Retry:IsRetryable` | `Func<Exception, bool>` | (unset) | code only | no | `false` dead-letters at once as `NonRetryableError`; unset: every exception is retried |

How the retry waits depends on the provider: the in-memory queue waits on a timer, RabbitMQ parks a copy in a
per-delay retry queue (`{QueueName}.retry.{ms}`, [ADR 0006](adr/0006-rabbitmq-broker-side-backoff.md)), a Service Bus
queue schedules a copy, and a Service Bus subscription waits under the message lock or abandons at once
([ADR 0009](adr/0009-azure-service-bus-backoff-and-attempt-counting.md)).

## In-memory: `InMemoryConfig`

`Provider = InMemory`. Single process, nothing durable ([ADR 0003](adr/0003-in-memory-provider-guarantees.md)): for
local development, tests and single-process tools.

| Setting | Type | Default | Valid values | Required | Notes |
|---|---|---|---|---|---|
| `ShutdownTimeout` | TimeSpan | `00:00:30` | > 0 | no | How long unsubscribe and dispose wait for running handlers |

```json
{
  "MessageQueue": {
    "Provider": "InMemory",
    "QueueName": "order-placed",
    "Retry": { "MaxRetries": 3, "InitialDelay": "00:00:01", "MaxDelay": "00:00:05" }
  }
}
```

## RabbitMQ: `RabbitMqConfig`

`Provider = RabbitMq`, package `ESE.MessageQueue.RabbitMQ`. Set **exactly one** endpoint form: `ConnectionString`, or
`HostName` with the separate `Port`, `UserName`, `Password` and `VirtualHost`.

| Setting | Type | Default | Valid values | Required | Notes |
|---|---|---|---|---|---|
| `ConnectionString` | string | (empty) | absolute `amqp://` or `amqps://` URI | one endpoint form | `amqps://user:password@host:port/vhost`; percent-encode special characters in the password. `amqps://` turns TLS on and checks the server certificate against the URI's host. Redacted from `ToString()` |
| `HostName` | string | (empty) | a host name or IP | one endpoint form | The alternative to `ConnectionString` |
| `Port` | int | `0` | 0 to 65535 | no | `0`: 5672, or 5671 with `Tls:Enabled`. `HostName` form only |
| `UserName` | string | (empty) | | no | Empty: the client's default (`guest`, which RabbitMQ accepts only from localhost). `HostName` form only |
| `Password` | string | (empty) | | no | Redacted from `ToString()`, never logged. `HostName` form only |
| `VirtualHost` | string | (empty) | | no | Empty: `/`. `HostName` form only |
| `RequireTls` | bool? | (unset) | `true`, `false` | no | Unset: TLS is required for every host except `localhost`, `127.0.0.0/8` and `::1`. Set `false` only for a trusted network |
| `Tls` | section | (section) | see [`RabbitMqTlsConfig`](#rabbitmq-tls-rabbitmqtlsconfig) | no | |
| `DeadLetterQueueName` | string | (empty) | at most 255 UTF-8 bytes | no | Empty: `{QueueName}.deadletter` |
| `UseQuorumQueues` | bool | `true` | | no | Quorum main, dead-letter and retry queues (replicated, at-least-once dead-lettering). `false`: classic queues |
| `QuorumInitialGroupSize` | int | `3` | 1 to 2147483647 | no | Replicas of each new quorum queue (RabbitMQ uses at most the cluster's node count) |
| `PrefetchCount` | int | `10` | 1 to 65535 | no | Unacknowledged deliveries per consumer; keep it ≥ `MaxConcurrentCalls` |
| `ExchangeName` | string | (empty) | at most 255 UTF-8 bytes | no | Empty: `QueueName`. The queue is bound to it with routing key `QueueName` |
| `ExchangeType` | string | `direct` | `direct`, `fanout`, `topic`, `headers` | yes | Only checked for non-empty at startup; an unknown type fails when the broker declares it |
| `EnablePublisherConfirms` | bool | `true` | | no | `PublishAsync` waits for the broker's confirm. Retry and dead-letter copies are always confirmed |
| `PublisherChannelPoolSize` | int | `2` | 1 to 64 | no | Publisher channels, used round-robin |
| `MaxOutstandingConfirms` | int | `1000` | 1 to 2147483647 | no | Unconfirmed publishes per channel; further publishes wait |
| `ConfirmTimeout` | TimeSpan | `00:00:30` | > 0 | no | Longest wait for publish capacity plus the confirm |
| `ShutdownTimeout` | TimeSpan | `00:00:30` | > 0 | no | Longest wait for running handlers on unsubscribe or dispose |
| `HeartbeatInterval` | TimeSpan | `00:01:00` | ≥ 0 | no | AMQP heartbeat; `0` disables it |
| `NetworkRecoveryInterval` | TimeSpan | `00:00:10` | > 0 | no | Wait between automatic reconnection attempts after a connection is lost |

### RabbitMQ TLS: `RabbitMqTlsConfig`

Keys are `Tls:<Setting>` ([ADR 0008](adr/0008-rabbitmq-tls-and-consumer-shutdown.md),
[ADR 0022](adr/0022-rabbitmq-client-certificates.md)).

| Setting | Type | Default | Valid values | Required | Notes |
|---|---|---|---|---|---|
| `Tls:Enabled` | bool | `false` | | no | TLS for the `HostName` form; with a `ConnectionString` use `amqps://` instead |
| `Tls:ServerName` | string | (empty) | | no | Name the server certificate must match; empty: the host |
| `Tls:AcceptablePolicyErrors` | `SslPolicyErrors` flags | `None` | `None`, `RemoteCertificateNotAvailable`, `RemoteCertificateNameMismatch`, `RemoteCertificateChainErrors` | no | Server-certificate errors to accept; anything but `None` also needs `Tls:AllowInsecureCertificates` |
| `Tls:AllowInsecureCertificates` | bool | `false` | | no | Confirms `AcceptablePolicyErrors`; development only |
| `Tls:ClientCertificatePath` | string | (empty) | a readable PKCS#12 (`.pfx`/`.p12`) or PEM file | no | Mutual TLS. Loaded at startup: it must exist and hold a private key |
| `Tls:ClientCertificateKeyPath` | string | (empty) | a PEM key file | no | When the PEM private key is not in the certificate file |
| `Tls:ClientCertificatePassword` | string | (empty) | | no | PFX password or encrypted PEM key password; redacted, never logged |
| `Tls:ClientCertificateThumbprint` | string | (empty) | 40 hex characters (`:`, `-`, spaces ignored) | no | The alternative to a file: a certificate with a private key in the OS store |
| `Tls:ClientCertificateStoreName` | `StoreName` | `My` | a `StoreName` member | no | Store searched for the thumbprint |
| `Tls:ClientCertificateStoreLocation` | `StoreLocation` | `CurrentUser` | `CurrentUser`, `LocalMachine` | no | |

```json
{
  "MessageQueue": {
    "Provider": "RabbitMq",
    "HostName": "rabbit.internal.example.com",
    "VirtualHost": "orders",
    "UserName": "orders-app",
    "Tls": { "Enabled": true, "ServerName": "rabbit.internal.example.com" },
    "QueueName": "order-placed",
    "PrefetchCount": 20,
    "MaxConcurrentCalls": 10,
    "RequireDelayedRedelivery": true
  }
}
```

(`Password` comes from a secret: `MessageQueue__Password`.)

## Azure Service Bus: `AzureServiceBusConfig`

`Provider = AzureServiceBus`, package `ESE.MessageQueue.AzureServiceBus`. Set **exactly one** of
`FullyQualifiedNamespace` (Entra ID, recommended) and `ConnectionString` (shared access key).

| Setting | Type | Default | Valid values | Required | Notes |
|---|---|---|---|---|---|
| `ConnectionString` | string | (empty) | `Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=...` or `...;SharedAccessSignature=...` | one of the two | Development and the emulator. Redacted from `ToString()`; never echoed in errors |
| `FullyQualifiedNamespace` | string | (empty) | a host name (`contoso.servicebus.windows.net`), no scheme or path | one of the two | Entra ID: a `TokenCredential` registered in the container, else `DefaultAzureCredential` |
| `MessageTimeToLive` | TimeSpan | `14.00:00:00` | > 0 | no | Set on every published message; the entity's own default TTL caps it |
| `PrefetchCount` | int | `10` | 0 to 2147483647 | no | Receiver prefetch. On a topic subscription prefer `0` (warning 3015) |
| `MaxAutoLockRenewalDuration` | TimeSpan | `00:05:00` | ≥ 0 | no | How long the processor renews a message's lock while the handler runs; also a subscription retry's wait budget |
| `EnableBatchedOperations` | bool | `true` | | no | `PublishAsync(IEnumerable<T>)` sends batches; `false`: one send per message |
| `MaxBatchSize` | int | `100` | 1 to 2147483647 | no | Messages per batch (also split by byte size) |
| `TopicName` | string? | (unset) | | with `SubscriptionName` | Publish to this topic and consume from `SubscriptionName` instead of a queue |
| `SubscriptionName` | string? | (unset) | | with `TopicName` | |
| `SendTimeout` | TimeSpan | `00:00:30` | > 0 | no | Bounds every send and settlement |
| `ShutdownTimeout` | TimeSpan | `00:00:30` | > 0 | no | Longest wait for running handlers on unsubscribe or dispose |
| `TransactionalRetries` | bool | `true` | | no | Queue retries send the scheduled copy and complete the original in one transaction. Set `false` for partitioned entities, the Basic tier and the emulator (they refuse; the queue falls back after warning 3014 either way) |

```json
{
  "MessageQueue": {
    "Provider": "AzureServiceBus",
    "FullyQualifiedNamespace": "contoso.servicebus.windows.net",
    "QueueName": "order-placed",
    "MaxConcurrentCalls": 16,
    "PrefetchCount": 0,
    "MaxAutoLockRenewalDuration": "00:10:00",
    "RequireDelayedRedelivery": true
  }
}
```

Entity settings (`MaxDeliveryCount`, lock duration, partitioning, duplicate detection, the entity's default TTL) belong
to the entity's infrastructure definition, not to this library. Keep `MaxDeliveryCount` above `Retry:MaxRetries + 1`
on topic subscriptions (warning 3012 when the policy allows more than the default 10).

## Production and development

| Concern | Development | Production |
|---|---|---|
| Provider | `InMemory`, or a local broker (`docker/docker-compose.dev.yml`) | `RabbitMq` or `AzureServiceBus`; the setting is required, there is no fallback |
| RabbitMQ transport | `amqp://localhost` is allowed (TLS is only required off-loopback) | `amqps://` (or `Tls:Enabled`); never `RequireTls = false` across an untrusted network |
| Certificate errors | `Tls:AcceptablePolicyErrors` plus `Tls:AllowInsecureCertificates = true` for a self-signed broker | `None`; trust the broker's CA in the OS store instead |
| Service Bus identity | `ConnectionString` (the emulator needs one) | `FullyQualifiedNamespace` with a managed identity holding only **Azure Service Bus Data Sender** and **Data Receiver** |
| Secrets | user secrets or a git-ignored `.env` | environment variables or a secret store; never in a committed `appsettings*.json`. Config `ToString()` redacts `ConnectionString`, `Password` and `Tls:ClientCertificatePassword` |
| Backoff guarantee | warning 1101 is enough | `RequireDelayedRedelivery = true` so a configuration that cannot delay retries (a Service Bus subscription) fails at startup instead of retrying hot |
| Invalid configuration | fails at startup the same way | fails at startup: deploy with a readiness check so a failing release never takes traffic |
| Typos, removed settings, enum numbers | fail at startup | fail at startup |

Behavior for missing or invalid configuration is the same in every environment: the library never falls back to a
default provider, endpoint or credential. Where each problem surfaces:

| Problem | When | Exception |
|---|---|---|
| `Provider` missing or not registered; a second queue for the same type | `AddMessageQueue<T>` (registration) | `ConfigurationException` |
| A value out of range, a broken rule, an enum given as a number | host start | `OptionsValidationException` (all failures in one message) |
| An unknown key, a value that cannot be converted | host start | `InvalidOperationException` (from the configuration binder) |
| Client certificate missing, unreadable or without a private key | host start | `OptionsValidationException` |
| Broker unreachable, credentials refused, a queue argument mismatch | first use: RabbitMQ connects on the first publish or subscribe (or `InitializeAsync`); Service Bus on the first send or when the processor starts | `MessageQueueException` (RabbitMQ; the hosted service's subscribe fails the host start); Service Bus processor errors are logged (event 3009) and retried by the SDK |

## Startup errors

The text each invalid value produces, quoted from the library. Messages from DataAnnotations and the configuration
binder are wrapped by .NET (`DataAnnotation validation failed for 'RabbitMqConfig' members: ...`); the quoted part is
what to search for.

| Provider | Setting | Message |
|---|---|---|
| InMemory | `Provider=` | `MessageQueue:Provider is required. Registered providers: InMemory, RabbitMq, AzureServiceBus` |
| InMemory | `Provider=Kafka` | `MessageQueue:Provider 'Kafka' is not registered. Registered providers: InMemory, RabbitMq, AzureServiceBus. Add the provider's package and register it (for example services.AddRabbitMqProvider()) before AddMessageQueue<T>.` |
| InMemory | `QueueName=` | `The QueueName field is required.` |
| InMemory | `MaxConcurrentCalls=0` | `The field MaxConcurrentCalls must be between 1 and 1000.` |
| InMemory | `MaxMessageSizeBytes=0` | `The field MaxMessageSizeBytes must be between 1 and 2147483647.` |
| InMemory | `SerializationFormat=Xml` | `Failed to convert configuration value 'Xml' at 'MessageQueue:SerializationFormat'` |
| InMemory | `SerializationFormat=1` | `SerializationFormat '1' is not supported. Supported values: Json, MessagePack` |
| InMemory | `Retry:MaxRetries=-1` | `Retry:MaxRetries must be between 0 and 1000` |
| InMemory | `Retry:InitialDelay=-00:00:01` | `Retry:InitialDelay must be >= 0` |
| InMemory | `Retry:MaxDelay=00:00:01` | `Retry:MaxDelay must be >= Retry:InitialDelay` |
| InMemory | `Retry:MaxDelay=2.00:00:00` | `Retry:MaxDelay must be <= 1.00:00:00` |
| InMemory | `Retry:BackoffMultiplier=0.5` | `Retry:BackoffMultiplier must be between 1 and 100` |
| InMemory | `Retry:JitterFactor=1.5` | `Retry:JitterFactor must be between 0 and 1` |
| InMemory | `Retry:Strategy=Custom` | `Retry:Strategy Custom requires a CustomDelayCalculator` |
| InMemory | `Retry:Strategy=2` | `Retry:Strategy '2' is not supported. Supported values: Fixed, Linear, Exponential, Custom` |
| InMemory | `ShutdownTimeout=00:00:00` | `ShutdownTimeout must be > 0` |
| InMemory | `RetryAttempts=3` | `'ErrorOnUnknownConfiguration' was set on the provided BinderOptions, but the following properties were not found on the instance of ESE.MessageQueue.InMemory.InMemoryConfig: 'RetryAttempts'` |
| InMemory | `Retry:MaxRetry=3` | `the following properties were not found on the instance of ESE.MessageQueue.Retry.RetryPolicy: 'MaxRetry'` |
| RabbitMq | `ConnectionString=` | `ConnectionString (amqps://user:password@host/vhost) or HostName is required` |
| RabbitMq | `HostName=rabbit.example.com` (next to a connection string) | `Set either ConnectionString or HostName, not both` |
| RabbitMq | `ConnectionString=http://rabbit.example.com` | `ConnectionString must be an absolute amqp:// or amqps:// URI` |
| RabbitMq | `Port=5671` (next to a connection string) | `Port, UserName, Password and VirtualHost apply only with HostName; put them in the ConnectionString instead` |
| RabbitMq | `Tls:Enabled=true` (next to a connection string) | `Tls:Enabled applies only with HostName; use an amqps:// ConnectionString instead` |
| RabbitMq | `ConnectionString=amqp://rabbit.example.com` | `RequireTls is on (the default for any host other than localhost, 127.0.0.0/8 or ::1), but the connection is not encrypted: use an amqps:// ConnectionString (or Tls:Enabled with HostName), or set RequireTls = false explicitly` |
| RabbitMq | `ConnectionString=` `HostName=rabbit.example.com` | `RequireTls is on (the default for any host other than localhost, 127.0.0.0/8 or ::1), but the connection is not encrypted` |
| RabbitMq | `Tls:AcceptablePolicyErrors=RemoteCertificateNameMismatch` | `Tls:AcceptablePolicyErrors accepts invalid server certificates; set Tls:AllowInsecureCertificates = true to confirm that this is intended` |
| RabbitMq | `Tls:AcceptablePolicyErrors=2` | `Tls:AcceptablePolicyErrors '2' is not supported. Supported values: None, RemoteCertificateNotAvailable, RemoteCertificateNameMismatch, RemoteCertificateChainErrors` |
| RabbitMq | `Tls:ClientCertificateThumbprint=abc` | `Tls:ClientCertificateThumbprint must be a SHA-1 thumbprint: 40 hexadecimal characters (whitespace, ':', '-' and direction marks are ignored)` |
| RabbitMq | `Tls:ClientCertificatePath=/nonexistent/client.pfx` | `Tls:ClientCertificatePath '/nonexistent/client.pfx' does not exist or cannot be read` |
| RabbitMq | `Tls:ClientCertificatePassword=secret` | `Tls:ClientCertificatePassword applies only with Tls:ClientCertificatePath` |
| RabbitMq | `Tls:ClientCertificateKeyPath=/nonexistent/client.key` | `Tls:ClientCertificateKeyPath applies only with a PEM Tls:ClientCertificatePath` |
| RabbitMq | `Tls:ClientCertificatePath=/nonexistent/client.pfx` `Tls:ClientCertificateThumbprint=0123456789abcdef0123456789abcdef01234567` | `Set either Tls:ClientCertificatePath or Tls:ClientCertificateThumbprint, not both` |
| RabbitMq | `ConnectionString=amqp://localhost` `Tls:ClientCertificateThumbprint=0123456789abcdef0123456789abcdef01234567` | `A client certificate (Tls:ClientCertificatePath or Tls:ClientCertificateThumbprint) needs TLS: use an amqps:// ConnectionString (or Tls:Enabled with HostName)` |
| RabbitMq | `Tls:ClientCertificateStoreName=5` | `Tls:ClientCertificateStoreName '5' is not supported. Supported values: AddressBook, AuthRoot, CertificateAuthority, Disallowed, My, Root, TrustedPeople, TrustedPublisher` |
| RabbitMq | `PrefetchCount=0` | `The field PrefetchCount must be between 1 and 65535.` |
| RabbitMq | `ExchangeType=` | `The ExchangeType field is required.` |
| RabbitMq | `PublisherChannelPoolSize=65` | `The field PublisherChannelPoolSize must be between 1 and 64.` |
| RabbitMq | `MaxOutstandingConfirms=0` | `The field MaxOutstandingConfirms must be between 1 and 2147483647.` |
| RabbitMq | `QuorumInitialGroupSize=0` | `The field QuorumInitialGroupSize must be between 1 and 2147483647.` |
| RabbitMq | `ConnectionString=` `HostName=rabbit.example.com` `Tls:Enabled=true` `Port=70000` | `The field Port must be between 0 and 65535.` |
| RabbitMq | `ConfirmTimeout=00:00:00` | `ConfirmTimeout must be > 0` |
| RabbitMq | `HeartbeatInterval=-00:00:01` | `HeartbeatInterval must be >= 0` |
| RabbitMq | `NetworkRecoveryInterval=00:00:00` | `NetworkRecoveryInterval must be > 0` |
| RabbitMq | `ShutdownTimeout=00:00:00` | `ShutdownTimeout must be > 0` |
| AzureServiceBus | `FullyQualifiedNamespace=` | `Set exactly one of ConnectionString (shared access key) or FullyQualifiedNamespace (Entra ID identity, recommended)` |
| AzureServiceBus | `ConnectionString=Endpoint=sb://contoso.servicebus.windows.net/;SharedAccessKeyName=app;SharedAccessKey=abc` (next to a namespace) | `Set exactly one of ConnectionString (shared access key) or FullyQualifiedNamespace (Entra ID identity, recommended)` |
| AzureServiceBus | `FullyQualifiedNamespace=https://contoso.servicebus.windows.net` | `FullyQualifiedNamespace must be a host name such as contoso.servicebus.windows.net (no scheme or path)` |
| AzureServiceBus | `FullyQualifiedNamespace=` `ConnectionString=not-a-connection-string` | `ConnectionString is not a valid Service Bus connection string: it needs Endpoint=sb://<namespace>/ and SharedAccessKeyName with SharedAccessKey, or SharedAccessSignature` |
| AzureServiceBus | `TopicName=orders` | `SubscriptionName is required when TopicName is set` |
| AzureServiceBus | `SubscriptionName=billing` | `SubscriptionName requires TopicName: a subscription belongs to a topic` |
| AzureServiceBus | `TopicName=orders` `SubscriptionName=billing` `RequireDelayedRedelivery=true` | `RequireDelayedRedelivery is true, but this provider or entity cannot delay a retry (Capabilities.DelayedRedelivery = false) and the retry policy has a backoff (00:00:02 to 00:00:30)` |
| AzureServiceBus | `SendTimeout=00:00:00` | `SendTimeout must be > 0` |
| AzureServiceBus | `ShutdownTimeout=00:00:00` | `ShutdownTimeout must be > 0` |
| AzureServiceBus | `MessageTimeToLive=00:00:00` | `MessageTimeToLive must be > 0` |
| AzureServiceBus | `MaxAutoLockRenewalDuration=-00:00:01` | `MaxAutoLockRenewalDuration must be >= 0` |
| AzureServiceBus | `PrefetchCount=-1` | `The field PrefetchCount must be between 0 and 2147483647.` |
| AzureServiceBus | `MaxBatchSize=0` | `The field MaxBatchSize must be between 1 and 2147483647.` |
| AzureServiceBus | `EnablePartitioning=true` | `the following properties were not found on the instance of ESE.MessageQueue.AzureServiceBus.AzureServiceBusConfig: 'EnablePartitioning'` |

A queue whose JSON metadata lacks the message type fails with
`JsonTypeInfoResolver has no JSON metadata for <type>: add [JsonSerializable(typeof(<type>))] to its JsonSerializerContext`.
