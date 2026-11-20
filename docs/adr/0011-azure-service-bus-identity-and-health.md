# 0011. Azure Service Bus identity, least-privilege health checks and secrets

- Status: Accepted (2026-10-17)
- Context: OPTIMIZATION-PLAN S3, S4, S5, S10; ADR 0002 (options validation)

## Context

Only SAS connection strings were supported. The health check used `ServiceBusAdministrationClient` (Manage rights)
with a new client per probe and put `ex.Message` in its data, as did the generic `MessageQueueHealthCheck`.
Initialization errors echoed the client's exception text.

## Decision

- **Identity is the production default.** `FullyQualifiedNamespace` (a host name) with a `TokenCredential`: one
  passed to the constructor, else one registered in the container, else `DefaultAzureCredential` (Azure.Identity).
  `ConnectionString` remains for local development and the emulator. Validation requires exactly one of the two.
  A credential passed to the constructor with a connection string throws `ConfigurationException`; a container
  credential is then ignored. One `DefaultAzureCredential` is shared per process (its token cache).
  Required RBAC: **Azure Service Bus Data Sender** to publish, **Data Receiver** to consume and for the health peek.
  Retries need both on the queue. Never Owner or Manage.
- **Connection strings** are checked at startup with the SDK's parser (endpoint plus key name and key, or a SAS), so a
  malformed one fails with a fixed message. If the client cannot be created, the `MessageQueueException` names
  only the exception type and carries no inner exception, and the log records only the type (S10).
- **Health** (S4/S5): `AddAzureServiceBusHealthCheck<T>()` reuses the registered queue's client: Unhealthy while its
  processor is not running, else a peek (Listen rights) through one cached receiver on the consumed queue or
  subscription. The connection-string overload keeps one client and receiver per registration, disposed with the
  container; both standalone checks also take a namespace + `TokenCredential`. The admin-metrics check is opt-in
  (`AddAzureServiceBusMetricsHealthCheck`), states it needs Manage, and
  reuses one admin client. Results carry fixed descriptions, no exception text, host or namespace; the exception
  object is attached for the host's logging. `MessageQueueHealthCheck` no longer puts `ex.Message` in `Data`.

## Consequences

- Breaking: `ConnectionString` is no longer `[Required]` alone; malformed strings fail at startup.
- Azure.Identity (and MSAL) become dependencies of the core package until the provider split (ADR 0005).
- Client-creation failures lose the inner exception; the type and the settings to check are in the message.

## Later changes

- Superseded in part (Phase 7): health results carry **no exception** at all; the exception is logged (event 6001)
  instead of attached, because health endpoints may serialize it. Standalone checks validate the connection string
  or namespace at registration. Azure.Identity is a dependency of `ZedLabs.MessageQueue.AzureServiceBus` only (ADR 0015).
