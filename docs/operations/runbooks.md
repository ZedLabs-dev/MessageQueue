# Runbooks

Standard procedures for operating applications built on ESE.MessageQueue. Each starts from what you see (an
[alert](alerts.md), a [log event](monitoring.md#log-events), a health check) and ends with a verified fix. Background:
[failure modes](failure-modes.md), [configuration reference](../configuration.md).

Commands assume a queue named `order-placed` in RabbitMQ virtual host `orders`, or a Service Bus namespace `contoso`
in resource group `my-rg`. All library log categories start with `ESE.MessageQueue`.

## First look

1. **Health:** the queue-bound checks (`AddRabbitMqHealthCheck<T>()`, `AddAzureServiceBusHealthCheck<T>()`) say whether
   the connection or processor the application uses is up. Their results never contain the reason; the reason is in
   log event 6001 on the same instance.
2. **Logs by event id**, newest first: 2003 or 3009 (cannot connect), 2001 or 3001 (cannot settle), 1001 (handler
   exceptions), 1003 (dead letters), 3008 (lock lost). Startup warnings (1101, 2010, 2013, 3012, 3015, 3016) point at
   configuration.
3. **Metrics:** retry and dead-letter rates by destination, `error.type` on `messaging.client.consumed.messages`
   (which exception, which reason), processing p95.
4. **Broker:** queue depths of the main, `.retry.*` and `.deadletter` queues (RabbitMQ) or active, scheduled and
   dead-letter counts (Service Bus).

## Connection failures

**Signals:** `MessageQueuePublishFailures` or `MessageQueueTransportErrors`; log event 2003 (RabbitMQ) or 3009
(Service Bus); health Unhealthy; on RabbitMQ the host fails to start.

### RabbitMQ

Event 2003 carries the client's exception; its message names the cause:

| In the exception | Cause | Fix |
|---|---|---|
| `BrokerUnreachableException` ... `Connection refused` / timeout | broker down, wrong host or port, firewall | `rabbitmq-diagnostics -q ping` and `rabbitmq-diagnostics listeners` on the broker; from the client host, test the port (5671 for TLS) |
| `ACCESS_REFUSED - Login was refused` | wrong user or password, or `guest` from a remote host | check the secret; `rabbitmqctl authenticate_user orders-app '<password>'` |
| `ACCESS_REFUSED - ... access to ... refused for user` | missing vhost permission | `rabbitmqctl list_permissions -p orders`; grant the patterns in the [RabbitMQ guide](../getting-started/rabbitmq.md#2-a-broker) (write on `amq.default` included) |
| `NOT_ALLOWED - access to vhost 'orders' refused for user '...'` | wrong `VirtualHost`, or no permissions in it | `rabbitmqctl list_vhosts`, `rabbitmqctl list_user_permissions orders-app` |
| `PRECONDITION_FAILED - inequivalent arg '...' for queue '...'` | a queue exists with other arguments than the configuration asks for | see [queue argument changes](#queue-argument-changes); do not delete queues that hold messages |
| `AuthenticationException` / TLS handshake errors | certificate not trusted, name mismatch, expired, or client certificate refused | see [TLS and certificates](#tls-and-certificates) |

While the connection is down, deliveries in flight are requeued by the broker and redelivered after recovery, and
publishes throw `MessageQueueException`. The client reconnects every `NetworkRecoveryInterval` (10 s) by itself; a
process that failed at startup must be restarted (your orchestrator does this). After the fix, confirm:
`rabbitmqctl list_connections -p orders name user state` shows connections named `ESE.MessageQueue:order-placed` in
state `running`, and `rabbitmqctl list_consumers -p orders` shows the consumer on `order-placed`.

### Azure Service Bus

Event 3009 names the error source and exception type; `ese.messagequeue.transport.errors` carries the reason:

| `error.type` / exception | Cause | Fix |
|---|---|---|
| `UnauthorizedAccessException` | the identity lacks a data role, or the SAS key is wrong or rotated | `az role assignment list --assignee "$APP_PRINCIPAL_ID" --all -o table`: it needs **Azure Service Bus Data Receiver** (consume, peek) and **Data Sender** (publish, queue retries, replay) on the entity or namespace. New assignments take a few minutes to apply |
| `MessagingEntityNotFound` | queue, topic or subscription missing, or a typo in `QueueName`/`TopicName`/`SubscriptionName` | `az servicebus queue show -g my-rg --namespace-name contoso -n order-placed`; the library never creates entities |
| `ServiceCommunicationProblem`, `ServiceTimeout`, socket errors | network path to `*.servicebus.windows.net` (AMQP on 5671) blocked, DNS, private endpoint | test name resolution and outbound 5671 from the host |
| `ServiceBusy` / `QuotaExceeded` | throttling or a full entity | check `ThrottledRequests`, the entity's size and the namespace tier |
| `CredentialUnavailableException` (from `DefaultAzureCredential`) | no identity available on this host | assign a managed identity or set `AZURE_CLIENT_ID`; register a specific `TokenCredential` to pin it |

The SDK keeps retrying, so consumption resumes on its own once the cause is fixed; the processor is never stopped by
these errors. Event 3003 (`could not be created`) instead means the configuration itself is unusable: fix the
`ConnectionString` or `FullyQualifiedNamespace` and restart.

## High retry rate

**Signals:** `MessageQueueRetryRateHigh`; many event 1001 lines; the retried counter climbs.

1. Find the failure: `error.type` on `messaging.client.consumed.messages` gives the exception type (or `Retry` when
   the handler returned it); event 1001 carries the exception with its stack trace.
2. Decide whether it is transient:
   - **A dependency is down or slow** (database, HTTP API). The retries are doing their job. Fix or wait for the
     dependency; messages that exhaust their retries meanwhile are dead-lettered as `MaxRetriesExceeded` and can be
     replayed afterwards. If the outage outlasts the retry budget (about a minute by default), consider a temporarily
     larger budget (below) rather than letting everything dead-letter.
   - **A bug or a bad message shape** after a release. Roll back the consumer. Messages dead-lettered meanwhile are
     replayed after the fix.
   - **Permanent errors being retried** (validation, not-found). Make the handler return `MessageResult.DeadLetter`,
     or set `Retry:IsRetryable` in code, so they stop costing a minute of retries each.
3. On RabbitMQ, retries also fill the `order-placed.retry.<ms>` queues; they drain by themselves as the TTL expires.

A larger retry budget for a known outage (redeploy, see [rolling out configuration changes](#rolling-out-configuration-changes)):

```json
{
  "MessageQueue": {
    "Provider": "RabbitMq",
    "HostName": "rabbit.internal.example.com",
    "Tls": { "Enabled": true },
    "VirtualHost": "orders",
    "UserName": "orders-app",
    "QueueName": "order-placed",
    "Retry": { "MaxRetries": 10, "InitialDelay": "00:00:05", "MaxDelay": "00:05:00" }
  }
}
```

On RabbitMQ the new schedule uses new retry queues (one per delay bucket); see [retry policy changes](#retry-policy-changes).

## Dead-letter backlog

**Signals:** `RabbitMqDeadLetterBacklog`, the Service Bus `DeadletteredMessages` alert, `MessageQueueDeadLetterRateHigh`
or `MessageQueueUnprocessableMessages`; event 1003.

The tool is `IDeadLetterQueue<T>` (registered next to `IMessageQueue<T>`): run it from an admin endpoint, an operator
command or a one-off job built from the same application, so it uses the same message type and configuration. The
[guides](../getting-started/in-memory.md#dead-letters) show the code; [dead-letter operations](../dead-letter-operations.md)
has the API details.

1. **Inspect.** `PeekAsync(50)` and group by `ReasonCode` and `ReasonDetail` (an exception type):

   | Reason | Meaning | Action |
   |---|---|---|
   | `MaxRetriesExceeded` | the handler kept failing | fix the cause (dependency, bug), then replay |
   | `NonRetryableError` | `IsRetryable` rejected the exception | replay only if the rule or the data changed |
   | `Rejected` | the handler returned `DeadLetter` | replay only if the handler's decision changed |
   | `DeserializationFailed` | the body does not match the message type (a publisher on another contract version) | fix the type or the publisher, then replay naming the reason; or purge |
   | `MessageTooLarge` | body above `MaxMessageSizeBytes` | raise the limit and replay naming the reason; or purge |
   | `DeliveryFailed` | unreadable delivery metadata | inspect on the broker; usually purge |
   | `delivery_limit` (RabbitMQ), `MaxDeliveryCountExceeded`, `TTLExpiredException` (Service Bus) | the broker dead-lettered it (crash loops, lock losses, expiry) | find why the deliveries never settled (crashes, lock loss), then replay |

2. **Fix the cause** and deploy the fix. Replaying first only dead-letters the messages again, after another full
   retry budget each.
3. **Count with a dry run.** `ReplayAsync(new() { MaxCount = 500, ReasonCodes = [...], Filter = ..., DryRun = true })`
   returns `Matched` without changing anything. Narrow with `ReasonCodes` and a `Filter` on headers, ids or the
   message (for example one tenant, or messages dead-lettered after the incident began via `DeadLetteredAt`).
   `MessageTooLarge` and `DeserializationFailed` are left out unless named.
4. **Replay** with the same options and `DryRun = false`. Each message goes back as a first delivery with the full
   retry budget, keeping its message id, correlation id, headers and trace. Check `Replayed` and `Failed` in the result
   and event 1201; failures stay dead letters (event 1203). One call examines at most 10,000 dead letters; repeat until
   `Matched` is 0. Replays are at-least-once: a failure between the send and the removal leaves a duplicate, which
   idempotent handlers absorb.
5. **Purge** what you decided to drop: `PurgeAsync(new() { MaxCount = ..., ReasonCodes = [...], Filter = ... })`.
   Purging is permanent and always bounded by `MaxCount`; peek (and keep what you need for an audit) first. Event 1202
   and `ese.messagequeue.dead_letters.purged` record it.

Provider notes:

- **RabbitMQ:** needs `EnableDeadLetterQueue = true`. While a call runs, the dead letters it read stay unacknowledged
  on its channel. A peek or dry run on a quorum dead-letter queue moves the messages it read to the end of the queue.
  If the queue logs warning 2013 at startup, apply the `delivery-limit` policy below before working the backlog.
- **Service Bus:** replay works on queues only; on a topic subscription `ReplayAsync` throws `NotSupportedException`
  (peek and purge work). Resubmit a subscription's dead letters with a tool that sends to that subscription only
  (for example Service Bus Explorer in the Azure portal, which can resend a dead letter to its entity), never to the
  topic, which would deliver it to every subscription.
- **In-memory:** dead letters are lost when the process stops.

## TLS and certificates

### RabbitMQ server certificate

The client validates the broker's certificate chain against the OS trust store and the host name (or
`Tls:ServerName`). Check what the broker presents and when it expires:

```bash
openssl s_client -connect rabbit.internal.example.com:5671 -servername rabbit.internal.example.com </dev/null 2>/dev/null \
  | openssl x509 -noout -subject -issuer -enddate
```

**Rotation:** install the new certificate on every node (same CA, or a CA the clients already trust) and reload the
listeners (`rabbitmqctl eval 'ssl:clear_pem_cache().'` for file changes, or a rolling node restart). Existing
connections keep their session; new and recovered connections use the new certificate. When the new certificate comes
from a **new CA**, add that CA to the clients' trust store and restart them **before** switching the broker. Never
"fix" an expired or mismatched certificate with `Tls:AcceptablePolicyErrors`.

### RabbitMQ client certificate (mutual TLS)

The client certificate (`Tls:ClientCertificatePath` or `Tls:ClientCertificateThumbprint`) is checked at startup and
loaded **once per queue**, then reused for every reconnection. Replacing the file or the store entry has no effect on a
running process. Expiry check: `openssl pkcs12 -in client.pfx -nokeys | openssl x509 -noout -enddate` (PFX) or
`openssl x509 -in client.pem -noout -enddate` (PEM).

**Rotation:**

1. Issue the new certificate (from a CA the broker trusts) and, if it authenticates the user by certificate
   (`rabbitmq_auth_mechanism_ssl`), make sure its subject maps to the same RabbitMQ user.
2. Deploy it next to the old one (a new file path, or the store) and change `Tls:ClientCertificatePath` (or the
   thumbprint) in the configuration.
3. Restart the instances, one at a time; each validates the new certificate at startup and fails fast if it is
   unreadable or lacks its private key, leaving the old instances running.
4. Remove the old certificate once every instance runs on the new one.

### Azure Service Bus

TLS to Service Bus is managed by Azure; there is no client-side certificate setting. Make sure hosts trust the public
CAs Azure uses (keep OS CA bundles current in container images).

## Credential rotation

Credentials are read once at startup; a running process keeps the one it started with, and **its automatic reconnects
use it too**. Rotate with an overlap so that no instance holds a credential that no longer works.

### RabbitMQ user password

Changing the password of the user the application runs as does not drop existing connections, but every reconnection
afterwards would fail with `ACCESS_REFUSED` until the instance restarts with the new secret. Use a second user:

```bash
rabbitmqctl add_user orders-app-2 '<new password>'
rabbitmqctl set_permissions -p orders orders-app-2 \
  '^order-placed(\..+)?$' '^(order-placed(\..+)?|amq\.default)$' '^order-placed(\..+)?$'
```

Roll the application onto the new user (secret store, then a rolling restart):

```bash
export MessageQueue__Provider=RabbitMq
export MessageQueue__HostName=rabbit.internal.example.com
export MessageQueue__Tls__Enabled=true
export MessageQueue__VirtualHost=orders
export MessageQueue__QueueName=order-placed
export MessageQueue__UserName=orders-app-2
export MessageQueue__Password="$NEW_RABBITMQ_PASSWORD"
```

When `rabbitmqctl list_connections -p orders user` shows only `orders-app-2`, delete the old user
(`rabbitmqctl delete_user orders-app`). With a `ConnectionString`, the user and password are in the URI; rotate the
whole string the same way.

### Azure Service Bus shared access keys

1. Switch the application's `ConnectionString` to the **secondary** key and do a rolling restart.
2. Regenerate the primary key (`az servicebus namespace authorization-rule keys renew --key PrimaryKey ...`, or the
   queue-level rule).
3. Next time, switch back to the primary and regenerate the secondary.

Better: move to Entra ID (`FullyQualifiedNamespace`), which has no secret in the application.

### Entra ID

Managed identities have nothing to rotate; tokens are refreshed by the credential. For a service principal with a
client secret or certificate (for example `AZURE_CLIENT_SECRET` for `DefaultAzureCredential`), add the new secret to
the app registration, roll the instances onto it, then remove the old one. Role changes (granting or removing Data
Sender/Receiver) take a few minutes to apply; a removed role shows as `UnauthorizedAccessException` (event 3009).

## Stuck or slow consumer

**Signals:** `MessageQueueConsumerStalled` (messages ready, none delivered), `MessageQueueProcessingSlow`, a growing
backlog, health Unhealthy (`processor is not running` on Service Bus).

1. **Is it consuming at all?**
   - RabbitMQ: `rabbitmqctl list_queues -p orders name messages_ready messages_unacknowledged consumers`. `consumers`
     0 means no subscriber: the instances are down, failed to start (event 2003), or the broker cancelled the consumer
     (for example the queue was deleted; health is Unhealthy). Restart the instance: on connect the library declares
     the queue again.
   - Service Bus: the `ActiveMessages` metric grows while nothing completes; the health check says whether the processor
     runs.
   - Logs: event 5003 means no processor is registered for the type; 5001 confirms the hosted service started.
2. **Are handlers blocked?** On RabbitMQ, `messages_unacknowledged` stuck at `PrefetchCount × instances` with no acks
   means every handler slot is busy. Take a stack dump of one instance (`dotnet-stack report -p <pid>`, or `dotnet-dump`)
   and look for handlers waiting on a dependency. The library has no handler timeout: add one in your handler. On
   Service Bus, warnings 3008 (lock lost) mean handlers outlive the lock.
3. **Too little concurrency?** Throughput is bounded by `MaxConcurrentCalls` per instance (and by `PrefetchCount` on
   RabbitMQ, warning 2010). Raise them, or add instances, if the handlers are I/O-bound and the dependency can take
   it.
4. **Recover:** restart the stuck instance. Its in-flight messages are requeued (RabbitMQ) or abandoned (Service Bus)
   and redelivered to the others; handlers must be idempotent for this.

## Rolling out configuration changes

**Every setting needs a restart.** The library reads its configuration once, when the queue is created; reloaded
`appsettings.json` or changed secrets do nothing to a running process. Roll changes out like code: a rolling
deployment with readiness gates, one instance first.

Because invalid configuration fails the host at startup, a bad value stops the first new instance and leaves the old
ones running, as long as the deployment waits for readiness. Rolling back is redeploying the previous configuration,
with the exceptions below.

| Change | Effect | Safe to roll out instance by instance? |
|---|---|---|
| `MaxConcurrentCalls`, `PrefetchCount`, timeouts, `HeartbeatInterval`, `NetworkRecoveryInterval`, publisher settings, `MaxAutoLockRenewalDuration`, `SendTimeout`, `TransactionalRetries` | per instance | yes |
| `Retry:MaxRetries`, delays, `Strategy` | applies to deliveries handled after the restart; messages already further along than a lower `MaxRetries` are dead-lettered on their next failure | yes; on RabbitMQ see [retry policy changes](#retry-policy-changes) |
| `MaxMessageSizeBytes` lowered | messages already queued above the new limit are dead-lettered as `MessageTooLarge` | yes, if you accept that |
| `RequireDelayedRedelivery`, `EnableDeadLetterQueue` (Service Bus, in-memory) | startup check / drop-vs-dead-letter behavior | yes |
| `SerializationFormat` | consumers on the new format dead-letter old-format messages as `DeserializationFailed` (and vice versa) | **no**: drain the queue, or switch consumers and publishers together; replay what was dead-lettered meanwhile |
| Credentials, certificates | see the sections above | yes, with an overlap |
| `QueueName`, `TopicName`/`SubscriptionName`, `ExchangeName` | a different entity: messages in the old one are no longer consumed | **no**: run consumers on both until the old one is empty |
| RabbitMQ `UseQuorumQueues`, `QuorumInitialGroupSize`, `EnableDeadLetterQueue`, `DeadLetterQueueName`, `ExchangeType` | changes the arguments of an existing queue or exchange | **no**: see [queue argument changes](#queue-argument-changes) |

### Queue argument changes

RabbitMQ never changes the arguments of an existing queue or exchange. A new instance that declares
`order-placed` with other arguments is refused with `PRECONDITION_FAILED - inequivalent arg ...` (event 2003) and does
not start; the old instances keep running. These settings set arguments:

- `UseQuorumQueues` (`x-queue-type`) and `QuorumInitialGroupSize` (`x-quorum-initial-group-size`) on the main,
  dead-letter and retry queues;
- `EnableDeadLetterQueue` and `DeadLetterQueueName` (`x-dead-letter-exchange`, `x-dead-letter-routing-key`,
  `x-overflow`) on the main queue;
- `ExchangeType` on the exchange.

Options, safest first:

1. **A new queue name.** Deploy consumers for `order-placed-v2` (new arguments) next to the old ones, switch
   publishers to it, and retire the old consumers once `order-placed` and its retry queues are empty. Rollback is
   switching publishers back.
2. **Policies instead of arguments** where RabbitMQ has one (delivery limit, overflow, TTL, max length): a policy
   changes a live queue without redeclaring it. The dead-letter queue policy below is an example.
3. **Recreate during a maintenance window:** stop publishers, let consumers drain `order-placed` and its
   `.retry.*` queues, stop consumers, move what remains (for example with a shovel), delete the queues, deploy the
   new configuration. A rollback then fails the same way in reverse, so plan it as a second migration.

Never delete a queue that still holds messages to "fix" a `PRECONDITION_FAILED`.

### Retry policy changes

On RabbitMQ a retry waits in `{QueueName}.retry.{ms}`, one queue per delay bucket of the policy's schedule (at most
three per step with jitter). A new schedule declares new retry queues on first use; the old ones keep their messages,
which still flow back to the main queue when their TTL expires, so nothing is lost and the change is safe to roll out.
Afterwards the old buckets are empty and unused:

- classic retry queues (`UseQuorumQueues = false`) delete themselves after `max(1 h, 4 × TTL)` unused;
- quorum retry queues never expire. Delete an old bucket once **every** instance runs the new policy (so nothing
  publishes into it any more) and it shows 0 messages:

```bash
rabbitmqctl list_queues -p orders name messages | grep '^order-placed\.retry\.'
rabbitmqctl delete_queue -p orders order-placed.retry.2000
```

`delete_queue --if-empty` is not available for quorum queues (RabbitMQ 4.3.6 answers `not_implemented`), so check the
count first; deleting a bucket that still holds messages loses them. A bucket the current policy still uses is simply
declared again by the next retry that needs it.

## RabbitMQ

Useful checks (`rabbitmqctl` on a broker node; the management UI shows the same):

```bash
rabbitmqctl list_queues -p orders name type messages_ready messages_unacknowledged consumers policy arguments
rabbitmqctl list_connections -p orders name user state client_properties
rabbitmqctl list_consumers -p orders
rabbitmqctl list_policies -p orders
rabbitmq-queues quorum_status order-placed --vhost orders
rabbitmq-diagnostics check_if_node_is_quorum_critical
```

- **Connections** are named `ESE.MessageQueue:<QueueName>`: one connection per queue per instance.
- **Quorum queues** need a majority of their replicas online; a node maintenance window that loses quorum stops the
  queue. Check `check_if_node_is_quorum_critical` before taking a node down.
- **Delivery limit.** RabbitMQ 4 quorum queues dead-letter (or drop, without a dead-letter exchange) a message
  returned unsettled more than 20 times. The library declares main queues with its dead-letter queue as their
  dead-letter target, and new dead-letter queues with `x-delivery-limit = -1` (unlimited). A dead-letter queue created
  by an earlier version keeps the limit; the queue logs **warning 2013** at startup. Lift it with a policy, which
  applies to live queues:

  ```bash
  rabbitmqctl set_policy -p orders dlq-unlimited '\.deadletter$' '{"delivery-limit": -1}' \
    --apply-to quorum_queues --priority 10
  ```

  A queue follows only one policy, the highest-priority match: if another policy already matches the dead-letter
  queues, add `"delivery-limit": -1` to that policy's definition instead.
- **Inspecting a dead letter by hand:** `rabbitmqadmin` or the management UI "Get messages" with requeue. On a quorum
  queue that counts as a delivery and moves the message to the back; prefer `IDeadLetterQueue<T>.PeekAsync`. The
  library's headers say why: `x-ese-dead-letter-reason`, `x-ese-dead-letter-detail`, `x-ese-dead-lettered-at`,
  `x-delivery-attempt`.
- **Resource alarms** (memory or disk) block publishers: `PublishAsync` waits and then fails after `ConfirmTimeout`.
  `rabbitmq-diagnostics alarms` lists them.
- **Consumer timeout:** a delivery left unacknowledged longer than the broker's `consumer_timeout` (30 minutes by
  default) closes the channel; the message is redelivered. Fix the slow handler rather than raising the timeout.

## Azure Service Bus

Useful checks:

```bash
az servicebus queue show -g my-rg --namespace-name contoso -n order-placed \
  --query "{active:countDetails.activeMessageCount, deadLetter:countDetails.deadLetterMessageCount, scheduled:countDetails.scheduledMessageCount, maxDeliveryCount:maxDeliveryCount, lockDuration:lockDuration, partitioned:enablePartitioning, duplicateDetection:requiresDuplicateDetection}"
az servicebus topic subscription show -g my-rg --namespace-name contoso --topic-name order-events -n billing \
  --query "{active:countDetails.activeMessageCount, deadLetter:countDetails.deadLetterMessageCount, maxDeliveryCount:maxDeliveryCount}"
az role assignment list --assignee "$APP_PRINCIPAL_ID" --all -o table
```

- **Service Bus Explorer** (the entity's page in the Azure portal) peeks active and dead-lettered messages, with the
  library's properties (`x-ese-dead-letter-reason`, `x-ese-dead-letter-detail`, `x-ese-dead-lettered-at`,
  `x-ese-dead-letter-attempt`) and the broker's `DeadLetterReason`.
- **`MaxDeliveryCount`.** On a queue, library retries are scheduled copies, so `MaxDeliveryCount` only counts
  deliveries that ended without settlement (crashes, lock loss, shutdown hand-backs, settlement failures); the default
  10 is fine. On a **topic subscription** every retry is a redelivery of the same message: keep `MaxDeliveryCount`
  above `Retry:MaxRetries + 1` (warning 3012), or the broker dead-letters first with `MaxDeliveryCountExceeded`.
- **Partitioned entities, the Basic tier and the emulator** refuse the retry transaction. The queue logs warning 3014
  once and switches to send-then-complete (a crash in between can duplicate a retry, never lose it). Set
  `TransactionalRetries = false` for those entities to skip the refused attempt.
- **Lock duration and renewal.** The entity's lock duration (for example 1 minute) is renewed by the processor for up
  to `MaxAutoLockRenewalDuration`; warnings 3008/3010 mean handlers outlive it. On a subscription, a retry delay
  longer than the remaining renewal budget is redelivered at once (warning 3011).
- **Duplicate detection** on the entity drops resends with the same `MessageId` within its window: it absorbs
  publisher retries. Library retry and replay copies use new broker ids, so it never drops them.
- **Throttling** (`ServiceBusy`, `ThrottledRequests`): lower `MaxConcurrentCalls` and `PrefetchCount`, or move to a
  higher tier or more messaging units.
