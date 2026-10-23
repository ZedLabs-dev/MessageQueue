# Recommended alerts

Starting points for alert rules on the library's [metrics](monitoring.md#metrics) and the brokers' own metrics. The
thresholds are defaults to tune against your traffic and SLOs, not measured limits. Each alert links to the
[runbook](runbooks.md) that handles it.

## Naming assumption

The queries assume metrics reach Prometheus through the OpenTelemetry Prometheus exporter or the OpenTelemetry
Collector's `prometheus` / `prometheusremotewrite` exporter with the default name translation:

- dots become underscores (`ese.messagequeue.messages.retried` → `ese_messagequeue_messages_retried`), in metric and
  label names alike (`messaging.destination.name` → `messaging_destination_name`);
- counters get `_total`;
- the unit is appended when it is a real unit (`s` → `_seconds`); annotation units in braces (`{message}`, `{error}`)
  are dropped;
- histograms become `_bucket`, `_sum` and `_count` series.

| Library metric | Prometheus series |
|---|---|
| `messaging.client.sent.messages` | `messaging_client_sent_messages_total` |
| `messaging.client.operation.duration` | `messaging_client_operation_duration_seconds_bucket` / `_sum` / `_count` |
| `messaging.client.consumed.messages` | `messaging_client_consumed_messages_total` |
| `messaging.process.duration` | `messaging_process_duration_seconds_bucket` / `_sum` / `_count` |
| `ese.messagequeue.messages.retried` | `ese_messagequeue_messages_retried_total` |
| `ese.messagequeue.messages.dead_lettered` | `ese_messagequeue_messages_dead_lettered_total` |
| `ese.messagequeue.transport.errors` | `ese_messagequeue_transport_errors_total` |
| `ese.messagequeue.dead_letters.replayed` | `ese_messagequeue_dead_letters_replayed_total` |
| `ese.messagequeue.dead_letters.purged` | `ese_messagequeue_dead_letters_purged_total` |

If your pipeline keeps the original dotted names (Prometheus 3 with UTF-8 names and no translation), quote them
instead: `{"ese.messagequeue.messages.retried"}`. Label values differ by provider: the destination is the queue on
in-memory and Service Bus (the topic in subscription mode) and `{exchange}:{queue}` on RabbitMQ, and the publish
operation is `publish`, or `send` on Service Bus; the rules below group by those labels rather than hard-coding them.

## Rules

```yaml
groups:
  - name: ese-messagequeue
    rules:
      # Dead letters that will never succeed by themselves: a contract or size problem, not a transient one.
      - alert: MessageQueueUnprocessableMessages
        expr: |
          sum by (messaging_destination_name, ese_messagequeue_dead_letter_reason) (
            increase(ese_messagequeue_messages_dead_lettered_total{
              ese_messagequeue_dead_letter_reason=~"DeserializationFailed|MessageTooLarge|DeliveryFailed"}[15m])
          ) > 0
        labels: { severity: warning }
        annotations:
          summary: "{{ $labels.messaging_destination_name }}: {{ $value }} message(s) dead-lettered as {{ $labels.ese_messagequeue_dead_letter_reason }}"
          runbook: docs/operations/runbooks.md#dead-letter-backlog

      # Dead-letter rate: more than 1% of deliveries end in the dead-letter queue.
      - alert: MessageQueueDeadLetterRateHigh
        expr: |
          sum by (messaging_destination_name) (rate(ese_messagequeue_messages_dead_lettered_total[10m]))
            /
          sum by (messaging_destination_name) (rate(messaging_client_consumed_messages_total[10m]))
            > 0.01
        for: 10m
        labels: { severity: critical }
        annotations:
          summary: "{{ $labels.messaging_destination_name }}: {{ $value | humanizePercentage }} of deliveries dead-lettered"
          runbook: docs/operations/runbooks.md#dead-letter-backlog

      # Retry spike: more than 10% of deliveries are rescheduled (a failing dependency or a bad release).
      - alert: MessageQueueRetryRateHigh
        expr: |
          sum by (messaging_destination_name) (rate(ese_messagequeue_messages_retried_total[5m]))
            /
          sum by (messaging_destination_name) (rate(messaging_client_consumed_messages_total[5m]))
            > 0.10
        for: 10m
        labels: { severity: warning }
        annotations:
          summary: "{{ $labels.messaging_destination_name }}: {{ $value | humanizePercentage }} of deliveries retried"
          runbook: docs/operations/runbooks.md#high-retry-rate

      # Processing latency: p95 from receipt to settlement above 5 s.
      - alert: MessageQueueProcessingSlow
        expr: |
          histogram_quantile(0.95,
            sum by (le, messaging_destination_name) (rate(messaging_process_duration_seconds_bucket[5m]))
          ) > 5
        for: 15m
        labels: { severity: warning }
        annotations:
          summary: "{{ $labels.messaging_destination_name }}: p95 processing time {{ $value | humanizeDuration }}"
          runbook: docs/operations/runbooks.md#stuck-or-slow-consumer

      # Publishing fails.
      - alert: MessageQueuePublishFailures
        expr: |
          sum by (messaging_destination_name, error_type) (
            increase(messaging_client_sent_messages_total{error_type!=""}[5m])
          ) > 0
        for: 5m
        labels: { severity: critical }
        annotations:
          summary: "{{ $labels.messaging_destination_name }}: publishes failing with {{ $labels.error_type }}"
          runbook: docs/operations/runbooks.md#connection-failures

      # Repeated broker trouble: Service Bus processor errors (authentication, entity, network, throttling), RabbitMQ
      # connection losses, failed reconnects, closed consumer channels and cancelled consumers.
      - alert: MessageQueueTransportErrors
        expr: |
          sum by (messaging_destination_name, error_type) (
            increase(ese_messagequeue_transport_errors_total[10m])
          ) > 5
        for: 10m
        labels: { severity: critical }
        annotations:
          summary: "{{ $labels.messaging_destination_name }}: transport errors ({{ $labels.error_type }})"
          runbook: docs/operations/runbooks.md#connection-failures

      # Nothing is consumed while messages wait (RabbitMQ; needs per-queue broker metrics, below).
      - alert: MessageQueueConsumerStalled
        expr: |
          max by (queue) (rabbitmq_detailed_queue_messages_ready{queue!~".*\\.(deadletter|retry\\..*)"}) > 0
            unless on (queue)
          max by (queue) (rate(rabbitmq_detailed_queue_messages_delivered_ack_total[10m])) > 0
        for: 15m
        labels: { severity: critical }
        annotations:
          summary: "RabbitMQ queue {{ $labels.queue }} has ready messages but delivers none"
          runbook: docs/operations/runbooks.md#stuck-or-slow-consumer

      # Dead-letter backlog on RabbitMQ (needs per-queue broker metrics, below).
      - alert: RabbitMqDeadLetterBacklog
        expr: max by (vhost, queue) (rabbitmq_detailed_queue_messages{queue=~".*\\.deadletter"}) > 0
        for: 30m
        labels: { severity: warning }
        annotations:
          summary: "{{ $labels.queue }} holds {{ $value }} dead letter(s)"
          runbook: docs/operations/runbooks.md#dead-letter-backlog

      - alert: RabbitMqDeadLetterBacklogLarge
        expr: max by (vhost, queue) (rabbitmq_detailed_queue_messages{queue=~".*\\.deadletter"}) > 1000
        for: 10m
        labels: { severity: critical }
        annotations:
          summary: "{{ $labels.queue }} holds {{ $value }} dead letters"
          runbook: docs/operations/runbooks.md#dead-letter-backlog

      # Informational: dead letters were deleted (purge is permanent).
      - alert: MessageQueueDeadLettersPurged
        expr: sum by (messaging_destination_name) (increase(ese_messagequeue_dead_letters_purged_total[1h])) > 0
        labels: { severity: info }
        annotations:
          summary: "{{ $value }} dead letter(s) purged from {{ $labels.messaging_destination_name }}"
```

Notes on the rules:

- **Ratios need traffic.** With no deliveries the ratio is empty and the alert stays silent; the stalled-consumer rule
  covers "nothing moves".
- **`MaxRetriesExceeded` and `Rejected`** are counted by the rate rule rather than alerted on individually: a few are
  normal for most systems. `NonRetryableError` usually means a validation rule rejected a message; include it in the
  first rule if your handlers should never see invalid input.
- **The latency histogram tops out at 10 s.** A p95 above 10 s is reported as 10. For handlers that legitimately take
  longer, alert on `rate(messaging_process_duration_seconds_sum[5m]) / rate(messaging_process_duration_seconds_count[5m])`
  instead, or on lock loss (log event 3008) on Service Bus.
- **Retries on RabbitMQ also show broker-side** as messages in `{queue}.retry.{ms}` queues; a growing count there with
  a flat retry rate means retries expire back faster than they are processed.

## Broker-side metrics

### RabbitMQ

The `rabbitmq_prometheus` plugin (enabled in the official `rabbitmq:*-management` images) serves:

- `/metrics`: aggregated over all queues, **no `queue` label** (`rabbitmq_queue_messages_ready` is one number);
- `/metrics/per-object`: per queue, `rabbitmq_queue_messages{vhost, queue}` and friends;
- `/metrics/detailed?family=queue_coarse_metrics`: per queue, `rabbitmq_detailed_queue_messages{vhost, queue}`,
  `rabbitmq_detailed_queue_messages_ready`, `rabbitmq_detailed_queue_messages_unacked`; add
  `&family=queue_delivery_metrics` for `rabbitmq_detailed_queue_messages_delivered_ack_total` (deliveries to consumers
  that acknowledge, as this library's do; `rabbitmq_detailed_queue_messages_delivered_total` counts only auto-ack
  deliveries and stays 0), used by the stalled-consumer rule.

The rules above use the detailed endpoint, which is cheap enough to scrape on busy brokers because it returns only the
families you ask for. Scrape configuration:

```yaml
- job_name: rabbitmq-queues
  metrics_path: /metrics/detailed
  params:
    family: [queue_coarse_metrics, queue_delivery_metrics]
  static_configs:
    - targets: ["rabbit-0.rabbit:15692", "rabbit-1.rabbit:15692", "rabbit-2.rabbit:15692"]
```

(Verified against RabbitMQ 4.3.6 with this library consuming: `rabbitmq_detailed_queue_messages{vhost="/",queue="orders.deadletter"}`
and `rabbitmq_detailed_queue_messages_delivered_ack_total{vhost="/",queue="orders"}` appear on the detailed endpoint,
and `rabbitmq_queue_messages{vhost="/",queue="orders.deadletter"}` on `/metrics/per-object`.)
Also alert on the broker itself (memory and disk alarms, node down, quorum queues without quorum) as the RabbitMQ
monitoring guide recommends; when the broker blocks publishers on a resource alarm, `PublishAsync` waits and then
fails after `ConfirmTimeout`.

### Azure Service Bus

Service Bus metrics live in Azure Monitor. Alert on the dead-letter count per entity:

```bash
namespace_id=$(az servicebus namespace show --resource-group my-rg --name contoso --query id -o tsv)
az monitor metrics alert create --name order-placed-dead-letters --resource-group my-rg --scopes "$namespace_id" \
  --condition "max DeadletteredMessages > 0 where EntityName includes order-placed" \
  --window-size 30m --evaluation-frequency 5m --severity 2 \
  --description "Dead letters in order-placed: see docs/operations/runbooks.md#dead-letter-backlog"
```

Useful companions on the same scope: `ActiveMessages` (backlog; with no consumption, a stalled consumer),
`ServerErrors`, `UserErrors` (often authorization or a missing entity) and `ThrottledRequests`. If you collect Azure
Monitor metrics into Prometheus (for example with an Azure metrics exporter), the series names depend on that
exporter; keep the same conditions. Without Azure Monitor access, the opt-in
`AddAzureServiceBusMetricsHealthCheck` reports `deadLetterMessageCount` in its health result (it needs Manage rights).

## Health and logs

- **Health:** page when the queue-bound readiness check (`AddRabbitMqHealthCheck<T>()`,
  `AddAzureServiceBusHealthCheck<T>()`) has been Unhealthy for 5 minutes on any instance. With Kubernetes and
  kube-state-metrics, for example: `sum by (namespace, pod) (kube_pod_status_ready{condition="false"}) > 0` for 5m,
  scoped to the consumer's deployment.
- **Logs:** where log-based alerting is available, alert on event 2003 (RabbitMQ connect failure) and 3003 (Service Bus
  client creation) at any count, on 2001/3001 (settlement failures) when they repeat for 10 minutes, and review the
  startup warnings 1101, 2010, 2013, 3012, 3015, 3016 and 5005 after each deployment. RabbitMQ connection problems also
  show in `ese.messagequeue.transport.errors` (`MessageQueueTransportErrors`), the publish-failure rule and the health
  check.
