# ESE.MessageQueue documentation

## Start here

- [Getting started: in-memory](getting-started/in-memory.md): the pipeline with no broker (development, tests)
- [Getting started: RabbitMQ](getting-started/rabbitmq.md): broker setup, least-privilege permissions, TLS
- [Getting started: Azure Service Bus](getting-started/azure-service-bus.md): entities, Entra ID roles, topics

## Reference

- [Configuration reference](configuration.md): every setting, default, range and startup error
- [Message queue API](message-queue-api.md): interfaces, `PublishOptions`, `MessageContext`, provider classes
- [Azure Service Bus provider](azure-service-bus-api.md): retries, lifecycle and health in detail
- [Dead-letter operations](dead-letter-operations.md): peek, replay and purge with `IDeadLetterQueue<T>`
- [Benchmarks](benchmarks.md)
- [Internals](message-queue-internal.md): how registration, publishing and consuming fit together
- [Architecture decision records](adr/): why the library works the way it does

## Operations

- [Failure modes](operations/failure-modes.md): guarantees, duplicates, poison messages, outages, shutdown
- [Monitoring](operations/monitoring.md): metrics, spans, every log event, health checks
- [Alerts](operations/alerts.md): Prometheus and Azure Monitor rules with thresholds
- [Runbooks](operations/runbooks.md): connection failures, retries, dead-letter backlog, certificates, credentials,
  configuration rollouts, provider troubleshooting

## Project

- [Release checklist](release-checklist.md): the regression suite every release runs
- [Production readiness status](production-readiness-status.md): the readiness plan, item by item
- [Improvement suggestions](message-queue-improvements.md): an early, historical list of ideas; not a roadmap and
  partly superseded (see the readiness status and `OPTIMIZATION-PLAN.md`)

The guides, the configuration reference and the monitoring reference are checked against the code by specs in
`tests/ESE.MessageQueue.Tests/Packaging/` (`Given_the_documentation_snippets`, `Given_the_configuration_reference`,
`Given_the_monitoring_reference`): their C# is compiled from `samples/ESE.MessageQueue.Samples/Docs/`, their
configuration examples are bound and validated, and their tables must match the settings, defaults, log events and
metrics the library actually has.
