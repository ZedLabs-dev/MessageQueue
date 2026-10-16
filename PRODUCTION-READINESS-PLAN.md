## Production Readiness Plan for ESE.MessageQueue

This document is a working plan for taking ESE.MessageQueue from its current state to production readiness. Edit, add, or remove items as needed.

### 1. Functional & API Surface Review
- [ ] Review public interfaces (`IMessageQueue<T>`, `IMessageProcessor<T>`, configuration types, DI extensions) and lock the public API for v1
- [ ] Document any planned breaking changes and decide whether to apply them now or defer to a later major version
- [ ] Ensure all public types and members are intentional (no accidental exposure of internal helpers)

### 2. Configuration & Environment
- [ ] Define required and optional settings for RabbitMQ and Azure Service Bus providers
- [ ] Validate configuration at startup (fail fast on invalid or incomplete configs)
- [ ] Standardize configuration binding and section names (e.g., `MessageQueue` section)
- [ ] Decide and document behavior for missing/invalid configuration in production vs. development

### 3. Error Handling & Resilience
- [ ] Review all exception flows and ensure they use `MessageQueueException` (or other domain-specific exceptions) consistently
- [ ] Clarify and document retry behavior for each provider (max attempts, backoff, dead-letter behavior)
- [ ] Ensure transient vs. fatal error handling is clearly defined and tested
- [ ] Confirm that dead-letter queues are created and used consistently across providers

### 4. Telemetry, Logging, and Observability
- [ ] Integrate Microsoft.Extensions.Logging for structured logging
- [ ] Add logs for key operations: publish, subscribe, retries, dead-lettering, connection failures
- [ ] Define an `ITelemetryProvider` abstraction to allow pluggable telemetry backends
- [ ] Implement OpenTelemetry provider (traces for publish/consume, metrics for throughput/latency/retries/dead-letters)
- [ ] Ensure trace context propagation across message boundaries (W3C Trace Context in message headers)
- [ ] Document how to register custom telemetry providers

### 5. Performance & Scalability
- [ ] Benchmark publish/subscribe throughput for RabbitMQ and Azure Service Bus under realistic workloads
- [ ] Validate behavior under high concurrency (e.g., `MaxConcurrentCalls`, prefetch, batch sizes)
- [ ] Identify and remove any synchronous blocking on async paths
- [ ] Verify connection pooling and client reuse patterns for both providers

### 6. Security & Compliance
- [ ] Confirm secure handling of connection strings and credentials (no logging of secrets, use of secure configuration providers)
- [ ] Verify TLS/SSL requirements and defaults for both RabbitMQ and Azure Service Bus
- [ ] Document recommended IAM/permission scopes for production usage
- [ ] Review serialization settings for potential data leakage or injection issues

### 7. Testing Strategy
- [ ] Ensure unit tests cover core behaviors for each provider and the processing pipeline
- [ ] Provide Docker Compose for local integration testing (RabbitMQ and Azure Service Bus emulator)
- [ ] Add integration tests for edge cases: large messages, invalid payloads, deserialization failures, max retries, dead-lettering
- [ ] Define a regression test suite to run before releases

### 8. Documentation (Markdown Guide)
- [ ] Write a "Getting Started" guide for each provider (RabbitMQ, Azure Service Bus)
- [ ] Document configuration examples for appsettings.json and environment variables
- [ ] Provide usage samples: simple publisher, simple consumer, message processor pattern
- [ ] Describe failure modes and how to monitor/operate the system in production
- [ ] Document telemetry integration (OpenTelemetry setup, custom providers)

### 9. Operational Runbooks (External Consumers)
- [ ] Define standard operating procedures for common issues (connection failures, high retry rates, dead-letter backlog)
- [ ] Document recommended alert rules (e.g., dead-letter rate, retry spikes, processing latency)
- [ ] Provide guidance for safe configuration changes and rollbacks
- [ ] Include troubleshooting steps for each provider (RabbitMQ, Azure Service Bus)
- [ ] Document how to drain and replay dead-letter queues

### 10. Open Questions / Design Decisions
Use this section to capture decisions you still need to make.

- [ ] Should the library expose provider-specific advanced options directly or via extension points only?
- [ ] Support only .NET 10.x and forward
- [ ] Retry and backoff strategies should be configurable. Start with 5 retries and a total time of 60 seconds.

