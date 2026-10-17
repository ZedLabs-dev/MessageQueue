# ESE.MessageQueue.Samples

A worker that publishes three orders and processes them with `OrderCreatedProcessor`: one is acknowledged, one asks
for a retry and succeeds on attempt 2, one is dead-lettered. Spans and metrics go to the console through OpenTelemetry
(`Telemetry:ConsoleExporter`); the library itself only uses `System.Diagnostics`.

```bash
dotnet run --project samples/ESE.MessageQueue.Samples                                   # in-memory, no broker
dotnet run --project samples/ESE.MessageQueue.Samples --environment RabbitMq            # docker compose broker
dotnet run --project samples/ESE.MessageQueue.Samples --environment AzureServiceBus     # Entra ID (DefaultAzureCredential)
```

For RabbitMQ, start `MessageQueue/docker-compose.dev.yml` and pass the user and password you chose in
`MessageQueue/.env` as environment variables (`MessageQueue__UserName`, `MessageQueue__Password`); credentials are
never committed, and the sample stops at startup with that instruction when they are missing. For Azure
Service Bus, set `FullyQualifiedNamespace` and sign in (`az login`); your identity needs the Data Sender and Data
Receiver roles. Press Ctrl+C to stop.
