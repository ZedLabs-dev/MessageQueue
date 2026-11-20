namespace ZedLabs.MessageQueue.Telemetry;

// Values of the OpenTelemetry `messaging.system` attribute, one per provider ("rabbitmq" and "servicebus" are the
// semantic conventions' well-known values; the in-memory provider has none, so it uses its own).
static class MessagingSystems
{
    public const string RabbitMq = "rabbitmq";
    public const string ServiceBus = "servicebus";
    public const string InMemory = "zedlabs.inmemory";
}
