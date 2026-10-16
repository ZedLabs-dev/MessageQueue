namespace ESE.MessageQueue.Tests;

// Azure Service Bus integration tests run only against a namespace the owner provides: a real one,
// or the local emulator (whose EULA must be accepted by a person, see MessageQueue/docker-compose.dev.yml).
static class AzureServiceBusEnvironment
{
    public const string ConnectionStringVariable = "MQ_ASB_CONNECTION_STRING";
    public const string QueueNameVariable = "MQ_ASB_QUEUE_NAME";

    // Predeclared in MessageQueue/servicebus-emulator-config.json; the emulator cannot create entities at runtime.
    const string DefaultQueueName = "integration-test-queue";

    public static string? ConnectionString => NullIfEmpty(Environment.GetEnvironmentVariable(ConnectionStringVariable));

    public static string QueueName => NullIfEmpty(Environment.GetEnvironmentVariable(QueueNameVariable)) ?? DefaultQueueName;

    public static string RequireConnectionString()
    {
        var connectionString = ConnectionString;
        Assert.SkipWhen(connectionString is null,
            $"{ConnectionStringVariable} is not set. Point it at an Azure Service Bus namespace or a running emulator " +
            $"that has the queue '{QueueName}' ({QueueNameVariable} overrides the name).");
        return connectionString!;
    }

    static string? NullIfEmpty(string? value) => string.IsNullOrWhiteSpace(value) ? null : value;
}
