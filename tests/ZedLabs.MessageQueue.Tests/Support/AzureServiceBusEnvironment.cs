namespace ZedLabs.MessageQueue.Tests;

// Settings of the Azure Service Bus integration tests. ServiceBusTestTarget decides which Service Bus they run against:
// a namespace the owner provides, or the emulator once the owner accepts its licence (MQ_ASB_ACCEPT_EULA).
static class AzureServiceBusEnvironment
{
    public const string ConnectionStringVariable = "MQ_ASB_CONNECTION_STRING";
    public const string QueueNameVariable = "MQ_ASB_QUEUE_NAME";
    public const string TopicNameVariable = "MQ_ASB_TOPIC_NAME";
    public const string SubscriptionNameVariable = "MQ_ASB_SUBSCRIPTION_NAME";
    public const string TransactionalRetriesVariable = "MQ_ASB_TRANSACTIONAL_RETRIES";

    // Predeclared in docker/servicebus-emulator-config.json; the emulator cannot create entities at runtime.
    const string DefaultQueueName = "integration-test-queue";
    const string DefaultTopicName = "integration-test-topic";
    const string DefaultSubscriptionName = "integration-test-subscription";

    public static string QueueName => NullIfEmpty(Environment.GetEnvironmentVariable(QueueNameVariable)) ?? DefaultQueueName;

    public static string TopicName => NullIfEmpty(Environment.GetEnvironmentVariable(TopicNameVariable)) ?? DefaultTopicName;

    public static string SubscriptionName => NullIfEmpty(Environment.GetEnvironmentVariable(SubscriptionNameVariable)) ?? DefaultSubscriptionName;

    // Defaults to true. Set it to false for an emulator, Basic tier or partitioned entity that refuses transactions
    // (the provider falls back on its own anyway, after one refused attempt and a warning).
    public static bool TransactionalRetries =>
        !bool.TryParse(Environment.GetEnvironmentVariable(TransactionalRetriesVariable), out var value) || value;

    static string? NullIfEmpty(string? value) => string.IsNullOrWhiteSpace(value) ? null : value;
}
