using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.AzureServiceBus;
using ESE.MessageQueue.Retry;

namespace ESE.MessageQueue.Tests;

// Configs for unit specs that run the Azure Service Bus provider against FakeServiceBus.
static class AzureServiceBusTestConfigs
{
    public const string Secret = "TOPSECRETKEY0123456789";
    public const string ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=" + Secret;

    public static readonly RetryPolicy FastRetry = new()
    {
        MaxRetries = 2,
        InitialDelay = TimeSpan.FromMilliseconds(50),
        MaxDelay = TimeSpan.FromSeconds(1),
        UseJitter = false
    };

    public static AzureServiceBusConfig Queue() => new()
    {
        ConnectionString = ConnectionString,
        QueueName = "orders",
        Retry = FastRetry,
        ShutdownTimeout = TimeSpan.FromSeconds(5)
    };

    public static AzureServiceBusConfig Subscription() => Queue() with { TopicName = "events", SubscriptionName = "billing" };

    public static Func<TestMessage, MessageContext, CancellationToken, Task<MessageResult>> Returns(MessageResult result) =>
        (_, _, _) => Task.FromResult(result);
}
