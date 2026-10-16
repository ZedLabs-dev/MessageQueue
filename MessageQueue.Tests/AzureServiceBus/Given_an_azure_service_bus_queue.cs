using AwesomeAssertions;
using ESE.MessageQueue.AzureServiceBus;
using ESE.MessageQueue.Exceptions;

namespace ESE.MessageQueue.Tests.AzureServiceBus;

// The ServiceBusClient connects lazily, so constructing the queue needs no broker.
[Trait("Category", "Unit")]
public class Given_an_azure_service_bus_queue
{
    readonly AzureServiceBusConfig config = new()
    {
        ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=test-key",
        QueueName = "test-queue",
        RetryAttempts = 3,
        EnableDeadLetterQueue = true,
        MessageTimeToLive = TimeSpan.FromDays(1),
        MaxConcurrentCalls = 5,
        PrefetchCount = 10
    };

    [Fact]
    public void When_ConnectionString_Is_Invalid()
    {
        var invalidConfig = config with { ConnectionString = "invalid-connection-string" };

        var action = () => new AzureServiceBusMessageQueue<TestMessage>(invalidConfig);

        action.Should().Throw<MessageQueueException>("it should fail when connection string is invalid")
            .WithMessage("*Failed to initialize Azure Service Bus connection*");
    }

    [Fact]
    public void When_Config_Is_Valid()
    {
        var action = () =>
        {
            using var queue = new AzureServiceBusMessageQueue<TestMessage>(config);
        };

        action.Should().NotThrow("it should create instance with valid configuration");
    }
}
