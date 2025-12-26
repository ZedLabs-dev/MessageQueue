using ESE.MessageQueue.AzureServiceBus;
using ESE.MessageQueue.Exceptions;
using FluentAssertions;

namespace ESE.MessageQueue.Tests;

public class AzureServiceBusMessageQueueTests
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
        if (!IsAzureServiceBusAvailable())
        {
            Skip.If(true, "Azure Service Bus is not available for testing");
            return;
        }

        var action = () => new AzureServiceBusMessageQueue<TestMessage>(config);

        action.Should().NotThrow("it should create instance with valid configuration");
    }

    [Fact]
    public async Task When_Publishing_Single_Message()
    {
        if (!IsAzureServiceBusAvailable())
        {
            Skip.If(true, "Azure Service Bus is not available for testing");
            return;
        }

        using var queue = new AzureServiceBusMessageQueue<TestMessage>(config);
        var message = new TestMessage("test-id", "test-content");

        var action = () => queue.PublishAsync(message);

        await action.Should().NotThrowAsync("it should publish message without error");
    }

    [Fact]
    public async Task When_Publishing_Multiple_Messages()
    {
        if (!IsAzureServiceBusAvailable())
        {
            Skip.If(true, "Azure Service Bus is not available for testing");
            return;
        }

        using var queue = new AzureServiceBusMessageQueue<TestMessage>(config);
        var messages = new[]
        {
            new TestMessage("test-id-1", "test-content-1"),
            new TestMessage("test-id-2", "test-content-2")
        };

        var action = () => queue.PublishAsync(messages);

        await action.Should().NotThrowAsync("it should publish multiple messages without error");
    }

    [Fact]
    public async Task When_Subscribing_With_Handler()
    {
        if (!IsAzureServiceBusAvailable())
        {
            Skip.If(true, "Azure Service Bus is not available for testing");
            return;
        }

        using var queue = new AzureServiceBusMessageQueue<TestMessage>(config);

        var action = () => queue.SubscribeAsync(msg => Task.FromResult(true));

        await action.Should().NotThrowAsync("it should subscribe without error");
    }

    static bool IsAzureServiceBusAvailable()
    {
        return Environment.GetEnvironmentVariable("AZURE_SERVICEBUS_CONNECTION_STRING") != null;
    }
}

public record TestMessage(string Id, string Content);