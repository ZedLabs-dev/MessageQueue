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

    [Fact]
    public void When_Config_Has_SenderPoolSize()
    {
        var poolConfig = config with { SenderPoolSize = 4 };

        poolConfig.SenderPoolSize.Should().Be(4, "it should have the configured sender pool size");
    }

    [Fact]
    public void When_Config_Has_MaxConcurrentSends()
    {
        var concurrentConfig = config with { MaxConcurrentSends = 500 };

        concurrentConfig.MaxConcurrentSends.Should().Be(500, "it should have the configured max concurrent sends");
    }

    [Fact]
    public void When_Config_Has_SendTimeout()
    {
        var timeoutConfig = config with { SendTimeout = TimeSpan.FromSeconds(60) };

        timeoutConfig.SendTimeout.Should().Be(TimeSpan.FromSeconds(60), "it should have the configured send timeout");
    }

    [Fact]
    public void When_Config_Has_TopicName()
    {
        var topicConfig = config with { TopicName = "test-topic", SubscriptionName = "test-subscription" };

        topicConfig.TopicName.Should().Be("test-topic", "it should have the configured topic name");
        topicConfig.SubscriptionName.Should().Be("test-subscription", "it should have the configured subscription name");
    }

    [Fact]
    public void When_Config_Has_Default_Values()
    {
        var defaultConfig = new AzureServiceBusConfig
        {
            ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=test-key",
            QueueName = "test-queue"
        };

        defaultConfig.SenderPoolSize.Should().Be(2, "it should have default sender pool size of 2");
        defaultConfig.MaxConcurrentSends.Should().Be(1000, "it should have default max concurrent sends of 1000");
        defaultConfig.SendTimeout.Should().Be(TimeSpan.FromSeconds(30), "it should have default send timeout of 30 seconds");
        defaultConfig.TopicName.Should().BeNull("it should have no topic name by default");
        defaultConfig.SubscriptionName.Should().BeNull("it should have no subscription name by default");
    }

    static bool IsAzureServiceBusAvailable()
    {
        return Environment.GetEnvironmentVariable("AZURE_SERVICEBUS_CONNECTION_STRING") != null;
    }
}

public record TestMessage(string Id, string Content);
