using ESE.MessageQueue.AzureServiceBus;
using ESE.MessageQueue.Exceptions;
using FluentAssertions;

namespace ESE.MessageQueue.Tests;

[Collection("Azure Service Bus Integration")]
public class AzureServiceBusIntegrationTests : IDisposable
{
    readonly AzureServiceBusConfig? config;
    readonly AzureServiceBusMessageQueue<TestMessage>? queue;

    public AzureServiceBusIntegrationTests()
    {
        var connectionString = Environment.GetEnvironmentVariable("AZURE_SERVICEBUS_CONNECTION_STRING");

        if (string.IsNullOrEmpty(connectionString))
        {
            Skip.If(true, "Azure Service Bus connection string not configured");
            return;
        }

        config = new AzureServiceBusConfig
        {
            ConnectionString = connectionString,
            QueueName = $"integration-test-{Guid.NewGuid():N}",
            RetryAttempts = 3,
            EnableDeadLetterQueue = true,
            MessageTimeToLive = TimeSpan.FromMinutes(30),
            MaxConcurrentCalls = 2,
            PrefetchCount = 5
        };

        try
        {
            queue = new AzureServiceBusMessageQueue<TestMessage>(config);
        }
        catch (MessageQueueException)
        {
            Skip.If(true, "Azure Service Bus is not available for integration testing");
        }
    }

    [Fact]
    public async Task When_Publishing_And_Subscribing()
    {
        if (queue == null) return;

        var receivedMessages = new List<TestMessage>();
        var message = new TestMessage("integration-test", "test-content");

        await queue.SubscribeAsync(msg =>
        {
            receivedMessages.Add(msg);
            return Task.FromResult(true);
        });
        await Task.Delay(1000);
        await queue.PublishAsync(message);
        await Task.Delay(2000);

        receivedMessages.Should().ContainSingle("it should receive the published message");
        receivedMessages[0].Id.Should().Be(message.Id, "it should receive the correct message");
    }

    [Fact]
    public async Task When_Publishing_Batch()
    {
        if (queue == null) return;

        var receivedMessages = new List<TestMessage>();
        var messages = new[]
        {
            new TestMessage("batch-1", "content-1"),
            new TestMessage("batch-2", "content-2"),
            new TestMessage("batch-3", "content-3")
        };

        await queue.SubscribeAsync(msg =>
        {
            receivedMessages.Add(msg);
            return Task.FromResult(true);
        });
        await Task.Delay(1000);
        await queue.PublishAsync(messages);
        await Task.Delay(3000);

        receivedMessages.Should().HaveCount(3, "it should receive all batch messages");
        receivedMessages.Should().Contain(m => m.Id == "batch-1", "it should receive batch message 1");
        receivedMessages.Should().Contain(m => m.Id == "batch-2", "it should receive batch message 2");
        receivedMessages.Should().Contain(m => m.Id == "batch-3", "it should receive batch message 3");
    }

    [Fact]
    public async Task When_Message_Processing_Fails()
    {
        if (queue == null || config == null) return;

        var attemptCount = 0;
        var message = new TestMessage("retry-test", "test-content");

        await queue.SubscribeAsync(_ =>
        {
            attemptCount++;
            return Task.FromResult(false);
        });
        await Task.Delay(1000);
        await queue.PublishAsync(message);
        await Task.Delay(5000);

        attemptCount.Should().BeGreaterThanOrEqualTo(config.RetryAttempts, "it should retry failed messages");
    }

    public void Dispose()
    {
        queue?.Dispose();
    }
}
