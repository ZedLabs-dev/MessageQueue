using ESE.MessageQueue.AzureServiceBus;
using ESE.MessageQueue.Exceptions;

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
    public async Task PublishAndSubscribe_ShouldProcessMessage()
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

        Assert.Single(receivedMessages);
        Assert.Equal(message.Id, receivedMessages[0].Id);
    }

    [Fact]
    public async Task PublishBatch_ShouldProcessAllMessages()
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

        Assert.Equal(3, receivedMessages.Count);
        Assert.All(messages, msg => 
            Assert.Contains(receivedMessages, received => received.Id == msg.Id));
    }

    [Fact]
    public async Task FailedMessage_ShouldRetryAndDeadLetter()
    {
        if (queue == null) return;

        var attemptCount = 0;
        var message = new TestMessage("retry-test", "test-content");

        await queue.SubscribeAsync(msg =>
        {
            attemptCount++;
            return Task.FromResult(false);
        });

        await Task.Delay(1000);
        await queue.PublishAsync(message);
        await Task.Delay(5000);

        Assert.True(attemptCount >= config.RetryAttempts);
    }

    public void Dispose()
    {
        queue?.Dispose();
    }
}