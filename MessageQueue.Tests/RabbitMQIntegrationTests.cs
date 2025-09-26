using ESE.MessageQueue.Exceptions;
using ESE.MessageQueue.RabbitMQ;
using RabbitMQ.Client;

namespace ESE.MessageQueue.Tests;

[Collection("RabbitMQ Integration")]
public class RabbitMqIntegrationTests : IDisposable
{
    readonly RabbitMqMessageQueue<TestMessage> queue = null!;
    readonly RabbitMqConfig config;
        
    public RabbitMqIntegrationTests()
    {
        config = new RabbitMqConfig
        {
            ConnectionString = "amqp://admin:password123@localhost:5672/",
            QueueName = $"integration-test-{Guid.NewGuid():N}",
            RetryAttempts = 3,
            EnableDeadLetterQueue = true,
            UseQuorumQueues = true,
            QuorumInitialGroupSize = 1
        };

        try
        {
            queue = new RabbitMqMessageQueue<TestMessage>(config);
        }
        catch (MessageQueueException)
        {
            Skip.If(true, "RabbitMQ is not available for integration testing");
        }
    }
    
    public void Dispose()
    {
        try
        {
            var factory = new ConnectionFactory { Uri = new Uri(config.ConnectionString) };
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();
            channel.QueueDelete(config.QueueName);
            channel.QueueDelete($"{config.QueueName}.deadletter");
        }
        catch (Exception)
        {
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

        await Task.Delay(100);

        await queue.PublishAsync(message);

        await Task.Delay(500);

        Assert.Single(receivedMessages);
        Assert.Equal(message.Id, receivedMessages[0].Id);
        Assert.Equal(message.Content, receivedMessages[0].Content);
    }

    [Fact]
    public async Task PublishMultiple_ShouldProcessAllMessages()
    {
        if (queue == null) return;

        var receivedMessages = new List<TestMessage>();
        var messages = new[]
        {
            new TestMessage("msg-1", "content-1"),
            new TestMessage("msg-2", "content-2"),
            new TestMessage("msg-3", "content-3")
        };

        await queue.SubscribeAsync(msg =>
        {
            receivedMessages.Add(msg);
            return Task.FromResult(true);
        });

        await Task.Delay(100);
        await queue.PublishAsync(messages);
        await Task.Delay(1000);

        Assert.Equal(3, receivedMessages.Count);
        Assert.Contains(receivedMessages, m => m.Id == "msg-1");
        Assert.Contains(receivedMessages, m => m.Id == "msg-2");
        Assert.Contains(receivedMessages, m => m.Id == "msg-3");
    }

    [Fact]
    public async Task QuorumQueue_PublishAndSubscribe_ShouldProcessMessage()
    {
        if (queue == null) return;

        var receivedMessages = new List<TestMessage>();
        var message = new TestMessage("quorum-test", "test-content");

        await queue.SubscribeAsync(msg =>
        {
            receivedMessages.Add(msg);
            return Task.FromResult(true);
        });

        await Task.Delay(100);
        await queue.PublishAsync(message);
        await Task.Delay(500);

        Assert.Single(receivedMessages);
        Assert.Equal(message.Id, receivedMessages[0].Id);
    }


}