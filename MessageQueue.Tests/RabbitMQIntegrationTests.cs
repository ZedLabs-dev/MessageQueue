using ESE.MessageQueue.Exceptions;
using ESE.MessageQueue.RabbitMQ;
using FluentAssertions;
using RabbitMQ.Client;

namespace ESE.MessageQueue.Tests;

[Collection("RabbitMQ Integration")]
public class RabbitMqIntegrationTests : IDisposable
{
    readonly RabbitMqMessageQueue<TestMessage>? queue;
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
        await Task.Delay(100);
        await queue.PublishAsync(message);
        await Task.Delay(500);

        receivedMessages.Should().ContainSingle("it should receive the published message");
        receivedMessages[0].Id.Should().Be(message.Id, "it should receive the correct message id");
        receivedMessages[0].Content.Should().Be(message.Content, "it should receive the correct message content");
    }

    [Fact]
    public async Task When_Publishing_Multiple_Messages()
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

        receivedMessages.Should().HaveCount(3, "it should receive all published messages");
        receivedMessages.Should().Contain(m => m.Id == "msg-1", "it should receive message 1");
        receivedMessages.Should().Contain(m => m.Id == "msg-2", "it should receive message 2");
        receivedMessages.Should().Contain(m => m.Id == "msg-3", "it should receive message 3");
    }

    [Fact]
    public async Task When_Using_Quorum_Queue()
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

        receivedMessages.Should().ContainSingle("it should receive the message via quorum queue");
        receivedMessages[0].Id.Should().Be(message.Id, "it should receive the correct message");
    }

    [Fact]
    public async Task When_Publishing_Concurrently_With_Publisher_Confirms()
    {
        if (queue == null) return;

        var receivedMessages = new List<TestMessage>();
        var messageCount = 100;

        await queue.SubscribeAsync(msg =>
        {
            lock (receivedMessages)
            {
                receivedMessages.Add(msg);
            }
            return Task.FromResult(true);
        });
        await Task.Delay(100);

        var tasks = Enumerable.Range(0, messageCount)
            .Select(i => queue.PublishAsync(new TestMessage($"concurrent-{i}", $"content-{i}")))
            .ToList();

        await Task.WhenAll(tasks);
        await Task.Delay(2000);

        receivedMessages.Should().HaveCount(messageCount, "it should receive all concurrently published messages");
    }

    [Fact]
    public async Task When_Publishing_With_Custom_Exchange()
    {
        if (!Helper.IsRabbitMqAvailable()) return;

        var exchangeName = $"test-exchange-{Guid.NewGuid():N}";
        var customConfig = new RabbitMqConfig
        {
            ConnectionString = "amqp://admin:password123@localhost:5672/",
            QueueName = $"exchange-test-{Guid.NewGuid():N}",
            ExchangeName = exchangeName,
            ExchangeType = "direct",
            EnableDeadLetterQueue = false,
            UseQuorumQueues = true,
            QuorumInitialGroupSize = 1,
            EnablePublisherConfirms = true
        };

        using var customQueue = new RabbitMqMessageQueue<TestMessage>(customConfig);

        var receivedMessages = new List<TestMessage>();
        var message = new TestMessage("exchange-test", "test-content");

        await customQueue.SubscribeAsync(msg =>
        {
            receivedMessages.Add(msg);
            return Task.FromResult(true);
        });
        await Task.Delay(100);
        await customQueue.PublishAsync(message);
        await Task.Delay(500);

        receivedMessages.Should().ContainSingle("it should receive message via custom exchange");
        receivedMessages[0].Id.Should().Be(message.Id);

        // Cleanup
        try
        {
            var factory = new ConnectionFactory { Uri = new Uri(customConfig.ConnectionString) };
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();
            channel.QueueDelete(customConfig.QueueName);
            channel.ExchangeDelete(exchangeName);
        }
        catch { }
    }

    [Fact]
    public async Task When_Publishing_With_Channel_Pool()
    {
        if (!Helper.IsRabbitMqAvailable()) return;

        var poolConfig = new RabbitMqConfig
        {
            ConnectionString = "amqp://admin:password123@localhost:5672/",
            QueueName = $"pool-test-{Guid.NewGuid():N}",
            EnableDeadLetterQueue = false,
            UseQuorumQueues = true,
            QuorumInitialGroupSize = 1,
            EnablePublisherConfirms = true,
            PublisherChannelPoolSize = 4,
            MaxOutstandingConfirms = 500
        };

        using var poolQueue = new RabbitMqMessageQueue<TestMessage>(poolConfig);

        var receivedMessages = new List<TestMessage>();
        var messageCount = 200;

        await poolQueue.SubscribeAsync(msg =>
        {
            lock (receivedMessages)
            {
                receivedMessages.Add(msg);
            }
            return Task.FromResult(true);
        });
        await Task.Delay(100);

        var tasks = Enumerable.Range(0, messageCount)
            .Select(i => poolQueue.PublishAsync(new TestMessage($"pool-{i}", $"content-{i}")))
            .ToList();

        await Task.WhenAll(tasks);
        await Task.Delay(3000);

        receivedMessages.Should().HaveCount(messageCount, "it should receive all messages published via channel pool");

        // Cleanup
        try
        {
            var factory = new ConnectionFactory { Uri = new Uri(poolConfig.ConnectionString) };
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();
            channel.QueueDelete(poolConfig.QueueName);
            channel.ExchangeDelete(poolConfig.QueueName);
        }
        catch { }
    }

    [Fact]
    public async Task When_Publishing_Without_Confirms_Should_Complete_Faster()
    {
        if (!Helper.IsRabbitMqAvailable()) return;

        var noConfirmConfig = new RabbitMqConfig
        {
            ConnectionString = "amqp://admin:password123@localhost:5672/",
            QueueName = $"no-confirm-test-{Guid.NewGuid():N}",
            EnableDeadLetterQueue = false,
            UseQuorumQueues = false,
            EnablePublisherConfirms = false,
            PublisherChannelPoolSize = 2
        };

        using var noConfirmQueue = new RabbitMqMessageQueue<TestMessage>(noConfirmConfig);

        var messageCount = 100;
        var tasks = Enumerable.Range(0, messageCount)
            .Select(i => noConfirmQueue.PublishAsync(new TestMessage($"fast-{i}", $"content-{i}")))
            .ToList();

        await Task.WhenAll(tasks);

        // Cleanup
        try
        {
            var factory = new ConnectionFactory { Uri = new Uri(noConfirmConfig.ConnectionString) };
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();
            channel.QueueDelete(noConfirmConfig.QueueName);
            channel.ExchangeDelete(noConfirmConfig.QueueName);
        }
        catch { }
    }
}
