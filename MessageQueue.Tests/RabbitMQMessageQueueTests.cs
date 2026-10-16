using ESE.MessageQueue.Exceptions;
using ESE.MessageQueue.RabbitMQ;
using FluentAssertions;
using RabbitMQ.Client;

namespace ESE.MessageQueue.Tests;

public class RabbitMqMessageQueueTests
{
    readonly RabbitMqConfig config = new()
    {
        ConnectionString = "amqp://admin:password123@localhost:5672/",
        QueueName = $"test-queue-{Guid.NewGuid():N}",
        RetryAttempts = 3,
        EnableDeadLetterQueue = true,
        UseQuorumQueues = true,
        QuorumInitialGroupSize = 1
    };

    [Fact]
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
    public void When_ConnectionString_Is_Invalid()
    {
        var invalidConfig = config with { ConnectionString = "amqp://invalid:invalid@nonexistent:5672/" };

        var action = () => new RabbitMqMessageQueue<TestMessage>(invalidConfig);

        action.Should().Throw<MessageQueueException>("it should fail when connection cannot be established")
            .WithMessage("*Failed to initialize RabbitMQ connection*");
    }

    [Fact]
    public void When_Config_Is_Valid()
    {
        if (!Helper.IsRabbitMqAvailable())
        {
            Skip.If(true, "RabbitMQ is not available for testing");
            return;
        }

        var action = () => new RabbitMqMessageQueue<TestMessage>(config);

        action.Should().NotThrow("it should create instance with valid configuration");
    }

    [Fact]
    public async Task When_Publishing_Single_Message()
    {
        if (!Helper.IsRabbitMqAvailable())
        {
            Skip.If(true, "RabbitMQ is not available for testing");
            return;
        }

        using var queue = new RabbitMqMessageQueue<TestMessage>(config);
        var message = new TestMessage("test-id", "test-content");

        var action = () => queue.PublishAsync(message);

        await action.Should().NotThrowAsync("it should publish message without error");
    }

    [Fact]
    public async Task When_Subscribing_With_Handler()
    {
        if (!Helper.IsRabbitMqAvailable())
        {
            Skip.If(true, "RabbitMQ is not available for testing");
            return;
        }

        using var queue = new RabbitMqMessageQueue<TestMessage>(config);

        var action = () => queue.SubscribeAsync(_ => Task.FromResult(true));

        await action.Should().NotThrowAsync("it should subscribe without error");
    }

    [Fact]
    public void When_Using_Quorum_Queues()
    {
        if (!Helper.IsRabbitMqAvailable())
        {
            Skip.If(true, "RabbitMQ is not available for testing");
            return;
        }

        var quorumConfig = config with { UseQuorumQueues = true };

        var action = () => new RabbitMqMessageQueue<TestMessage>(quorumConfig);

        action.Should().NotThrow("it should create instance with quorum queue configuration");
    }

    [Fact]
    public void When_Using_Classic_Queues()
    {
        if (!Helper.IsRabbitMqAvailable())
        {
            Skip.If(true, "RabbitMQ is not available for testing");
            return;
        }

        var classicConfig = config with { UseQuorumQueues = false };

        var action = () => new RabbitMqMessageQueue<TestMessage>(classicConfig);

        action.Should().NotThrow("it should create instance with classic queue configuration");
    }
}
