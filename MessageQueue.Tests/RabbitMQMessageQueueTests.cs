using ESE.MessageQueue.Exceptions;
using ESE.MessageQueue.RabbitMQ;
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
            // ignored
        }
    }

    [Fact]
    public void Constructor_InvalidConnectionString_ShouldThrowMessageQueueException()
    {
        var invalidConfig = config with { ConnectionString = "amqp://invalid:invalid@nonexistent:5672/" };
        
        var exception = Assert.Throws<MessageQueueException>(() => 
            new RabbitMqMessageQueue<TestMessage>(invalidConfig));
        
        Assert.Contains("Failed to initialize RabbitMQ connection", exception.Message);
    }

    [Fact]
    public void Constructor_ValidConfig_ShouldCreateInstance()
    {
        if (!Helper.IsRabbitMqAvailable())
        {
            Skip.If(true, "RabbitMQ is not available for testing");
            return;
        }

        var exception = Record.Exception(() => new RabbitMqMessageQueue<TestMessage>(config));
        
        Assert.Null(exception);
    }

    [Fact]
    public async Task PublishAsync_SingleMessage_ShouldNotThrow()
    {
        if (!Helper.IsRabbitMqAvailable())
        {
            Skip.If(true, "RabbitMQ is not available for testing");
            return;
        }

        using var queue = new RabbitMqMessageQueue<TestMessage>(config);
        var message = new TestMessage("test-id", "test-content");
        
        var exception = await Record.ExceptionAsync(() => queue.PublishAsync(message));
        
        Assert.Null(exception);
    }

    [Fact]
    public async Task SubscribeAsync_WithHandler_ShouldNotThrow()
    {
        if (!Helper.IsRabbitMqAvailable())
        {
            Skip.If(true, "RabbitMQ is not available for testing");
            return;
        }

        using var queue = new RabbitMqMessageQueue<TestMessage>(config);
        
        var exception = await Record.ExceptionAsync(() => 
            queue.SubscribeAsync(_ => Task.FromResult(true)));
        
        Assert.Null(exception);
    }

    [Fact]
    public void Constructor_WithQuorumQueues_ShouldCreateInstance()
    {
        if (!Helper.IsRabbitMqAvailable())
        {
            Skip.If(true, "RabbitMQ is not available for testing");
            return;
        }

        var quorumConfig = config with { UseQuorumQueues = true };
        var exception = Record.Exception(() => new RabbitMqMessageQueue<TestMessage>(quorumConfig));
        
        Assert.Null(exception);
    }

    [Fact]
    public void Constructor_WithClassicQueues_ShouldCreateInstance()
    {
        if (!Helper.IsRabbitMqAvailable())
        {
            Skip.If(true, "RabbitMQ is not available for testing");
            return;
        }

        var classicConfig = config with { UseQuorumQueues = false };
        var exception = Record.Exception(() => new RabbitMqMessageQueue<TestMessage>(classicConfig));
        
        Assert.Null(exception);
    }
}