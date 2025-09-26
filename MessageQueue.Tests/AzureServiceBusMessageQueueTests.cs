using ESE.MessageQueue.AzureServiceBus;
using ESE.MessageQueue.Exceptions;

namespace ESE.MessageQueue.Tests;

public class AzureServiceBusMessageQueueTests
{
    readonly AzureServiceBusConfig? config = new()
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
    public void Constructor_InvalidConnectionString_ShouldThrowMessageQueueException()
    {
        var invalidConfig = config with { ConnectionString = "invalid-connection-string" };
        
        var exception = Assert.Throws<MessageQueueException>(() => 
            new AzureServiceBusMessageQueue<TestMessage>(invalidConfig));
        
        Assert.Contains("Failed to initialize Azure Service Bus connection", exception.Message);
    }

    [Fact]
    public void Constructor_ValidConfig_ShouldCreateInstance()
    {
        if (!IsAzureServiceBusAvailable())
        {
            Skip.If(true, "Azure Service Bus is not available for testing");
            return;
        }

        var exception = Record.Exception(() => new AzureServiceBusMessageQueue<TestMessage>(config));
        
        Assert.Null(exception);
    }

    [Fact]
    public async Task PublishAsync_SingleMessage_ShouldNotThrow()
    {
        if (!IsAzureServiceBusAvailable())
        {
            Skip.If(true, "Azure Service Bus is not available for testing");
            return;
        }

        using var queue = new AzureServiceBusMessageQueue<TestMessage>(config);
        var message = new TestMessage("test-id", "test-content");
        
        var exception = await Record.ExceptionAsync(() => queue.PublishAsync(message));
        
        Assert.Null(exception);
    }

    [Fact]
    public async Task PublishAsync_MultipleMessages_ShouldNotThrow()
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
        
        var exception = await Record.ExceptionAsync(() => queue.PublishAsync(messages));
        
        Assert.Null(exception);
    }

    [Fact]
    public async Task SubscribeAsync_WithHandler_ShouldNotThrow()
    {
        if (!IsAzureServiceBusAvailable())
        {
            Skip.If(true, "Azure Service Bus is not available for testing");
            return;
        }

        using var queue = new AzureServiceBusMessageQueue<TestMessage>(config);
        
        var exception = await Record.ExceptionAsync(() => 
            queue.SubscribeAsync(msg => Task.FromResult(true)));
        
        Assert.Null(exception);
    }

    bool IsAzureServiceBusAvailable()
    {
        return Environment.GetEnvironmentVariable("AZURE_SERVICEBUS_CONNECTION_STRING") != null;
    }
}

public record TestMessage(string Id, string Content);