using AwesomeAssertions;
using Azure.Messaging.ServiceBus;
using ESE.MessageQueue.AzureServiceBus;
using ESE.MessageQueue.Exceptions;
using ESE.MessageQueue.Retry;
using Microsoft.Extensions.Logging;

namespace ESE.MessageQueue.Tests.AzureServiceBus;

// The ServiceBusClient connects lazily, so constructing the queue needs no broker.
[Trait("Category", "Unit")]
public class Given_an_azure_service_bus_queue
{
    readonly AzureServiceBusConfig config = new()
    {
        ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=test-key",
        QueueName = "test-queue",
        Retry = new RetryPolicy { MaxRetries = 2 },
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

    [Fact]
    public void When_Mapping_A_Received_Message_To_A_Context()
    {
        var enqueued = new DateTimeOffset(2026, 10, 16, 12, 0, 0, TimeSpan.Zero);
        var received = ServiceBusModelFactory.ServiceBusReceivedMessage(
            messageId: "order-1",
            correlationId: "checkout-1",
            enqueuedTime: enqueued,
            deliveryCount: 3,
            properties: new Dictionary<string, object> { ["tenant"] = "acme", ["priority"] = 5 });

        var context = AzureServiceBusMessageQueue<TestMessage>.ToContext(received);

        context.MessageId.Should().Be("order-1", "it should use the broker's message id");
        context.CorrelationId.Should().Be("checkout-1", "it should use the broker's correlation id");
        context.Timestamp.Should().Be(enqueued, "it should use the enqueue time");
        context.DeliveryAttempt.Should().Be(3, "it should use the broker's delivery count as the attempt number");
        context.Headers.Should().Contain("tenant", "acme", "it should expose string application properties as headers");
        context.Headers.Should().Contain("priority", "5", "it should format other application properties invariantly");
    }

    [Fact]
    public async Task When_Reading_Capabilities()
    {
        await using var queue = new AzureServiceBusMessageQueue<TestMessage>(config);

        queue.Capabilities.Durable.Should().BeTrue("it should report that Service Bus keeps messages across restarts");
        queue.Capabilities.DelayedRedelivery.Should().BeFalse("it should admit that retries are redelivered immediately for now");
    }

    [Fact]
    public async Task When_Disposed_Twice()
    {
        var queue = new AzureServiceBusMessageQueue<TestMessage>(config);
        await queue.DisposeAsync();

        var act = async () => await queue.DisposeAsync();

        await act.Should().NotThrowAsync("it should make dispose idempotent, because DI disposes every alias registration");
    }

    [Fact]
    public async Task When_The_Retry_Policy_Has_A_Backoff()
    {
        var logger = new RecordingLogger();

        await using var queue = new AzureServiceBusMessageQueue<TestMessage>(config with { Retry = new RetryPolicy() }, logger: logger);

        logger.Entries.Should().Contain(e => e.Level == LogLevel.Warning && e.Message.Contains("DelayedRedelivery"),
            "it should warn that this provider cannot honour the configured backoff yet");
    }

    [Fact]
    public async Task When_The_Retry_Policy_Never_Retries()
    {
        var logger = new RecordingLogger();

        await using var queue = new AzureServiceBusMessageQueue<TestMessage>(config with { Retry = RetryPolicy.None }, logger: logger);

        logger.Entries.Should().NotContain(e => e.Level == LogLevel.Warning, "it should not warn when there is no backoff to honour");
    }
}
