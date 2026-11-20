using AwesomeAssertions;
using Azure.Messaging.ServiceBus;
using ZedLabs.MessageQueue.Abstractions;
using ZedLabs.MessageQueue.Exceptions;
using ZedLabs.MessageQueue.Tests.Serialization;

namespace ZedLabs.MessageQueue.Tests.AzureServiceBus;

// Publishing through one ServiceBusSender (§4.3), bounded by SendTimeout, with no client text in errors.
[Trait("Category", "Unit")]
public class Given_an_azure_service_bus_publisher : IAsyncDisposable
{
    readonly FakeServiceBus bus = new();

    // The fake stands in for the SDK client; disposing it with the test class keeps CA1001 honest.
    public ValueTask DisposeAsync() => bus.DisposeAsync();

    [Fact]
    public async Task When_Publishing_A_Message()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue() with { MessageTimeToLive = TimeSpan.FromHours(1) });

        await queue.PublishAsync(new TestMessage("1", "x"), new PublishOptions
        {
            MessageId = "order-1",
            CorrelationId = "checkout-1",
            Headers = new Dictionary<string, string> { ["tenant"] = "acme" }
        }, TestContext.Current.CancellationToken);

        var sent = bus.Sender.Sent.Should().ContainSingle("it should send the message").Which;
        sent.MessageId.Should().Be("order-1", "it should use the publisher's idempotency key");
        sent.CorrelationId.Should().Be("checkout-1", "it should carry the correlation id");
        sent.ContentType.Should().Be("application/json", "it should name the serialization format");
        sent.Subject.Should().Be(nameof(TestMessage), "it should name the message type");
        sent.TimeToLive.Should().Be(TimeSpan.FromHours(1), "it should apply MessageTimeToLive");
        sent.ApplicationProperties.Should().Contain("tenant", "acme", "it should send headers as application properties");
        bus.Sender.EntityPath.Should().Be("orders", "it should publish to the queue");
    }

    [Fact]
    public async Task When_Publishing_Many_Times()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());

        for (var i = 0; i < 5; i++)
            await queue.PublishAsync(new TestMessage($"{i}", "x"), TestContext.Current.CancellationToken);

        bus.Senders.Should().ContainSingle("it should share one thread-safe ServiceBusSender instead of a pool");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Send_Times_Out()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue() with { SendTimeout = TimeSpan.FromMilliseconds(100) });
        bus.Sender.SendDelay = TimeSpan.FromSeconds(30);

        var act = () => queue.PublishAsync(new TestMessage("1", "x"), TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<MessageQueueException>("it should bound a send by SendTimeout").WithMessage("*timed out*");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Caller_Cancels()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        bus.Sender.SendDelay = TimeSpan.FromSeconds(30);
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cancel.CancelAfter(TimeSpan.FromMilliseconds(100));

        var act = () => queue.PublishAsync(new TestMessage("1", "x"), cancel.Token);

        await act.Should().ThrowAsync<OperationCanceledException>("it should honour the caller's cancellation");
    }

    [Fact]
    public async Task When_The_Send_Fails()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        bus.Sender.SendFailure = _ => new ServiceBusException("sb://secret-host.servicebus.windows.net refused", ServiceBusFailureReason.ServiceBusy);

        var act = () => queue.PublishAsync(new TestMessage("1", "x"), TestContext.Current.CancellationToken);

        var error = (await act.Should().ThrowAsync<MessageQueueException>("it should wrap client failures")).Which;
        error.Message.Should().Contain(nameof(ServiceBusException), "it should name the failure type")
            .And.NotContain("secret-host", "it should not echo the client's text, which can name the namespace");
        error.InnerException.Should().BeOfType<ServiceBusException>("it should keep the original for logging");
    }

    [Fact]
    public async Task When_The_Queue_Has_Json_Metadata()
    {
        var resolver = new RecordingJsonResolver();
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue() with { JsonTypeInfoResolver = resolver });

        await queue.PublishAsync(new TestMessage("1", "x"), TestContext.Current.CancellationToken);

        resolver.Requested.Should().Contain(typeof(TestMessage), "it should serialize with the queue's JSON metadata");
        bus.Sender.Sent.Should().ContainSingle().Which.Body.ToString().Should().Be("""{"id":"1","content":"x"}""",
            "it should write the same camelCase JSON");
    }
    static int SizeOf(TestMessage message) =>
        ZedLabs.MessageQueue.Providers.MessageSerializer.Serialize(message, ZedLabs.MessageQueue.Serialization.SerializationFormat.Json).Length;

    [Fact]
    public async Task When_A_Message_Exceeds_MaxMessageSizeBytes()
    {
        var message = new TestMessage("1", "SENSITIVE-" + new string('z', 100));
        var limit = SizeOf(message) - 1;
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue() with { MaxMessageSizeBytes = limit });

        var act = () => queue.PublishAsync(message, TestContext.Current.CancellationToken);

        var error = (await act.Should().ThrowAsync<MessageQueueException>("it should refuse a message above MaxMessageSizeBytes")).Which;
        error.Message.Should().Contain($"{limit + 1} bytes", "it should name the size")
            .And.Contain($"MaxMessageSizeBytes ({limit}) for 'orders'; it was not published", "it should name the limit, the destination and the outcome")
            .And.NotContain("SENSITIVE", "it should not echo the content");
        bus.Sender.Sent.Should().BeEmpty("it should never send an oversized message to the broker");
    }

    [Fact]
    public async Task When_A_Message_Is_Exactly_MaxMessageSizeBytes()
    {
        var message = new TestMessage("1", new string('z', 100));
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue() with { MaxMessageSizeBytes = SizeOf(message) });

        await queue.PublishAsync(message, TestContext.Current.CancellationToken);

        bus.Sender.Sent.Should().ContainSingle("it should send a message exactly at the limit");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task When_A_Batch_Holds_A_Message_Above_MaxMessageSizeBytes(bool batched)
    {
        bus.BatchLimitBytes = 1_000_000;
        var small = new TestMessage("1", "x");
        var large = new TestMessage("2", new string('z', 300));
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue() with
        {
            MaxMessageSizeBytes = SizeOf(large) - 1,
            EnableBatchedOperations = batched
        });

        var act = () => queue.PublishAsync([small, large, small], TestContext.Current.CancellationToken);

        (await act.Should().ThrowAsync<MessageQueueException>("it should refuse a batch holding an oversized message"))
            .WithMessage("*index 1 of the batch*for 'orders'; no message of the batch was published",
                "it should name the oversized message's position and that nothing of the batch was sent");
        bus.Sender.Sent.Should().BeEmpty("it should check the whole batch before sending any of it");
        bus.Sender.Batches.Should().BeEmpty("it should check the whole batch before sending any of it");
    }
    [Fact]
    public async Task When_Publishing_With_Debug_Logging()
    {
        var logger = new RecordingLogger();
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue(), logger);

        await queue.PublishAsync(new TestMessage("1", "SECRET-CONTENT"), new PublishOptions { MessageId = "order-7" }, TestContext.Current.CancellationToken);
        await queue.PublishAsync([new TestMessage("2", "a"), new TestMessage("3", "b")], TestContext.Current.CancellationToken);
        bus.Sender.SendFailure = _ => new ServiceBusException("refused", ServiceBusFailureReason.ServiceBusy);
        var failing = () => queue.PublishAsync(new TestMessage("4", "x"), TestContext.Current.CancellationToken);
        await failing.Should().ThrowAsync<MessageQueueException>();

        logger.Entries.Where(e => e.Level == Microsoft.Extensions.Logging.LogLevel.Debug && e.Message.StartsWith("Published", StringComparison.Ordinal))
            .Select(e => e.Message).Should().Equal(["Published message order-7 to orders", "Published a batch of 2 messages to orders"],
                "it should log each successful publish at Debug level, and not a failed one");
        logger.Entries.Should().NotContain(e => e.Message.Contains("SECRET-CONTENT"), "it should never log message content");
    }
    [Fact]
    public async Task When_The_Logger_Throws_After_A_Successful_Publish()
    {
        var telemetry = new ZedLabs.MessageQueue.Tests.Telemetry.TestTelemetryProvider();
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue(), new ThrowingLogger(), telemetry: telemetry);

        var single = () => queue.PublishAsync(new TestMessage("1", "a"), TestContext.Current.CancellationToken);
        var batch = () => queue.PublishAsync([new TestMessage("2", "b"), new TestMessage("3", "c")], TestContext.Current.CancellationToken);

        await single.Should().NotThrowAsync("it should never let a broken logger fail a publish that succeeded");
        await batch.Should().NotThrowAsync("it should never let a broken logger fail a batch that succeeded");
        telemetry.Published.Select(p => p.ErrorType).Should().Equal([null, null], "it should record both publishes as successful");
    }

    [Fact]
    public async Task When_A_Message_Id_Carries_Line_Breaks()
    {
        var logger = new RecordingLogger();
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue(), logger);

        await queue.PublishAsync(new TestMessage("1", "a"), new PublishOptions { MessageId = "a\r\n2026-10-23 INFO forged line" }, TestContext.Current.CancellationToken);

        logger.Entries.Should().Contain(e => e.Message.Contains("a2026-10-23 INFO forged line"), "it should log the id without its control characters");
        logger.Entries.Should().NotContain(e => e.Message.Contains('\r') || e.Message.Contains('\n'), "it should never write a forged log line");
    }

    [Fact]
    public async Task When_A_Publish_Is_Refused_For_Its_Size()
    {
        var telemetry = new ZedLabs.MessageQueue.Tests.Telemetry.TestTelemetryProvider();
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue() with { MaxMessageSizeBytes = 16 }, telemetry: telemetry);

        var single = () => queue.PublishAsync(new TestMessage("1", new string('x', 100)), TestContext.Current.CancellationToken);
        var batch = () => queue.PublishAsync([new TestMessage("1", new string('x', 100))], TestContext.Current.CancellationToken);
        await single.Should().ThrowAsync<MessageQueueException>();
        await batch.Should().ThrowAsync<MessageQueueException>();

        telemetry.Published.Should().BeEmpty("it should record no publish metric for a message it refused before sending");
        telemetry.Activities.Should().BeEmpty("it should start no publish span for a message it refused before sending");
    }

    [Fact]
    public async Task When_Reading_Its_Shutdown_Budget()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue() with { ShutdownTimeout = TimeSpan.FromSeconds(7) });

        ((ZedLabs.MessageQueue.Hosting.IShutdownBudget)queue).ShutdownTimeout.Should().Be(TimeSpan.FromSeconds(7),
            "it should tell the hosted service how long its drain may take");
    }
}
