using AwesomeAssertions;
using Azure.Messaging.ServiceBus;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.AzureServiceBus;
using ESE.MessageQueue.Processing;
using ESE.MessageQueue.Providers;
using ESE.MessageQueue.Retry;
using Microsoft.Extensions.Logging;

namespace ESE.MessageQueue.Tests.AzureServiceBus;

// Queue mode (B5): a retry is a scheduled copy of the message on the same queue, then the original is completed.
[Trait("Category", "Unit")]
public class Given_an_azure_service_bus_queue_retrying : IAsyncDisposable
{
    readonly FakeServiceBus bus = new();

    // The fake stands in for the SDK client; disposing it with the test class keeps CA1001 honest.
    public ValueTask DisposeAsync() => bus.DisposeAsync();

    async Task<FakeServiceBusDelivery> RetryOnceAsync(AzureServiceBusConfig config, ServiceBusReceivedMessage message, CancellationToken cancellationToken)
    {
        await using var queue = bus.CreateQueue(config);
        await queue.SubscribeAsync(AzureServiceBusTestConfigs.Returns(MessageResult.Retry), cancellationToken);
        var delivery = bus.Processor.Deliver(message);
        await delivery.Handled.WaitAsync(cancellationToken);
        return delivery;
    }

    [Fact(Timeout = 10_000)]
    public async Task When_A_Handler_Asks_For_A_Retry()
    {
        var before = DateTimeOffset.UtcNow;

        var delivery = await RetryOnceAsync(AzureServiceBusTestConfigs.Queue(), FakeServiceBus.Message("order-1"), TestContext.Current.CancellationToken);

        var copy = bus.Sender.Sent.Should().ContainSingle("it should send one retry copy").Which;
        copy.ScheduledEnqueueTime.Should().BeOnOrAfter(before + TimeSpan.FromMilliseconds(50), "it should schedule the copy after the backoff delay");
        copy.ScheduledEnqueueTime.Should().BeBefore(DateTimeOffset.UtcNow + TimeSpan.FromMilliseconds(50), "it should not wait longer than the delay");
        copy.ApplicationProperties.Should().Contain(ServiceBusMessages.AttemptProperty, 2, "it should carry the next attempt number");
        copy.ApplicationProperties.Should().Contain(ServiceBusMessages.DeliveryCountProperty, 1, "it should mark the copy as the library's own");
        copy.ApplicationProperties.Should().Contain(ServiceBusMessages.OriginalMessageIdProperty, "order-1", "it should remember the original message id");
        copy.MessageId.Should().NotBe("order-1", "it should give the copy its own id, so duplicate detection cannot drop it");
        delivery.Settlement.Should().Be("complete", "it should complete the original once the copy is sent");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Retries_Are_Transactional()
    {
        await RetryOnceAsync(AzureServiceBusTestConfigs.Queue(), FakeServiceBus.Message("order-1"), TestContext.Current.CancellationToken);

        bus.Operations.Should().HaveCount(2).And.AllSatisfy(op => op.Should().EndWith(":tx", "it should send the copy and complete the original in one transaction"));
        bus.Operations.First().Should().StartWith("send:", "it should send the copy before completing the original");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Retries_Are_Not_Transactional()
    {
        await RetryOnceAsync(AzureServiceBusTestConfigs.Queue() with { TransactionalRetries = false }, FakeServiceBus.Message("order-1"), TestContext.Current.CancellationToken);

        bus.Operations.Should().HaveCount(2).And.NotContain(op => op.EndsWith(":tx"), "it should not open a transaction when they are disabled");
        bus.Operations.First().Should().StartWith("send:", "it should send the copy first, so the worst case is a duplicate and never a loss");
        bus.Operations.Last().Should().Be("complete:order-1", "it should complete the original last");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Retry_Copy_Cannot_Be_Sent()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        await queue.SubscribeAsync(AzureServiceBusTestConfigs.Returns(MessageResult.Retry), TestContext.Current.CancellationToken);
        bus.Sender.SendFailure = _ => new ServiceBusException("unavailable", ServiceBusFailureReason.ServiceBusy);

        var delivery = bus.Processor.Deliver(FakeServiceBus.Message("order-1"));
        await delivery.Handled.WaitAsync(TestContext.Current.CancellationToken);

        delivery.Attempts.Should().NotContain("complete", "it should never complete the original without its copy");
        delivery.Settlement.Should().Be("abandon", "it should hand the message back for redelivery");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_A_Retry_Has_No_Delay()
    {
        var config = AzureServiceBusTestConfigs.Queue() with { Retry = AzureServiceBusTestConfigs.FastRetry with { InitialDelay = TimeSpan.Zero } };

        await RetryOnceAsync(config, FakeServiceBus.Message("order-1"), TestContext.Current.CancellationToken);

        bus.Sender.Sent.Single().ScheduledEnqueueTime.Should().Be(default, "it should send the copy for immediate delivery");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Original_Has_Application_Properties()
    {
        var original = ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: new BinaryData("""{"Id":"1","Content":"x"}"""u8.ToArray()),
            messageId: "order-1",
            correlationId: "checkout-1",
            contentType: "application/json",
            subject: "TestMessage",
            properties: new Dictionary<string, object> { ["tenant"] = "acme", ["Diagnostic-Id"] = "00-a-b-01", ["x-foreign"] = "y" },
            deliveryCount: 1,
            lockedUntil: DateTimeOffset.UtcNow.AddMinutes(1));

        await RetryOnceAsync(AzureServiceBusTestConfigs.Queue(), original, TestContext.Current.CancellationToken);

        var copy = bus.Sender.Sent.Single();
        copy.Body.ToString().Should().Be("""{"Id":"1","Content":"x"}""", "it should copy the body unchanged");
        copy.CorrelationId.Should().Be("checkout-1", "it should keep the correlation id");
        copy.ContentType.Should().Be("application/json", "it should keep the content type");
        copy.ApplicationProperties.Should().Contain("tenant", "acme", "it should keep application properties");
        copy.ApplicationProperties.Keys.Should().NotContain(["Diagnostic-Id", "x-foreign"], "it should drop SDK tracing and foreign reserved properties");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_A_Retry_Copy_Reaches_The_Handler()
    {
        var seen = new TaskCompletionSource<MessageContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        await queue.SubscribeAsync((_, context, _) =>
        {
            seen.TrySetResult(context);
            return Task.FromResult(MessageResult.Acknowledge);
        }, TestContext.Current.CancellationToken);

        bus.Processor.Deliver(FakeServiceBus.Message("copy-id", properties: new Dictionary<string, object>
        {
            [ServiceBusMessages.AttemptProperty] = 2,
            [ServiceBusMessages.DeliveryCountProperty] = 1,
            [ServiceBusMessages.OriginalMessageIdProperty] = "order-1"
        }));
        var context = await seen.Task;

        context.DeliveryAttempt.Should().Be(2, "it should tell the handler which attempt this is");
        context.MessageId.Should().Be("order-1", "it should give the handler the original message id");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Last_Retry_Fails()
    {
        var delivery = await RetryOnceAsync(AzureServiceBusTestConfigs.Queue(), FakeServiceBus.Message("copy-id", properties: new Dictionary<string, object>
        {
            [ServiceBusMessages.AttemptProperty] = 3,
            [ServiceBusMessages.DeliveryCountProperty] = 1
        }), TestContext.Current.CancellationToken);

        bus.Sender.Sent.Should().BeEmpty("it should not retry beyond MaxRetries");
        delivery.Settlement.Should().Be("deadletter", "it should dead-letter after the last retry");
        delivery.DeadLetterReason.Should().Be(DeadLetterReasons.MaxRetriesExceeded, "it should record why");
    }

    [Fact]
    public async Task When_Reading_Capabilities()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());

        queue.Capabilities.Durable.Should().BeTrue("it should report that Service Bus keeps messages across restarts");
        queue.Capabilities.DelayedRedelivery.Should().BeTrue("it should report broker-side delayed redelivery through scheduled copies");
    }

    [Fact]
    public async Task When_The_Retry_Policy_Has_A_Backoff()
    {
        var logger = new RecordingLogger();

        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue() with { Retry = new RetryPolicy() }, logger);

        logger.Entries.Should().NotContain(e => e.Level == LogLevel.Warning, "it should not warn: a queue honours the backoff on the broker");
    }

    public static TheoryData<string> TransactionRefusals => ["InvalidOperation", "NotSupported", "Transaction"];

    static Exception Refusal(string kind) => kind switch
    {
        "InvalidOperation" => new InvalidOperationException("partitioned entity: transaction refused"),
        "NotSupported" => new NotSupportedException("transactions are not implemented"),
        _ => new System.Transactions.TransactionException("transaction failed")
    };

    // P6: partitioned entities, the Basic tier and the emulator can refuse the transaction.
    [Theory(Timeout = 10_000)]
    [MemberData(nameof(TransactionRefusals))]
    public async Task When_The_Entity_Refuses_Transactions(string refusal)
    {
        var logger = new RecordingLogger();
        var config = AzureServiceBusTestConfigs.Queue() with { Retry = AzureServiceBusTestConfigs.FastRetry with { InitialDelay = TimeSpan.FromSeconds(5), MaxDelay = TimeSpan.FromSeconds(5) } };
        await using var queue = bus.CreateQueue(config, logger);
        await queue.SubscribeAsync(AzureServiceBusTestConfigs.Returns(MessageResult.Retry), TestContext.Current.CancellationToken);
        bus.Sender.SendFailure = _ => System.Transactions.Transaction.Current is null ? null : Refusal(refusal);
        var before = DateTimeOffset.UtcNow;

        var first = bus.Processor.Deliver(FakeServiceBus.Message("order-1"));
        await first.Handled.WaitAsync(TestContext.Current.CancellationToken);
        var second = bus.Processor.Deliver(FakeServiceBus.Message("order-2"));
        await second.Handled.WaitAsync(TestContext.Current.CancellationToken);

        bus.Sender.Sent.Should().HaveCount(2, "it should still send each retry copy, without a transaction");
        bus.Sender.Sent.Should().AllSatisfy(copy => copy.ScheduledEnqueueTime.Should().BeOnOrAfter(before + TimeSpan.FromSeconds(5),
            "it should keep the backoff delay"));
        first.Settlement.Should().Be("complete", "it should complete the original instead of abandoning it");
        second.Settlement.Should().Be("complete", "it should complete the original instead of abandoning it");
        bus.Operations.Where(op => op.StartsWith("send:", StringComparison.Ordinal) && op.EndsWith(":tx", StringComparison.Ordinal))
            .Should().ContainSingle("it should stop trying transactions after the first refusal");
        logger.Entries.Where(e => e.Level == LogLevel.Warning && e.Message.Contains("transaction")).Should().ContainSingle(
            "it should warn once that it fell back to send-then-complete");
    }

    // P7: a message resubmitted from the dead-letter queue starts over instead of inheriting the exhausted attempt.
    [Fact(Timeout = 10_000)]
    public async Task When_A_Dead_Letter_Is_Resubmitted()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        await queue.SubscribeAsync(AzureServiceBusTestConfigs.Returns(MessageResult.Retry), TestContext.Current.CancellationToken);
        var properties = new Dictionary<string, object>
        {
            [ServiceBusMessages.AttemptProperty] = 3,
            [ServiceBusMessages.DeliveryCountProperty] = 1,
            [ServiceBusMessages.OriginalMessageIdProperty] = "order-1"
        };
        var deadLettered = bus.Processor.Deliver(FakeServiceBus.Message("copy", properties: properties));
        await deadLettered.Handled.WaitAsync(TestContext.Current.CancellationToken);
        var resubmittedProperties = new Dictionary<string, object>(properties);
        foreach (var (key, value) in deadLettered.Properties!)
            resubmittedProperties[key] = value;

        var resubmitted = bus.Processor.Deliver(FakeServiceBus.Message("copy", deliveryCount: 1, properties: resubmittedProperties));
        await resubmitted.Handled.WaitAsync(TestContext.Current.CancellationToken);

        deadLettered.Settlement.Should().Be("deadletter", "it should dead-letter the exhausted message");
        resubmitted.Settlement.Should().Be("complete", "it should retry a resubmitted message instead of dead-lettering it at once");
        bus.Sender.Sent.Should().ContainSingle("it should send a retry copy for the resubmitted message")
            .Which.ApplicationProperties.Should().Contain(ServiceBusMessages.AttemptProperty, 2, "it should count the resubmission from its first attempt");
    }

    [Fact]
    public async Task When_Transactional_Retries_Run_With_Many_Concurrent_Handlers()
    {
        var logger = new RecordingLogger();

        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue() with { MaxConcurrentCalls = 101 }, logger);

        logger.Entries.Should().Contain(e => e.Level == LogLevel.Warning && e.Message.Contains("TransactionalRetries"),
            "it should warn that each transaction commit blocks a thread-pool thread");
    }

    [Theory]
    [InlineData(100, true)]
    [InlineData(500, false)]
    public async Task When_Transaction_Commits_Cannot_Starve_The_Thread_Pool(int maxConcurrentCalls, bool transactional)
    {
        var logger = new RecordingLogger();

        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue() with
        {
            MaxConcurrentCalls = maxConcurrentCalls,
            TransactionalRetries = transactional
        }, logger);

        logger.Entries.Should().NotContain(e => e.Message.Contains("TransactionalRetries"), "it should not warn below the threshold or without transactions");
    }
}
