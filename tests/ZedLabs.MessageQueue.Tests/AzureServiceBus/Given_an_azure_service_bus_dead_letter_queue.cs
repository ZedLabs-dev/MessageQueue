using System.Transactions;
using AwesomeAssertions;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using ZedLabs.MessageQueue.Abstractions;
using ZedLabs.MessageQueue.AzureServiceBus;
using ZedLabs.MessageQueue.Processing;

namespace ZedLabs.MessageQueue.Tests.AzureServiceBus;

// ADR 0023 on Service Bus: the entity's dead-letter subqueue. Peeks browse without locking; replays and purges receive
// under a lock, settle what they act on and abandon the rest. A replay is a copy like a retry copy (ADR 0009): a new
// broker id (duplicate detection must not drop it), the original id in x-zedlabs-message-id, attempt 1.
[Trait("Category", "Unit")]
public sealed class Given_an_azure_service_bus_dead_letter_queue : IAsyncDisposable
{
    const string TraceParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    readonly FakeServiceBus bus = new();
    readonly RecordingLogger logger = new();

    public ValueTask DisposeAsync() => bus.DisposeAsync();

    FakeDeadLetterSubqueue DeadLetters => bus.DeadLetterQueue("orders");

    // A dead letter the provider wrote after a retry: the broker id is the retry copy's, the original id is a property.
    ServiceBusReceivedMessage AddRetriedDeadLetter(string originalId, string reason = DeadLetterReasons.MaxRetriesExceeded) =>
        DeadLetters.Add($"copy-of-{originalId}", new TestMessage(originalId, "content"), new Dictionary<string, object>
        {
            [ServiceBusMessages.OriginalMessageIdProperty] = originalId,
            [ServiceBusMessages.AttemptProperty] = 3,
            [ServiceBusMessages.DeliveryCountProperty] = int.MaxValue,
            [ServiceBusMessages.DeadLetterReasonProperty] = reason,
            [ServiceBusMessages.DeadLetterDetailProperty] = "System.TimeoutException",
            [ServiceBusMessages.DeadLetteredAtProperty] = 1_700_000_000_000L,
            [ServiceBusMessages.DeadLetterAttemptProperty] = 3,
            [FakeDeadLetterSubqueue.ReasonProperty] = reason,
            [FakeDeadLetterSubqueue.DescriptionProperty] = "System.TimeoutException",
            ["tenant"] = "acme",
            ["traceparent"] = TraceParent,
            ["Diagnostic-Id"] = TraceParent
        }, correlationId: $"corr-{originalId}");

    [Fact]
    public async Task When_Asking_What_It_Supports()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        await using var subscription = bus.CreateQueue(AzureServiceBusTestConfigs.Subscription());

        queue.Capabilities.DeadLetterOperations.Should().Be(DeadLetterOperations.All, "it should support every operation on a queue");
        subscription.Capabilities.DeadLetterOperations.Should().Be(DeadLetterOperations.Peek | DeadLetterOperations.Purge,
            "it should not replay on a subscription, where a resend to the topic reaches every subscription");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Peeking()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        var enqueued = AddRetriedDeadLetter("order-1").EnqueuedTime;

        var letter = (await queue.PeekAsync(10, TestContext.Current.CancellationToken)).Single();

        letter.MessageId.Should().Be("order-1", "it should report the original message id, not the retry copy's broker id");
        letter.CorrelationId.Should().Be("corr-order-1", "it should report the correlation id");
        letter.ReasonCode.Should().Be(DeadLetterReasons.MaxRetriesExceeded, "it should report the reason");
        letter.ReasonDetail.Should().Be("System.TimeoutException", "it should report the detail");
        letter.DeadLetteredAt.Should().Be(DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_000L), "it should report when it was dead-lettered");
        letter.DeliveryAttempt.Should().Be(3, "it should report the attempt it died on");
        letter.EnqueuedAt.Should().Be(enqueued, "it should report when it was enqueued");
        letter.Headers.Should().Equal(new Dictionary<string, string> { ["tenant"] = "acme" },
            "it should report only the application headers, not library, SDK, trace or dead-letter properties");
        letter.Message.Should().Be(new TestMessage("order-1", "content"), "it should report the message");
        DeadLetters.LockedCount.Should().Be(0, "it should browse without locking anything");
        bus.Operations.Should().BeEmpty("it should settle nothing");
        bus.DeadLetterReceivers.Should().ContainSingle("it should use one receiver for the pass")
            .Which.Disposed.Should().BeTrue("it should close the receiver when the pass ends");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Peeking_Twice()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        DeadLetters.Add("a");
        DeadLetters.Add("b");

        await queue.PeekAsync(10, TestContext.Current.CancellationToken);
        var again = await queue.PeekAsync(10, TestContext.Current.CancellationToken);

        again.Select(l => l.MessageId).Should().Equal(["a", "b"], "it should start every peek from the beginning of the subqueue");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Peeking_More_Than_One_Page()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        for (var i = 0; i < 250; i++)
            DeadLetters.Add($"m-{i}");

        var letters = await queue.PeekAsync(240, TestContext.Current.CancellationToken);

        letters.Should().HaveCount(240, "it should page through the subqueue");
        letters.Select(l => l.MessageId).Should().OnlyHaveUniqueItems("it should not return a message twice");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_A_Delivery_Was_Dead_Lettered_By_The_Pipeline()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        await queue.SubscribeAsync(AzureServiceBusTestConfigs.Returns(MessageResult.DeadLetter), TestContext.Current.CancellationToken);
        var before = DateTimeOffset.UtcNow;
        await bus.Processor.Deliver(FakeServiceBus.Message("order-1", deliveryCount: 2)).Handled.WaitAsync(TestContext.Current.CancellationToken);

        var letter = (await queue.PeekAsync(10, TestContext.Current.CancellationToken)).Single();

        letter.MessageId.Should().Be("order-1", "it should report the message id");
        letter.ReasonCode.Should().Be(DeadLetterReasons.Rejected, "it should report the pipeline's reason");
        letter.DeliveryAttempt.Should().Be(2, "it should record the attempt the message was dead-lettered on");
        letter.DeadLetteredAt.Should().BeOnOrAfter(before.AddMilliseconds(-1), "it should record when it was dead-lettered")
            .And.BeOnOrBefore(DateTimeOffset.UtcNow.AddMilliseconds(1));
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Replaying()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        AddRetriedDeadLetter("order-1");

        var result = await queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);

        result.Replayed.Should().Be(1, "it should replay the dead letter");
        DeadLetters.Messages.Should().BeEmpty("it should remove the dead letter once the copy is sent");
        var copy = bus.Sender.Sent.Should().ContainSingle("it should send one copy to the queue").Which;
        bus.Sender.EntityPath.Should().Be("orders", "it should send the copy to the queue itself");
        copy.MessageId.Should().NotBe("order-1", "it should give the copy a new broker id, so duplicate detection cannot drop it")
            .And.NotBe("copy-of-order-1");
        copy.ApplicationProperties.Should().Contain(ServiceBusMessages.OriginalMessageIdProperty, "order-1", "it should keep the original id, the idempotency key");
        copy.ApplicationProperties.Should().Contain(ServiceBusMessages.AttemptProperty, 1, "it should make the copy a first attempt");
        copy.ApplicationProperties.Should().Contain(ServiceBusMessages.DeliveryCountProperty, 1, "it should mark it as the library's own copy");
        copy.ApplicationProperties.Keys.Should().NotContain([ServiceBusMessages.DeadLetterReasonProperty, ServiceBusMessages.DeadLetterDetailProperty,
            ServiceBusMessages.DeadLetteredAtProperty, ServiceBusMessages.DeadLetterAttemptProperty, FakeDeadLetterSubqueue.ReasonProperty,
            FakeDeadLetterSubqueue.DescriptionProperty, "Diagnostic-Id"], "it should strip every dead-letter, SDK and stale library property");
        copy.ApplicationProperties.Should().Contain("tenant", "acme", "it should keep the application headers");
        copy.ApplicationProperties.Should().Contain("traceparent", TraceParent, "it should keep the trace context");
        copy.CorrelationId.Should().Be("corr-order-1", "it should keep the correlation id");
        copy.Body.ToObjectFromJson<TestMessage>(new System.Text.Json.JsonSerializerOptions(System.Text.Json.JsonSerializerDefaults.Web))
            .Should().Be(new TestMessage("order-1", "content"), "it should send the same body");
        bus.Operations.Should().Equal(["send:" + copy.MessageId + ":tx", "dlq-complete:copy-of-order-1:tx"],
            "it should send the copy and complete the dead letter in one transaction, send first");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Replayed_Copy_Is_Delivered()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        AddRetriedDeadLetter("order-1");
        await queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);
        var copy = bus.Sender.Sent.Single();
        var contexts = new List<MessageContext>();
        await queue.SubscribeAsync((_, context, _) =>
        {
            contexts.Add(context);
            return Task.FromResult(MessageResult.Acknowledge);
        }, TestContext.Current.CancellationToken);

        await bus.Processor.Deliver(ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: copy.Body, messageId: copy.MessageId, correlationId: copy.CorrelationId,
            properties: copy.ApplicationProperties, deliveryCount: 1, lockedUntil: DateTimeOffset.UtcNow.AddMinutes(1)))
            .Handled.WaitAsync(TestContext.Current.CancellationToken);

        var context = contexts.Should().ContainSingle("it should deliver the replayed copy").Which;
        context.MessageId.Should().Be("order-1", "it should present the original message id to the handler");
        context.DeliveryAttempt.Should().Be(1, "it should give the replay the full retry budget, not treat its markers as forged");
        context.CorrelationId.Should().Be("corr-order-1", "it should keep the correlation id");
        context.Headers.Should().Contain("tenant", "acme", "it should keep the headers");
        context.TraceParent.Should().Be(TraceParent, "it should continue the original trace");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Entity_Refuses_Transactions()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue(), logger);
        AddRetriedDeadLetter("order-1");
        AddRetriedDeadLetter("order-2");
        bus.Sender.SendFailure = _ => Transaction.Current is null ? null : new InvalidOperationException("transactions are not supported");

        var result = await queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);

        result.Replayed.Should().Be(2, "it should fall back to send-then-complete");
        DeadLetters.Messages.Should().BeEmpty("it should complete both dead letters");
        bus.Operations.Where(op => op.StartsWith("dlq-complete", StringComparison.Ordinal)).Should().AllSatisfy(op =>
            op.Should().NotEndWith(":tx", "it should complete outside the refused transaction"));
        bus.Operations.Count(op => op.StartsWith("send:", StringComparison.Ordinal) && op.EndsWith(":tx", StringComparison.Ordinal))
            .Should().Be(1, "it should stop trying transactions once the entity refused one");
        logger.Entries.Count(e => e.Message.Contains("refused a transaction", StringComparison.Ordinal)).Should().Be(1, "it should warn once");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Replays_Are_Not_Transactional()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue() with { TransactionalRetries = false });
        AddRetriedDeadLetter("order-1");

        await queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);

        bus.Operations.Should().HaveCount(2).And.NotContain(op => op.EndsWith(":tx"), "it should not open a transaction when they are disabled");
        bus.Operations.First().Should().StartWith("send:", "it should send first, so the worst case is a duplicate and never a loss");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Copy_Cannot_Be_Sent()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue() with { TransactionalRetries = false });
        AddRetriedDeadLetter("order-1");
        bus.Sender.SendFailure = _ => new ServiceBusException("unavailable", ServiceBusFailureReason.ServiceBusy);

        var result = await queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);

        result.Failed.Should().Be(1, "it should report the failure");
        result.Failures.Single().Error.Should().Be(typeof(ServiceBusException).FullName, "it should name the exception type");
        bus.Operations.Should().NotContain(op => op.StartsWith("dlq-complete", StringComparison.Ordinal), "it should never complete a dead letter without its copy");
        bus.Operations.Should().Contain("dlq-abandon:copy-of-order-1", "it should hand the dead letter back");
        DeadLetters.Messages.Should().ContainSingle("it should keep the dead letter");
        DeadLetters.LockedCount.Should().Be(0, "it should leave it unlocked for the next pass");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Replaying_With_A_Filter()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        AddRetriedDeadLetter("order-1");
        AddRetriedDeadLetter("order-2", DeadLetterReasons.DeserializationFailed);

        var result = await queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);

        result.Replayed.Should().Be(1, "it should replay the selected dead letter");
        result.Skipped.Should().Be(1, "it should skip one that would fail to deserialize again");
        DeadLetters.Messages.Select(m => m.MessageId).Should().Equal(["copy-of-order-2"], "it should keep the skipped one");
        DeadLetters.LockedCount.Should().Be(0, "it should abandon what it skipped when the pass ends");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Pass_Stops_Before_Using_Every_Received_Message()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        for (var i = 0; i < 5; i++)
            AddRetriedDeadLetter($"order-{i}");

        await queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 2 }, TestContext.Current.CancellationToken);

        bus.Sender.Sent.Should().HaveCount(2, "it should replay MaxCount");
        DeadLetters.Messages.Should().HaveCount(3, "it should keep the rest");
        DeadLetters.LockedCount.Should().Be(0, "it should abandon every message it received but did not use");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Locks_Expire_During_A_Pass()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        for (var i = 0; i < 30; i++)
            DeadLetters.Add($"m-{i}", properties: new Dictionary<string, object> { [FakeDeadLetterSubqueue.ReasonProperty] = DeadLetterReasons.MessageTooLarge });
        DeadLetters.BeforeReceive = DeadLetters.ExpireLocks;

        var result = await queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);

        result.Skipped.Should().BeLessThanOrEqualTo(30, "it should stop when a message comes around again instead of examining it twice");
        DeadLetters.Messages.Should().HaveCount(30, "it should keep every skipped dead letter");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Receiving_From_An_Empty_Subqueue()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());

        var result = await queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);

        result.Matched.Should().Be(0, "it should find nothing");
        bus.DeadLetterReceivers.Single().ReceiveWaits.Should().AllSatisfy(wait =>
            wait.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(5), "it should wait briefly for an empty subqueue, not the SDK's 60 s default"));
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Purging()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        AddRetriedDeadLetter("order-1", DeadLetterReasons.DeserializationFailed);
        AddRetriedDeadLetter("order-2");

        var purged = await queue.PurgeAsync(new DeadLetterPurgeOptions<TestMessage> { MaxCount = 10, ReasonCodes = [DeadLetterReasons.DeserializationFailed] },
            TestContext.Current.CancellationToken);

        purged.Should().Be(1, "it should delete the selected dead letter");
        DeadLetters.Messages.Select(m => m.MessageId).Should().Equal(["copy-of-order-2"], "it should keep the other");
        bus.Sender.Sent.Should().BeEmpty("it should send nothing");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Purging_A_Subscription()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Subscription());
        bus.DeadLetterQueue("events", "billing").Add("event-1");

        var purged = await queue.PurgeAsync(new DeadLetterPurgeOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);

        purged.Should().Be(1, "it should purge the subscription's dead letters");
        bus.DeadLetterReceivers.Single().Should().Match<FakeServiceBusDeadLetterReceiver>(
            r => r.EntityPath == "events" && r.SubscriptionName == "billing", "it should read the subscription's dead-letter subqueue");
    }

    [Theory(Timeout = 10_000)]
    [InlineData("Disposed")]
    [InlineData("Aborted")]
    public async Task When_A_Transactional_Replay_Fails_Transiently(string failure)
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue(), logger);
        AddRetriedDeadLetter("order-1");
        AddRetriedDeadLetter("order-2");
        var calls = 0;
        bus.Sender.SendFailure = _ => Interlocked.Increment(ref calls) != 1 ? null
            : failure == "Disposed" ? new ObjectDisposedException("ServiceBusSender") : new TransactionAbortedException("timed out");

        var result = await queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);

        result.Failed.Should().Be(1, "it should count the message whose transaction failed");
        result.Replayed.Should().Be(1, "it should replay the next one");
        bus.Operations.Where(op => op.StartsWith("send:", StringComparison.Ordinal)).Should().AllSatisfy(op =>
            op.Should().EndWith(":tx", "it should not switch to send-then-complete for a transient failure"));
        DeadLetters.Messages.Select(m => m.MessageId).Should().Equal(["copy-of-order-1"], "it should keep the failed dead letter");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_A_Send_Hangs()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue() with { SendTimeout = TimeSpan.FromMilliseconds(200), TransactionalRetries = false });
        AddRetriedDeadLetter("order-1");
        bus.Sender.SendDelay = TimeSpan.FromSeconds(30);

        var result = await queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);

        result.Failed.Should().Be(1, "it should give up on a send after SendTimeout and count it as failed");
        DeadLetters.Messages.Should().ContainSingle("it should keep the dead letter");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Replaying_Inside_An_Application_Transaction()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        AddRetriedDeadLetter("order-1");

        using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);
            // Not completed: the application's transaction rolls back.
        }

        bus.Sender.Sent.Should().ContainSingle("it should commit the replay in its own transaction, not the caller's");
        DeadLetters.Messages.Should().BeEmpty("it should complete the dead letter regardless of the caller's transaction");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Purging_It_Does_Not_Abandon_What_It_Deleted()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        AddRetriedDeadLetter("order-1");

        await queue.PurgeAsync(new DeadLetterPurgeOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);

        bus.Operations.Should().Equal(["dlq-complete:copy-of-order-1"], "it should complete the dead letter and not try to hand it back afterwards");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Handing_Back_One_Dead_Letter_Fails()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue(), logger);
        AddRetriedDeadLetter("order-1", DeadLetterReasons.DeserializationFailed);
        AddRetriedDeadLetter("order-2", DeadLetterReasons.DeserializationFailed);
        DeadLetters.AbandonFailure = message => message.MessageId == "copy-of-order-1" ? new ServiceBusException("busy", ServiceBusFailureReason.ServiceBusy) : null;

        var result = await queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);

        result.Skipped.Should().Be(2, "it should skip both");
        bus.Operations.Should().Contain("dlq-abandon:copy-of-order-2", "it should hand back the others after one hand-back failed");
        logger.Entries.Should().Contain(e => e.Level == LogLevel.Warning && e.Exception is ServiceBusException, "it should log the failure");
    }

    [Fact]
    public async Task When_The_Queue_Is_Disposed()
    {
        var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        await queue.DisposeAsync();

        var peek = () => queue.PeekAsync(1, TestContext.Current.CancellationToken);
        var purge = () => queue.PurgeAsync(new DeadLetterPurgeOptions<TestMessage> { MaxCount = 1 }, TestContext.Current.CancellationToken);
        var replay = () => queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 1 }, TestContext.Current.CancellationToken);

        await peek.Should().ThrowAsync<ObjectDisposedException>("it should refuse a peek after dispose");
        await purge.Should().ThrowAsync<ObjectDisposedException>("it should refuse a purge after dispose");
        await replay.Should().ThrowAsync<ObjectDisposedException>("it should refuse a replay after dispose");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Broker_Dead_Lettered_The_Message()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        DeadLetters.Add("m-1", properties: new Dictionary<string, object>
        {
            [FakeDeadLetterSubqueue.ReasonProperty] = "MaxDeliveryCountExceeded",
            [FakeDeadLetterSubqueue.DescriptionProperty] = "Message could not be consumed after 10 delivery attempts."
        });

        var letter = (await queue.PeekAsync(1, TestContext.Current.CancellationToken)).Single();

        letter.ReasonCode.Should().Be("MaxDeliveryCountExceeded", "it should report the broker's reason");
        letter.ReasonDetail.Should().Be("Message could not be consumed after 10 delivery attempts.", "it should report the broker's description");
        letter.Headers.Should().BeEmpty("it should not report the broker's properties as headers");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Dead_Letter_Properties_Use_Another_Case()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        DeadLetters.Add("m-1", properties: new Dictionary<string, object> { ["deadletterreason"] = "odd", ["DEADLETTERERRORDESCRIPTION"] = "odd too", ["tenant"] = "acme" });

        await queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);

        bus.Sender.Sent.Single().ApplicationProperties.Keys.Should().BeEquivalentTo(
            [ServiceBusMessages.OriginalMessageIdProperty, ServiceBusMessages.AttemptProperty, ServiceBusMessages.DeliveryCountProperty, "tenant"],
            "it should strip the dead-letter properties whatever their case, as the snapshot does");
    }

    [Fact(Timeout = 30_000)]
    public async Task When_A_Pass_Holds_Many_Skipped_Dead_Letters()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        for (var i = 0; i < 1005; i++)
            DeadLetters.Add($"m-{i}", properties: new Dictionary<string, object> { [FakeDeadLetterSubqueue.ReasonProperty] = DeadLetterReasons.MessageTooLarge });

        var result = await queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);

        result.Skipped.Should().Be(1000, "it should end a pass once it holds 1000 locked dead letters, bounding the memory their bodies use");
        DeadLetters.LockedCount.Should().Be(0, "it should hand them all back");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Subqueue_Returns_Small_Pages()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        DeadLetters.MaxPageSize = 2;
        for (var i = 0; i < 7; i++)
            AddRetriedDeadLetter($"order-{i}");

        var peeked = await queue.PeekAsync(10, TestContext.Current.CancellationToken);
        var result = await queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);

        peeked.Should().HaveCount(7, "it should page through short peeks");
        result.Replayed.Should().Be(7, "it should keep receiving while pages come back short");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Replaying_On_A_Subscription()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Subscription());
        bus.DeadLetterQueue("events", "billing").Add("event-1");

        var replay = () => queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);

        await replay.Should().ThrowAsync<NotSupportedException>("it should refuse to resend to the topic")
            .WithMessage("*every subscription*", "it should say why");
        bus.Senders.Single().Sent.Should().BeEmpty("it should send nothing");
    }
}
