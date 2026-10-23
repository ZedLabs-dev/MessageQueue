using System.Collections.Concurrent;
using System.Text;
using AwesomeAssertions;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Processing;
using ESE.MessageQueue.Providers;
using ESE.MessageQueue.Serialization;
using ESE.MessageQueue.Tests.Telemetry;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace ESE.MessageQueue.Tests.RabbitMQ;

// ADR 0023 on a real broker: the dead-letter queue is read with basic.get on its own channel, replays go through the
// default exchange to the queue with a publisher confirm before the dead letter is acked, and everything a pass held
// goes back with an explicit nack (a quorum queue counts a channel close as a failed delivery).
[Trait("Category", "Integration")]
[Collection(RabbitMqCollection.Name)]
public sealed class Given_a_rabbitmq_dead_letter_queue : IAsyncLifetime
{
    readonly RabbitMqBrokerFixture broker;
    readonly RabbitMqTestQueues queues;
    readonly TestTelemetryProvider telemetry = new();
    readonly ConcurrentQueue<(TestMessage Message, MessageContext Context)> deliveries = new();
    volatile bool accept;

    public Given_a_rabbitmq_dead_letter_queue(RabbitMqBrokerFixture broker)
    {
        this.broker = broker;
        queues = new RabbitMqTestQueues(broker);
    }

    public ValueTask InitializeAsync()
    {
        broker.SkipUnlessAvailable();
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync() => queues.DisposeAsync();

    // Fails every delivery with `failure` until `accept` is set.
    Func<TestMessage, MessageContext, CancellationToken, Task<MessageResult>> Handler(MessageResult failure = MessageResult.Retry) =>
        (message, context, _) =>
        {
            deliveries.Enqueue((message, context));
            return Task.FromResult(accept ? MessageResult.Acknowledge : failure);
        };

    async Task WaitForDepthAsync(string queue, int expected) =>
        await Wait.ForCountAsync(async () => (int)await broker.GetMessageCountAsync(queue), expected, Wait.DeliveryTimeout);

    static Task PublishAsync(ESE.MessageQueue.RabbitMQ.RabbitMqMessageQueue<TestMessage> queue, string id) =>
        queue.PublishAsync(new TestMessage(id, "content"), new PublishOptions
        {
            MessageId = id,
            CorrelationId = $"corr-{id}",
            Headers = new Dictionary<string, string> { ["tenant"] = "acme" }
        }, TestContext.Current.CancellationToken);

    [Fact(Timeout = 60_000)]
    public async Task When_A_Dead_Letter_Is_Peeked_And_Replayed()
    {
        var config = queues.NewConfig("dlq-replay");
        var queue = queues.Create(config, telemetry);
        await queue.SubscribeAsync(Handler(), TestContext.Current.CancellationToken);
        var published = DateTimeOffset.UtcNow;
        await PublishAsync(queue, "order-1");
        await WaitForDepthAsync(config.EffectiveDeadLetterQueueName, 1);
        var original = deliveries.First().Context;

        var letter = (await queue.PeekAsync(10, TestContext.Current.CancellationToken)).Single();

        letter.MessageId.Should().Be("order-1", "it should report the message id");
        letter.CorrelationId.Should().Be("corr-order-1", "it should report the correlation id");
        letter.ReasonCode.Should().Be(DeadLetterReasons.MaxRetriesExceeded, "it should report the reason");
        letter.DeliveryAttempt.Should().Be(3, "it should report the attempt it died on");
        letter.Headers.Should().Equal(new Dictionary<string, string> { ["tenant"] = "acme" }, "it should report only the application headers");
        letter.Message.Should().Be(new TestMessage("order-1", "content"), "it should report the message");
        letter.EnqueuedAt.Should().BeCloseTo(published, TimeSpan.FromSeconds(2), "it should report the publish time (whole seconds in AMQP)");
        letter.DeadLetteredAt.Should().BeCloseTo(DateTimeOffset.UtcNow, TimeSpan.FromSeconds(30), "it should report when it was dead-lettered");
        await WaitForDepthAsync(config.EffectiveDeadLetterQueueName, 1);
        (await broker.GetMessageCountAsync(config.EffectiveDeadLetterQueueName)).Should().Be(1u, "it should leave the dead letter in place after a peek");

        accept = true;
        var result = await queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);
        await Wait.UntilAsync(() => deliveries.Count(d => d.Context.DeliveryAttempt == 1) == 2, Wait.DeliveryTimeout);

        result.Replayed.Should().Be(1, "it should replay the dead letter");
        var replayed = deliveries.Last().Context;
        replayed.DeliveryAttempt.Should().Be(1, "it should deliver the replay as a first attempt, not trusting any old attempt header");
        replayed.MessageId.Should().Be("order-1", "it should keep the message id");
        replayed.CorrelationId.Should().Be("corr-order-1", "it should keep the correlation id");
        replayed.Headers.Should().Equal(original.Headers, "it should keep the application headers and add none");
        replayed.TraceParent.Should().Be(telemetry.InjectedTraceParent, "it should keep the publisher's trace context");
        await WaitForDepthAsync(config.EffectiveDeadLetterQueueName, 0);
        (await broker.GetMessageCountAsync(config.EffectiveDeadLetterQueueName)).Should().Be(0u, "it should ack the dead letter after the confirm");
        (await broker.GetMessageCountAsync(config.QueueName)).Should().Be(0u, "it should leave nothing on the queue once handled");
        telemetry.DeadLettersReplayed.Should().ContainSingle("it should count the replay").Which.Count.Should().Be(1);
    }

    [Fact(Timeout = 60_000)]
    public async Task When_A_Replayed_Message_Fails_Again()
    {
        var config = queues.NewConfig("dlq-refail");
        var queue = queues.Create(config);
        await queue.SubscribeAsync(Handler(), TestContext.Current.CancellationToken);
        await PublishAsync(queue, "order-1");
        await WaitForDepthAsync(config.EffectiveDeadLetterQueueName, 1);

        await queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);
        await Wait.ForCountAsync(() => deliveries.Count, 6, Wait.DeliveryTimeout);
        await WaitForDepthAsync(config.EffectiveDeadLetterQueueName, 1);

        deliveries.Select(d => d.Context.DeliveryAttempt).Should().Equal([1, 2, 3, 1, 2, 3],
            "it should give the replayed message the full retry budget again");
        (await broker.GetMessageCountAsync(config.EffectiveDeadLetterQueueName)).Should().Be(1u, "it should dead-letter it again");
        (await queue.PeekAsync(1, TestContext.Current.CancellationToken)).Single().DeliveryAttempt.Should().Be(3,
            "it should record the attempt of its second life");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_Purging_With_A_Filter()
    {
        var config = queues.NewConfig("dlq-purge");
        var queue = queues.Create(config);
        await queue.SubscribeAsync(Handler(MessageResult.DeadLetter), TestContext.Current.CancellationToken);
        await PublishAsync(queue, "a");
        await PublishAsync(queue, "b");
        await WaitForDepthAsync(config.EffectiveDeadLetterQueueName, 2);

        var purged = await queue.PurgeAsync(new DeadLetterPurgeOptions<TestMessage> { MaxCount = 10, Filter = m => m.MessageId == "a" },
            TestContext.Current.CancellationToken);
        await WaitForDepthAsync(config.EffectiveDeadLetterQueueName, 1);

        purged.Should().Be(1, "it should delete the selected dead letter");
        (await queue.PeekAsync(10, TestContext.Current.CancellationToken)).Select(l => l.MessageId).Should().Equal(["b"],
            "it should return the skipped dead letter to the queue, ready");
    }

    [Fact(Timeout = 120_000)]
    public async Task When_A_Quorum_Dead_Letter_Queue_Is_Peeked_Many_Times()
    {
        var config = queues.NewConfig("dlq-peek-many") with { UseQuorumQueues = true };
        var queue = queues.Create(config);
        await queue.SubscribeAsync(Handler(MessageResult.DeadLetter), TestContext.Current.CancellationToken);
        await PublishAsync(queue, "order-1");
        await WaitForDepthAsync(config.EffectiveDeadLetterQueueName, 1);

        for (var i = 0; i < 25; i++)
            (await queue.PeekAsync(10, TestContext.Current.CancellationToken)).Should().ContainSingle("it should see the dead letter on every peek");
        await WaitForDepthAsync(config.EffectiveDeadLetterQueueName, 1);

        (await broker.GetMessageCountAsync(config.EffectiveDeadLetterQueueName)).Should().Be(1u,
            "it should return what it peeked with an explicit nack, which the quorum queue's delivery limit (20) does not count");
        var headers = await broker.TakeMessageHeadersAsync(config.EffectiveDeadLetterQueueName);
        headers.Should().NotContainKey("x-delivery-count",
            "it should hand back what it read with an explicit nack, not a channel close the broker counts as a failed delivery");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_A_Replay_Skips_A_Message_That_Would_Fail_Again()
    {
        var config = queues.NewConfig("dlq-skip");
        var queue = queues.Create(config);
        await queue.SubscribeAsync(Handler(MessageResult.DeadLetter), TestContext.Current.CancellationToken);
        await broker.PublishRawAsync(config.QueueName, Encoding.UTF8.GetBytes("not json"), new Dictionary<string, string>());
        await PublishAsync(queue, "order-1");
        await WaitForDepthAsync(config.EffectiveDeadLetterQueueName, 2);
        accept = true;

        var result = await queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);
        await WaitForDepthAsync(config.EffectiveDeadLetterQueueName, 1);

        result.Replayed.Should().Be(1, "it should replay the rejected message");
        result.Skipped.Should().Be(1, "it should skip the one that failed to deserialize");
        (await queue.PeekAsync(10, TestContext.Current.CancellationToken)).Single().ReasonCode.Should().Be(DeadLetterReasons.DeserializationFailed,
            "it should keep the skipped dead letter, ready for the next pass");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_Replaying_Through_A_Fanout_Exchange()
    {
        var config = queues.NewConfig("dlq-fanout") with { ExchangeName = $"dlq-fanout-x-{Guid.NewGuid():N}", ExchangeType = "fanout" };
        var queue = queues.Create(config);
        await queue.InitializeAsync(TestContext.Current.CancellationToken);
        var probe = await queues.BindProbeQueueAsync(config.ExchangeName!, string.Empty);
        await queue.SubscribeAsync(Handler(MessageResult.DeadLetter), TestContext.Current.CancellationToken);
        await PublishAsync(queue, "order-1");
        await WaitForDepthAsync(config.EffectiveDeadLetterQueueName, 1);
        await WaitForDepthAsync(probe, 1);
        accept = true;

        await queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);
        await Wait.ForCountAsync(() => deliveries.Count, 2, Wait.DeliveryTimeout);
        await WaitForDepthAsync(probe, 1);

        (await broker.GetMessageCountAsync(probe)).Should().Be(1u,
            "it should send the replay to this queue only, not to every queue bound to the exchange");
    }

    [Fact(Timeout = 120_000)]
    public async Task When_A_Replay_Is_Cancelled_Midway()
    {
        var config = queues.NewConfig("dlq-cancel");
        var queue = queues.Create(config);
        await queue.SubscribeAsync(Handler(MessageResult.DeadLetter), TestContext.Current.CancellationToken);
        for (var i = 0; i < 20; i++)
            await PublishAsync(queue, $"order-{i}");
        await WaitForDepthAsync(config.EffectiveDeadLetterQueueName, 20);
        await queue.UnsubscribeAsync(TestContext.Current.CancellationToken);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var examined = 0;

        var replay = () => queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage>
        {
            MaxCount = 100,
            Filter = _ =>
            {
                if (++examined == 8)
                    cancellation.Cancel();
                return true;
            }
        }, cancellation.Token);

        await replay.Should().ThrowAsync<OperationCanceledException>("it should honor the cancellation");
        await Wait.UntilAsync(async () => await broker.GetMessageCountAsync(config.QueueName) + await broker.GetMessageCountAsync(config.EffectiveDeadLetterQueueName) >= 20,
            Wait.DeliveryTimeout);
        var onQueue = await broker.GetMessageCountAsync(config.QueueName);
        var dead = await broker.GetMessageCountAsync(config.EffectiveDeadLetterQueueName);
        onQueue.Should().BeInRange(7u, 8u, "it should have replayed the messages before the cancellation, and at most the one in hand");
        (onQueue + dead).Should().Be(20u, "it should lose no message: each is replayed and acked, or still a dead letter");
    }

    // Takes every ready message off `queue` (acking) and returns their message ids ("" when none), in order.
    async Task<List<string>> DrainIdsAsync(string queue)
    {
        await using var connection = await new ConnectionFactory { Uri = new Uri(broker.ConnectionString) }.CreateConnectionAsync(TestContext.Current.CancellationToken);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: TestContext.Current.CancellationToken);
        var ids = new List<string>();
        while (await channel.BasicGetAsync(queue, autoAck: true, TestContext.Current.CancellationToken) is { } result)
            ids.Add(result.BasicProperties.MessageId ?? string.Empty);
        return ids;
    }

    async Task<ESE.MessageQueue.RabbitMQ.RabbitMqMessageQueue<TestMessage>> DeadLetteredQueueAsync(ESE.MessageQueue.RabbitMQ.RabbitMqConfig config, int count)
    {
        var queue = queues.Create(config);
        await queue.SubscribeAsync(Handler(MessageResult.DeadLetter), TestContext.Current.CancellationToken);
        for (var i = 0; i < count; i++)
            await PublishAsync(queue, $"order-{i}");
        await WaitForDepthAsync(config.EffectiveDeadLetterQueueName, count);
        await queue.UnsubscribeAsync(TestContext.Current.CancellationToken);
        return queue;
    }

    // A pass whose connection is lost (When_The_Connection_Is_Lost_Mid_Replay) leaves its held dead letters to the broker,
    // which returns them as failed deliveries. Here the connection of a reader holding the dead letter is force-closed 25
    // times, more than the quorum default delivery limit (20) allows.
    [Fact(Timeout = 120_000)]
    public async Task When_Connections_Close_While_A_Pass_Holds_A_Dead_Letter()
    {
        var config = queues.NewConfig("dlq-conn-loss") with { UseQuorumQueues = true };
        await DeadLetteredQueueAsync(config, 1);
        var held = 0;

        for (var i = 0; i < 25; i++)
        {
            var connection = await new ConnectionFactory { Uri = new Uri(broker.ConnectionString), AutomaticRecoveryEnabled = false }
                .CreateConnectionAsync(TestContext.Current.CancellationToken);
            var channel = await connection.CreateChannelAsync(cancellationToken: TestContext.Current.CancellationToken);
            if (await channel.BasicGetAsync(config.EffectiveDeadLetterQueueName, autoAck: false, TestContext.Current.CancellationToken) is not null)
                held++;
            await connection.AbortAsync(TestContext.Current.CancellationToken);
            await connection.DisposeAsync();
            await Wait.UntilAsync(async () => await broker.GetMessageCountAsync(config.EffectiveDeadLetterQueueName) == 1, TimeSpan.FromSeconds(2));
        }
        await Task.Delay(500, TestContext.Current.CancellationToken);

        held.Should().Be(25, "it should still find the dead letter after every forced close");
        (await broker.GetMessageCountAsync(config.EffectiveDeadLetterQueueName)).Should().Be(1u,
            "it should keep a dead letter however often a lost connection returns it (no quorum delivery limit on the dead-letter queue)");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_The_Dead_Letter_Queue_Was_Declared_Without_A_Delivery_Limit()
    {
        var config = queues.NewConfig("dlq-old") with { UseQuorumQueues = true };
        await broker.DeclareQueueAsync(config.EffectiveDeadLetterQueueName, new Dictionary<string, object?> { ["x-queue-type"] = "quorum" });
        var logger = new RecordingLogger();
        var queue = queues.Create(config, logger: logger);

        await queue.InitializeAsync(TestContext.Current.CancellationToken);
        var letters = await queue.PeekAsync(10, TestContext.Current.CancellationToken);

        letters.Should().BeEmpty("it should start against an existing dead-letter queue it cannot redeclare");
        logger.Entries.Should().Contain(e => e.Level == LogLevel.Warning && e.Message.Contains("delivery-limit", StringComparison.Ordinal),
            "it should warn that the existing dead-letter queue keeps the default delivery limit, and name the policy that lifts it");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_A_Purge_Skips_A_Message_Before_The_One_It_Deletes()
    {
        var config = queues.NewConfig("dlq-purge-order");
        var queue = queues.Create(config);
        await queue.SubscribeAsync(Handler(MessageResult.DeadLetter), TestContext.Current.CancellationToken);
        await PublishAsync(queue, "b");
        await WaitForDepthAsync(config.EffectiveDeadLetterQueueName, 1);
        await PublishAsync(queue, "a");
        await WaitForDepthAsync(config.EffectiveDeadLetterQueueName, 2);

        var purged = await queue.PurgeAsync(new DeadLetterPurgeOptions<TestMessage> { MaxCount = 10, Filter = m => m.MessageId == "a" },
            TestContext.Current.CancellationToken);
        await WaitForDepthAsync(config.EffectiveDeadLetterQueueName, 1);

        purged.Should().Be(1, "it should delete the selected dead letter");
        (await DrainIdsAsync(config.EffectiveDeadLetterQueueName)).Should().Equal(["b"],
            "it should ack only the selected delivery, never the skipped ones held before it");
    }

    [Theory(Timeout = 120_000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task When_The_Connection_Is_Lost_Mid_Replay(bool waitForRecovery)
    {
        var config = queues.NewConfig("dlq-kill") with { NetworkRecoveryInterval = TimeSpan.FromMilliseconds(500) };
        var queue = await DeadLetteredQueueAsync(config, 20);
        var examined = 0;

        var result = await queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage>
        {
            MaxCount = 100,
            Filter = _ =>
            {
                if (++examined == 8)
                {
                    broker.CloseConnectionsAsync($"ESE.MessageQueue:{config.QueueName}").GetAwaiter().GetResult();
                    // The client recovers the connection and its channels in the background: the pass must not read
                    // on through a recovered channel, where its earlier delivery tags mean nothing.
                    if (waitForRecovery)
                        Thread.Sleep(3000);
                }
                return true;
            }
        }, TestContext.Current.CancellationToken);
        await Task.Delay(2000, TestContext.Current.CancellationToken);
        var onQueue = await DrainIdsAsync(config.QueueName);
        var dead = await DrainIdsAsync(config.EffectiveDeadLetterQueueName);

        result.Replayed.Should().Be(7, "it should keep the replays done before the connection was lost");
        result.Failed.Should().Be(1, "it should count the message in hand as failed");
        examined.Should().Be(8, "it should end the pass once its channel is gone instead of reading on a recovered channel");
        onQueue.Concat(dead).Distinct().Should().HaveCount(20, "it should lose no message");
        onQueue.Should().HaveCount(7, "it should not publish the message in hand once its channel is gone (its ack would fail: a duplicate)");
        dead.Should().HaveCount(13, "it should leave every message it did not replay in the dead-letter queue");
    }

    [Fact(Timeout = 120_000)]
    public async Task When_A_Message_Without_An_Id_Keeps_Failing()
    {
        var config = queues.NewConfig("dlq-noid");
        var queue = queues.Create(config);
        await queue.SubscribeAsync(Handler(), TestContext.Current.CancellationToken);
        await broker.PublishRawWithAsync(config.QueueName, MessageSerializer.Serialize(new TestMessage("noid", "c"), SerializationFormat.Json),
            properties => properties.MessageId = null);
        for (var i = 0; i < 30; i++)
            await PublishAsync(queue, $"id-{i}");
        await WaitForDepthAsync(config.EffectiveDeadLetterQueueName, 31);
        deliveries.Clear();

        await queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage>
        {
            MaxCount = 200,
            Filter = _ =>
            {
                Thread.Sleep(100);
                return true;
            }
        }, TestContext.Current.CancellationToken);
        await WaitForDepthAsync(config.EffectiveDeadLetterQueueName, 31);

        deliveries.Count(d => d.Context.DeliveryAttempt == 1 && d.Context.MessageId.Length == 0).Should().Be(1,
            "it should replay a message without an id once per call, even when it is dead-lettered again during the call");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_A_Quorum_Dead_Letter_Queue_Is_Peeked()
    {
        var config = queues.NewConfig("dlq-reorder") with { UseQuorumQueues = true };
        var queue = await DeadLetteredQueueAsync(config, 5);
        var before = (await queue.PeekAsync(10, TestContext.Current.CancellationToken)).Select(l => l.MessageId).ToList();

        await queue.PeekAsync(2, TestContext.Current.CancellationToken);
        await Task.Delay(500, TestContext.Current.CancellationToken);
        var after = (await queue.PeekAsync(10, TestContext.Current.CancellationToken)).Select(l => l.MessageId);

        after.Should().Equal(before.Skip(2).Concat(before.Take(2)),
            "it should behave as documented on a quorum queue: the dead letters a peek read go back to the end of the queue");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_A_Pass_Skips_A_Dead_Letter()
    {
        var config = queues.NewConfig("dlq-memory");
        var queue = await DeadLetteredQueueAsync(config, 2);
        var store = queue.DeadLetterStore;
        await using var scan = await store.OpenScanAsync(browseOnly: false, TestContext.Current.CancellationToken);

        var skipped = await TakeWeakAsync(scan);
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        skipped.IsAlive.Should().BeFalse("it should keep only the delivery tag of a dead letter it holds, not the entry and its body");
        (await scan.NextAsync(TestContext.Current.CancellationToken)).Should().NotBeNull("it should still read on after the skipped one");
    }

    // Not async: an async helper's completed state machine could keep the entry reachable and hide a leak or fake one.
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    static Task<WeakReference> TakeWeakAsync(IDeadLetterScan scan) =>
        Task.FromResult(new WeakReference(scan.NextAsync(TestContext.Current.CancellationToken).GetAwaiter().GetResult()!));

    [Fact(Timeout = 60_000)]
    public async Task When_The_Queue_Is_Gone_During_A_Replay()
    {
        var config = queues.NewConfig("dlq-gone");
        var queue = queues.Create(config);
        await queue.SubscribeAsync(Handler(MessageResult.DeadLetter), TestContext.Current.CancellationToken);
        await PublishAsync(queue, "order-1");
        await WaitForDepthAsync(config.EffectiveDeadLetterQueueName, 1);
        await queue.UnsubscribeAsync(TestContext.Current.CancellationToken);
        await broker.DeleteQueuesAndExchangesAsync([config.QueueName], []);

        var result = await queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);
        await WaitForDepthAsync(config.EffectiveDeadLetterQueueName, 1);

        result.Failed.Should().Be(1, "it should report a replay the broker could not route");
        result.Failures.Single().Error.Should().Be(typeof(ESE.MessageQueue.RabbitMQ.PublishRefusedException).FullName,
            "it should publish mandatory, so an unroutable replay is refused instead of dropped");
        (await broker.GetMessageCountAsync(config.EffectiveDeadLetterQueueName)).Should().Be(1u,
            "it should ack the dead letter only after a confirmed, routed publish, so it is never lost");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_The_Dead_Letter_Queue_Is_Disabled()
    {
        var config = queues.NewConfig("dlq-off") with { EnableDeadLetterQueue = false };
        var queue = queues.Create(config);

        var peek = () => queue.PeekAsync(1, TestContext.Current.CancellationToken);

        queue.Capabilities.DeadLetterOperations.Should().Be(DeadLetterOperations.None, "it should have no dead letters to operate on");
        await peek.Should().ThrowAsync<NotSupportedException>("it should refuse dead-letter operations without a dead-letter queue");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_A_Broker_Dead_Lettered_Message_Is_Peeked()
    {
        var config = queues.NewConfig("dlq-broker");
        var queue = queues.Create(config);
        await queue.InitializeAsync(TestContext.Current.CancellationToken);
        await broker.PublishRawWithAsync(config.EffectiveDeadLetterQueueName,
            MessageSerializer.Serialize(new TestMessage("x", "y"), SerializationFormat.Json), properties =>
            {
                properties.MessageId = "broker-1";
                properties.Headers = new Dictionary<string, object?> { ["x-first-death-reason"] = "delivery_limit", ["x-delivery-attempt"] = "zero" };
            });

        var letter = (await queue.PeekAsync(1, TestContext.Current.CancellationToken)).Single();

        letter.ReasonCode.Should().Be("delivery_limit", "it should fall back to the broker's own reason");
        letter.DeliveryAttempt.Should().BeNull("it should not report an attempt it cannot parse");
        letter.Headers.Should().BeEmpty("it should not report broker or library headers as application headers");
    }
}
