using System.Collections.Concurrent;
using System.Diagnostics;
using AwesomeAssertions;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Retry;

namespace ESE.MessageQueue.Tests.RabbitMQ;

// Retry and dead-letter queues can disappear or be replaced behind the library's back. The transport must
// heal what it can, never lose the original before its copy is safe, and back off (not spin) when it cannot.
[Trait("Category", "Integration")]
[Collection(RabbitMqCollection.Name)]
public sealed class Given_a_rabbitmq_queue_whose_topology_changes : IAsyncLifetime
{
    static readonly RetryPolicy HundredMilliseconds = new()
    {
        MaxRetries = 3,
        InitialDelay = TimeSpan.FromMilliseconds(100),
        MaxDelay = TimeSpan.FromMilliseconds(100),
        UseJitter = false
    };

    readonly RabbitMqBrokerFixture broker;
    readonly RabbitMqTestQueues queues;

    public Given_a_rabbitmq_queue_whose_topology_changes(RabbitMqBrokerFixture broker)
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

    // Lets one message fail once so its retry queue is declared and cached, then returns that queue's name.
    static async Task<string> WarmRetryQueueAsync(RabbitMqBrokerFixture broker, string queueName, ConcurrentDictionary<string, bool> done)
    {
        await Wait.UntilAsync(() => done.ContainsKey("warm"), Wait.DeliveryTimeout);
        done.Should().ContainKey("warm", "it should retry the warm-up message once");
        var retryQueues = await broker.ListQueuesAsync($"{queueName}.retry.");
        retryQueues.Should().ContainSingle("it should have declared one retry queue for the warm-up retry");
        return retryQueues[0];
    }

    [Theory(Timeout = 60_000)]
    [InlineData(true)]
    [InlineData(false)]
    public async Task When_The_Dead_Letter_Queue_Was_Deleted(bool quorum)
    {
        var config = queues.NewConfig("dlq-deleted-test") with { UseQuorumQueues = quorum };
        var queue = queues.Create(config);
        await queue.InitializeAsync(TestContext.Current.CancellationToken);
        await broker.DeleteQueuesAndExchangesAsync([config.EffectiveDeadLetterQueueName], []);
        var calls = 0;

        await queue.SubscribeAsync((_, _, _) =>
        {
            Interlocked.Increment(ref calls);
            return Task.FromResult(MessageResult.DeadLetter);
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("reject", "x"), TestContext.Current.CancellationToken);
        await Wait.UntilAsync(async () => (await broker.ListQueuesAsync(config.EffectiveDeadLetterQueueName)).Count == 1
            && await broker.GetMessageCountAsync(config.EffectiveDeadLetterQueueName) == 1, Wait.DeliveryTimeout);

        (await broker.GetMessageCountAsync(config.EffectiveDeadLetterQueueName)).Should().Be(1u,
            "it should declare the dead-letter queue again and deliver the dead letter to it");
        calls.Should().Be(1, "it should dead-letter on the first attempt instead of looping");
    }

    [Theory(Timeout = 60_000)]
    [InlineData(true)]
    [InlineData(false)]
    public async Task When_A_Retry_Queue_Was_Deleted_Under_The_Cache(bool publisherConfirms)
    {
        var config = queues.NewConfig("retry-deleted-test") with { Retry = HundredMilliseconds, EnablePublisherConfirms = publisherConfirms };
        var queue = queues.Create(config);
        var calls = new ConcurrentQueue<string>();
        var done = new ConcurrentDictionary<string, bool>();
        await queue.SubscribeAsync((message, context, _) =>
        {
            calls.Enqueue($"{message.Id}@{context.DeliveryAttempt}");
            if (context.DeliveryAttempt < 2)
                return Task.FromResult(MessageResult.Retry);
            done[message.Id] = true;
            return Task.FromResult(MessageResult.Acknowledge);
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("warm", "x"), TestContext.Current.CancellationToken);
        var retryQueue = await WarmRetryQueueAsync(broker, config.QueueName, done);

        await broker.DeleteQueuesAndExchangesAsync([retryQueue], []);
        await queue.PublishAsync(new TestMessage("second", "x"), TestContext.Current.CancellationToken);
        await Wait.UntilAsync(() => done.ContainsKey("second"), Wait.DeliveryTimeout);

        done.Should().ContainKey("second", "it should declare the retry queue again instead of losing the retry copy");
        calls.Where(c => c.StartsWith("second", StringComparison.Ordinal)).Should().Equal(["second@1", "second@2"],
            "it should retry the message exactly once");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_A_Retry_Queue_Was_Deleted_And_The_Handler_Always_Fails()
    {
        var config = queues.NewConfig("retry-deleted-poison-test") with { Retry = HundredMilliseconds };
        var queue = queues.Create(config);
        var calls = new ConcurrentQueue<string>();
        var done = new ConcurrentDictionary<string, bool>();
        await queue.SubscribeAsync((message, context, _) =>
        {
            calls.Enqueue(message.Id);
            if (message.Id == "warm" && context.DeliveryAttempt == 2)
            {
                done["warm"] = true;
                return Task.FromResult(MessageResult.Acknowledge);
            }
            return Task.FromResult(MessageResult.Retry);
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("warm", "x"), TestContext.Current.CancellationToken);
        var retryQueue = await WarmRetryQueueAsync(broker, config.QueueName, done);

        await broker.DeleteQueuesAndExchangesAsync([retryQueue], []);
        await queue.PublishAsync(new TestMessage("poison", "x"), TestContext.Current.CancellationToken);
        await Wait.ForCountAsync(async () => (int)await broker.GetMessageCountAsync(config.EffectiveDeadLetterQueueName), 1, Wait.DeliveryTimeout);

        (await broker.GetMessageCountAsync(config.EffectiveDeadLetterQueueName)).Should().Be(1u,
            "it should still dead-letter the message once its retries are used up");
        calls.Count(id => id == "poison").Should().Be(HundredMilliseconds.MaxRetries + 1,
            "it should handle the message exactly MaxRetries + 1 times, without extra loops");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_A_Retry_Queue_Keeps_Rejecting_Copies()
    {
        var config = queues.NewConfig("retry-rejected-test") with { Retry = HundredMilliseconds };
        var queue = queues.Create(config);
        var poisonCalls = 0;
        var done = new ConcurrentDictionary<string, bool>();
        await queue.SubscribeAsync((message, context, _) =>
        {
            if (message.Id == "warm")
            {
                if (context.DeliveryAttempt < 2)
                    return Task.FromResult(MessageResult.Retry);
                done["warm"] = true;
                return Task.FromResult(MessageResult.Acknowledge);
            }
            Interlocked.Increment(ref poisonCalls);
            return Task.FromResult(MessageResult.Retry);
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("warm", "x"), TestContext.Current.CancellationToken);
        var retryQueue = await WarmRetryQueueAsync(broker, config.QueueName, done);

        // Replace the retry queue with one that refuses every message, and that the library cannot redeclare.
        await broker.DeleteQueuesAndExchangesAsync([retryQueue], []);
        await broker.DeclareQueueAsync(retryQueue, new Dictionary<string, object?> { ["x-max-length"] = 0, ["x-overflow"] = "reject-publish" });
        await queue.PublishAsync(new TestMessage("stuck", "x"), TestContext.Current.CancellationToken);
        var watch = Stopwatch.StartNew();
        await Task.Delay(TimeSpan.FromSeconds(4), TestContext.Current.CancellationToken);
        var callsIn4Seconds = Volatile.Read(ref poisonCalls);
        await queue.DisposeAsync();

        callsIn4Seconds.Should().BeInRange(2, 10, "it should keep retrying the transport with a growing backoff, not spin");
        (await broker.GetMessageCountAsync(config.QueueName)).Should().Be(1u,
            "it should keep the original until a retry copy is safely stored, so nothing is lost");
        (await broker.GetMessageCountAsync(config.EffectiveDeadLetterQueueName)).Should().Be(0u, "it should not dead-letter a transport failure");
        watch.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(30), "it should stop promptly");
    }

    [Theory(Timeout = 120_000)]
    [InlineData(true)]
    [InlineData(false)]
    public async Task When_Retry_Queues_Outlive_Their_Expiry_Period(bool quorum)
    {
        var config = queues.NewConfig("retry-expiry-test") with
        {
            UseQuorumQueues = quorum,
            RetryQueueExpiresOverride = TimeSpan.FromSeconds(6),
            Retry = new RetryPolicy { MaxRetries = 3, InitialDelay = TimeSpan.FromSeconds(1), MaxDelay = TimeSpan.FromSeconds(1), UseJitter = false }
        };
        var queue = queues.Create(config);
        var cameBack = new ConcurrentDictionary<string, bool>();
        await queue.SubscribeAsync((message, context, _) =>
        {
            if (context.DeliveryAttempt == 1)
                return Task.FromResult(MessageResult.Retry);
            cameBack[message.Id] = true;
            return Task.FromResult(MessageResult.Acknowledge);
        }, TestContext.Current.CancellationToken);

        var published = new List<string>();
        var watch = Stopwatch.StartNew();
        while (watch.Elapsed < TimeSpan.FromSeconds(14))
        {
            var id = $"e-{published.Count}";
            await queue.PublishAsync(new TestMessage(id, "x"), TestContext.Current.CancellationToken);
            published.Add(id);
            await Task.Delay(200, TestContext.Current.CancellationToken);
        }
        await Wait.UntilAsync(() => published.All(cameBack.ContainsKey), TimeSpan.FromSeconds(10));

        published.Where(id => !cameBack.ContainsKey(id)).Should().BeEmpty(
            "it should never lose a retry copy to an expiring retry queue, across more than two expiry periods");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_One_Message_Keeps_Failing_While_Others_Succeed()
    {
        var config = queues.NewConfig("per-message-backoff-test") with { Retry = HundredMilliseconds, MaxConcurrentCalls = 2, PrefetchCount = 4 };
        var queue = queues.Create(config);
        var stuckCalls = 0;
        var done = new ConcurrentDictionary<string, bool>();
        await queue.SubscribeAsync((message, context, _) =>
        {
            if (message.Id == "stuck")
            {
                Interlocked.Increment(ref stuckCalls);
                return Task.FromResult(MessageResult.Retry);
            }
            if (message.Id == "warm" && context.DeliveryAttempt < 2)
                return Task.FromResult(MessageResult.Retry);
            done[message.Id] = true;
            return Task.FromResult(MessageResult.Acknowledge);
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("warm", "x"), TestContext.Current.CancellationToken);
        var retryQueue = await WarmRetryQueueAsync(broker, config.QueueName, done);
        await broker.DeleteQueuesAndExchangesAsync([retryQueue], []);
        await broker.DeclareQueueAsync(retryQueue, new Dictionary<string, object?> { ["x-max-length"] = 0, ["x-overflow"] = "reject-publish" });

        await queue.PublishAsync(new TestMessage("stuck", "x"), TestContext.Current.CancellationToken);
        var watch = Stopwatch.StartNew();
        var i = 0;
        while (watch.Elapsed < TimeSpan.FromSeconds(4))
        {
            await queue.PublishAsync(new TestMessage($"ok-{i++}", "x"), TestContext.Current.CancellationToken);
            await Task.Delay(50, TestContext.Current.CancellationToken);
        }

        Volatile.Read(ref stuckCalls).Should().BeLessThanOrEqualTo(10,
            "it should keep backing off the failing message even while other messages succeed");
        done.Keys.Count(k => k.StartsWith("ok-", StringComparison.Ordinal)).Should().BeGreaterThan(i / 2, "it should keep handling the other messages");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_Unsubscribing_During_A_Transport_Backoff()
    {
        var config = queues.NewConfig("backoff-unsubscribe-test") with { Retry = HundredMilliseconds, ShutdownTimeout = TimeSpan.FromSeconds(20) };
        var queue = queues.Create(config);
        var done = new ConcurrentDictionary<string, bool>();
        await queue.SubscribeAsync((message, context, _) =>
        {
            if (message.Id == "warm" && context.DeliveryAttempt == 2)
            {
                done["warm"] = true;
                return Task.FromResult(MessageResult.Acknowledge);
            }
            return Task.FromResult(MessageResult.Retry);
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("warm", "x"), TestContext.Current.CancellationToken);
        var retryQueue = await WarmRetryQueueAsync(broker, config.QueueName, done);
        await broker.DeleteQueuesAndExchangesAsync([retryQueue], []);
        await broker.DeclareQueueAsync(retryQueue, new Dictionary<string, object?> { ["x-max-length"] = 0, ["x-overflow"] = "reject-publish" });
        await queue.PublishAsync(new TestMessage("stuck", "x"), TestContext.Current.CancellationToken);
        await Task.Delay(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        var watch = Stopwatch.StartNew();
        await queue.UnsubscribeAsync(TestContext.Current.CancellationToken);

        watch.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(1), "it should cut a transport backoff short when the subscription stops");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_A_Delivery_Cannot_Be_Read()
    {
        var config = queues.NewConfig("unreadable-test");
        var queue = queues.Create(config);
        queue.ReadFault = delivery =>
        {
            if (delivery.BasicProperties.MessageId == "unreadable")
                throw new FormatException("injected");
        };
        var handled = new ConcurrentQueue<string>();
        await queue.SubscribeAsync((message, _, _) =>
        {
            handled.Enqueue(message.Id);
            return Task.FromResult(MessageResult.Acknowledge);
        }, TestContext.Current.CancellationToken);

        await queue.PublishAsync(new TestMessage("bad", "x"), new PublishOptions { MessageId = "unreadable" }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("good", "x"), TestContext.Current.CancellationToken);
        await Wait.UntilAsync(() => handled.Contains("good"), Wait.DeliveryTimeout);
        await Wait.ForCountAsync(async () => (int)await broker.GetMessageCountAsync(config.EffectiveDeadLetterQueueName), 1, Wait.DeliveryTimeout);
        var headers = await broker.TakeMessageHeadersAsync(config.EffectiveDeadLetterQueueName);

        handled.Should().Equal(["good"], "it should never hand an unreadable delivery to the handler, nor block the next one");
        headers.Should().Contain("x-ese-dead-letter-reason", "DeliveryFailed", "it should dead-letter it as DeliveryFailed");
        headers.Should().Contain("x-ese-dead-letter-detail", typeof(FormatException).FullName!, "it should record the exception type");
    }
}
