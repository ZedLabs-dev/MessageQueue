using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json.Serialization.Metadata;
using AwesomeAssertions;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Exceptions;
using ESE.MessageQueue.Extensions;
using ESE.MessageQueue.InMemory;
using ESE.MessageQueue.Processing;
using ESE.MessageQueue.Retry;
using ESE.MessageQueue.Tests.Serialization;
using ESE.MessageQueue.Tests.Telemetry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace ESE.MessageQueue.Tests.InMemory;

[Trait("Category", "Unit")]
public sealed class Given_an_in_memory_queue : IAsyncDisposable
{
    public sealed record Order(string Id, string Content);

    public sealed class SelfReferencing
    {
        public SelfReferencing? Self { get; set; }
    }

    static readonly RetryPolicy FastRetry = new()
    {
        MaxRetries = 2,
        InitialDelay = TimeSpan.FromMilliseconds(5),
        MaxDelay = TimeSpan.FromMilliseconds(20),
        UseJitter = false
    };

    readonly List<IAsyncDisposable> created = [];

    InMemoryMessageQueue<T> Create<T>(
        InMemoryConfig? config = null,
        TestTelemetryProvider? telemetry = null,
        ESE.MessageQueue.Telemetry.ITelemetryProvider? otherTelemetry = null) where T : class
    {
        var queue = new InMemoryMessageQueue<T>(config ?? new InMemoryConfig { QueueName = "orders", Retry = FastRetry }, otherTelemetry ?? telemetry);
        created.Add(queue);
        return queue;
    }

    InMemoryMessageQueue<Order> Create(InMemoryConfig? config = null, TestTelemetryProvider? telemetry = null) =>
        Create<Order>(config, telemetry);

    public async ValueTask DisposeAsync()
    {
        foreach (var queue in created)
        {
            await queue.DisposeAsync();
        }
    }

    [Fact]
    public async Task When_Publishing_A_Single_Message()
    {
        var queue = Create();
        var message = new Order("1", "Hello");

        await queue.PublishAsync(message, TestContext.Current.CancellationToken);

        queue.PublishedMessages.Should().ContainSingle("it should record exactly one published message")
            .Which.Should().Be(message, "it should record the published message");
        queue.PendingCount.Should().Be(1, "it should hold the message until a subscriber handles it");
    }

    [Fact]
    public async Task When_Publishing_A_Batch_Of_Messages()
    {
        var queue = Create();

        await queue.PublishAsync([new Order("1", "a"), new Order("2", "b"), new Order("3", "c")], TestContext.Current.CancellationToken);

        queue.PublishedMessages.Should().HaveCount(3, "it should record every published message");
        queue.PendingCount.Should().Be(3, "it should queue every message");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_A_Subscriber_Is_Running()
    {
        var queue = Create();
        await queue.PublishAsync([new Order("1", "a"), new Order("2", "b")], TestContext.Current.CancellationToken);

        await queue.SubscribeAsync(_ => Task.FromResult(true), TestContext.Current.CancellationToken);
        await queue.WaitForIdleAsync(TestContext.Current.CancellationToken);

        queue.ProcessedMessages.Select(m => m.Id).Should().BeEquivalentTo(["1", "2"],
            "it should deliver queued messages on a background loop");
        queue.PendingCount.Should().Be(0, "it should have nothing left once every message is acknowledged");
        queue.IsSubscribed.Should().BeTrue("it should report the active subscription");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Delivering_A_Message()
    {
        var queue = Create();
        var published = new Order("1", "a");
        Order? received = null;
        MessageContext? context = null;
        await queue.SubscribeAsync((m, c, _) =>
        {
            received = m;
            context = c;
            return Task.FromResult(MessageResult.Acknowledge);
        }, TestContext.Current.CancellationToken);

        await queue.PublishAsync(published, TestContext.Current.CancellationToken);
        await queue.WaitForIdleAsync(TestContext.Current.CancellationToken);

        received.Should().Be(published, "it should deliver an equal message").And.NotBeSameAs(published,
            "it should deliver a deserialized copy, as a broker would, so serialization problems show up in development");
        Guid.Parse(context!.MessageId).Version.Should().Be(7, "it should generate a version 7 message id");
        context.DeliveryAttempt.Should().Be(1, "it should mark the first delivery as attempt 1");
        context.Timestamp.Should().BeCloseTo(DateTimeOffset.UtcNow, TimeSpan.FromSeconds(10), "it should stamp the publish time");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Publishing_With_Options()
    {
        var queue = Create();
        MessageContext? context = null;
        await queue.SubscribeAsync((_, c, _) =>
        {
            context = c;
            return Task.FromResult(MessageResult.Acknowledge);
        }, TestContext.Current.CancellationToken);

        await queue.PublishAsync(new Order("1", "a"), new PublishOptions
        {
            MessageId = "order-1",
            CorrelationId = "checkout-9",
            Headers = new Dictionary<string, string> { ["tenant"] = "acme" }
        }, TestContext.Current.CancellationToken);
        await queue.WaitForIdleAsync(TestContext.Current.CancellationToken);

        context!.MessageId.Should().Be("order-1", "it should use the publisher's idempotency key as the message id");
        context.CorrelationId.Should().Be("checkout-9", "it should carry the correlation id");
        context.Headers.Should().Contain("tenant", "acme", "it should carry the publisher's headers");
    }

    [Fact]
    public async Task When_Publishing_A_Message_That_Cannot_Be_Serialized()
    {
        var queue = Create<SelfReferencing>();
        var cyclic = new SelfReferencing();
        cyclic.Self = cyclic;

        var act = () => queue.PublishAsync(cyclic, TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<MessageQueueException>("it should serialize on publish, like a broker-backed provider");
        queue.PendingCount.Should().Be(0, "it should not queue a message it could not serialize");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Handler_Always_Fails()
    {
        var queue = Create();
        var attempts = new ConcurrentQueue<int>();
        await queue.SubscribeAsync((_, c, _) =>
        {
            attempts.Enqueue(c.DeliveryAttempt);
            return Task.FromResult(MessageResult.Retry);
        }, TestContext.Current.CancellationToken);

        await queue.PublishAsync(new Order("1", "poison"), TestContext.Current.CancellationToken);
        await queue.WaitForIdleAsync(TestContext.Current.CancellationToken);

        attempts.Should().Equal([1, 2, 3], "it should make the first attempt plus MaxRetries retries, then stop (no infinite loop)");
        queue.DeadLetters.Should().ContainSingle("it should dead-letter the message after the last retry")
            .Which.Reason.Should().Be(DeadLetterReasons.MaxRetriesExceeded, "it should record why");
        queue.ProcessedMessages.Should().BeEmpty("it should not count a dead-lettered message as processed");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Boolean_Handler_Always_Returns_False()
    {
        var queue = Create();
        var calls = 0;
        await queue.SubscribeAsync(_ =>
        {
            Interlocked.Increment(ref calls);
            return Task.FromResult(false);
        }, TestContext.Current.CancellationToken);

        await queue.PublishAsync(new Order("1", "poison"), TestContext.Current.CancellationToken);
        await queue.WaitForIdleAsync(TestContext.Current.CancellationToken);

        calls.Should().Be(3, "it should treat false as a retry and stop after MaxRetries");
        queue.DeadLetters.Should().ContainSingle("it should dead-letter the message");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Handler_Fails_Once()
    {
        var queue = Create();
        var calls = 0;
        await queue.SubscribeAsync((_, _, _) =>
            Interlocked.Increment(ref calls) == 1 ? throw new TimeoutException() : Task.FromResult(MessageResult.Acknowledge), TestContext.Current.CancellationToken);

        await queue.PublishAsync(new Order("1", "flaky"), TestContext.Current.CancellationToken);
        await queue.WaitForIdleAsync(TestContext.Current.CancellationToken);

        calls.Should().Be(2, "it should redeliver after a thrown exception");
        queue.ProcessedMessages.Should().ContainSingle("it should process the message once the handler succeeds");
        queue.DeadLetters.Should().BeEmpty("it should not dead-letter a message that succeeded on retry");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Retrying_After_A_Backoff()
    {
        var queue = Create(new InMemoryConfig
        {
            QueueName = "orders",
            Retry = new RetryPolicy { Strategy = RetryStrategy.Fixed, MaxRetries = 1, InitialDelay = TimeSpan.FromMilliseconds(200), UseJitter = false }
        });
        var stopwatch = Stopwatch.StartNew();
        var seen = new ConcurrentQueue<TimeSpan>();
        await queue.SubscribeAsync(_ =>
        {
            seen.Enqueue(stopwatch.Elapsed);
            return Task.FromResult(seen.Count > 1);
        }, TestContext.Current.CancellationToken);

        await queue.PublishAsync(new Order("1", "slow"), TestContext.Current.CancellationToken);
        await queue.WaitForIdleAsync(TestContext.Current.CancellationToken);

        var times = seen.ToArray();
        (times[1] - times[0]).Should().BeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(180),
            "it should wait the backoff delay before redelivering");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Handler_Rejects_A_Message()
    {
        var queue = Create();
        await queue.SubscribeAsync((_, _, _) => Task.FromResult(MessageResult.DeadLetter), TestContext.Current.CancellationToken);

        await queue.PublishAsync(new Order("1", "invalid"), new PublishOptions { MessageId = "m-1" }, TestContext.Current.CancellationToken);
        await queue.WaitForIdleAsync(TestContext.Current.CancellationToken);

        var deadLetter = queue.DeadLetters.Should().ContainSingle("it should dead-letter a rejected message").Subject;
        deadLetter.Reason.Should().Be(DeadLetterReasons.Rejected, "it should record that the handler rejected it");
        deadLetter.Message.Should().Be(new Order("1", "invalid"), "it should keep the message for inspection");
        deadLetter.Context.MessageId.Should().Be("m-1", "it should keep the delivery metadata");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_A_Message_Exceeds_The_Size_Limit()
    {
        var queue = Create(new InMemoryConfig { QueueName = "orders", MaxMessageSizeBytes = 16 });
        var handled = false;
        await queue.SubscribeAsync(_ => Task.FromResult(handled = true), TestContext.Current.CancellationToken);

        await queue.PublishAsync(new Order("1", new string('x', 100)), TestContext.Current.CancellationToken);
        await queue.WaitForIdleAsync(TestContext.Current.CancellationToken);

        handled.Should().BeFalse("it should not hand an oversized message to the handler");
        var deadLetter = queue.DeadLetters.Should().ContainSingle("it should dead-letter the oversized message").Subject;
        deadLetter.Reason.Should().Be(DeadLetterReasons.MessageTooLarge, "it should record why");
        deadLetter.Message.Should().BeNull("it should not deserialize an oversized body");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Dead_Lettering_Is_Disabled()
    {
        var queue = Create(new InMemoryConfig { QueueName = "orders", Retry = FastRetry, EnableDeadLetterQueue = false });
        await queue.SubscribeAsync(_ => Task.FromResult(false), TestContext.Current.CancellationToken);

        await queue.PublishAsync(new Order("1", "poison"), TestContext.Current.CancellationToken);
        await queue.WaitForIdleAsync(TestContext.Current.CancellationToken);

        queue.DeadLetters.Should().BeEmpty("it should drop the message instead of keeping it");
        queue.PendingCount.Should().Be(0, "it should not keep a dropped message pending");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Handlers_Run_Concurrently()
    {
        var queue = Create(new InMemoryConfig { QueueName = "orders", MaxConcurrentCalls = 2 });
        var running = 0;
        var maxRunning = 0;
        await queue.SubscribeAsync(async (_, _, ct) =>
        {
            var now = Interlocked.Increment(ref running);
            InterlockedMax(ref maxRunning, now);
            await Task.Delay(20, ct);
            Interlocked.Decrement(ref running);
            return MessageResult.Acknowledge;
        }, TestContext.Current.CancellationToken);

        await queue.PublishAsync(Enumerable.Range(0, 10).Select(i => new Order($"{i}", "x")), TestContext.Current.CancellationToken);
        await queue.WaitForIdleAsync(TestContext.Current.CancellationToken);

        maxRunning.Should().Be(2, "it should run handlers concurrently up to MaxConcurrentCalls and no further");
        queue.ProcessedMessages.Should().HaveCount(10, "it should process every message");
    }

    [Fact]
    public async Task When_Subscribing_Twice()
    {
        var queue = Create();
        await queue.SubscribeAsync(_ => Task.FromResult(true), TestContext.Current.CancellationToken);

        var act = () => queue.SubscribeAsync(_ => Task.FromResult(true), TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<InvalidOperationException>("it should allow one subscriber per queue");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Unsubscribed()
    {
        var queue = Create();
        var firstSubscriberCalls = 0;
        await queue.SubscribeAsync(_ => Task.FromResult(Interlocked.Increment(ref firstSubscriberCalls) > 0), TestContext.Current.CancellationToken);
        await queue.UnsubscribeAsync(TestContext.Current.CancellationToken);

        await queue.PublishAsync(new Order("1", "later"), TestContext.Current.CancellationToken);

        queue.PendingCount.Should().Be(1, "it should keep the message queued");
        queue.IsSubscribed.Should().BeFalse("it should report that no subscriber is active");

        await queue.SubscribeAsync(_ => Task.FromResult(true), TestContext.Current.CancellationToken);
        await queue.WaitForIdleAsync(TestContext.Current.CancellationToken);
        queue.ProcessedMessages.Should().ContainSingle("it should deliver the kept message to the next subscriber");
        firstSubscriberCalls.Should().Be(0, "it should never deliver to a handler after it unsubscribed");
    }

    [Fact]
    public async Task When_Waiting_For_Idle_With_Nothing_Published()
    {
        var queue = Create();

        var act = () => queue.WaitForIdleAsync(TestContext.Current.CancellationToken);

        await act.Should().CompleteWithinAsync(TimeSpan.FromSeconds(1), "it should already be idle");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Recording_Telemetry()
    {
        var telemetry = new TestTelemetryProvider();
        var queue = Create(telemetry: telemetry);
        await queue.SubscribeAsync(_ => Task.FromResult(true), TestContext.Current.CancellationToken);

        await queue.PublishAsync(new Order("1", "a"), TestContext.Current.CancellationToken);
        await queue.WaitForIdleAsync(TestContext.Current.CancellationToken);

        telemetry.ConsumedSuccessCount.Should().Be(1, "it should record consumption through the shared pipeline");
    }

    [Fact]
    public void When_Reading_Capabilities()
    {
        var capabilities = Create().Capabilities;

        capabilities.Durable.Should().BeFalse("it should admit that messages are lost when the process stops");
        capabilities.DeadLetter.Should().BeTrue("it should keep dead letters for inspection");
        capabilities.DelayedRedelivery.Should().BeTrue("it should wait out the backoff without holding a handler slot");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Clearing_The_Recorded_Messages()
    {
        var queue = Create();
        await queue.SubscribeAsync((_, _, _) => Task.FromResult(MessageResult.DeadLetter), TestContext.Current.CancellationToken);
        await queue.PublishAsync(new Order("1", "a"), TestContext.Current.CancellationToken);
        await queue.WaitForIdleAsync(TestContext.Current.CancellationToken);

        queue.Clear();

        queue.PublishedMessages.Should().BeEmpty("it should clear published messages");
        queue.ProcessedMessages.Should().BeEmpty("it should clear processed messages");
        queue.DeadLetters.Should().BeEmpty("it should clear dead letters");
    }

    [Fact]
    public async Task When_Registering_Via_DI()
    {
        var services = new ServiceCollection();
        services.AddInMemoryMessageQueue<Order>();
        await using var provider = services.BuildServiceProvider();

        var queue = provider.GetRequiredService<IMessageQueue<Order>>();

        queue.Should().BeSameAs(provider.GetRequiredService<InMemoryMessageQueue<Order>>(),
            "it should use the same singleton for the interface and the concrete type");
    }

    [Fact]
    public async Task When_Registering_An_Existing_Instance()
    {
        var instance = Create();
        var services = new ServiceCollection();
        services.AddInMemoryMessageQueue(instance);
        await using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<IMessageQueue<Order>>().Should().BeSameAs(instance, "it should register the given instance");
    }

    [Fact]
    public async Task When_The_Container_Is_Disposed()
    {
        var services = new ServiceCollection();
        services.AddInMemoryMessageQueue<Order>(config => config.Retry = FastRetry);
        var provider = services.BuildServiceProvider();
        var queue = provider.GetRequiredService<IMessageQueue<Order>>();

        await provider.DisposeAsync();
        var act = () => queue.PublishAsync(new Order("1", "late"), TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<ObjectDisposedException>("it should be disposed with the container that created it");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Telemetry_Throws()
    {
        var telemetry = new ThrowingTelemetryProvider();
        var queue = new InMemoryMessageQueue<Order>(new InMemoryConfig { QueueName = "orders", Retry = FastRetry }, telemetry);
        created.Add(queue);
        await queue.SubscribeAsync((_, context, _) =>
            Task.FromResult(context.DeliveryAttempt == 1 ? MessageResult.Retry : MessageResult.Acknowledge), TestContext.Current.CancellationToken);

        await queue.PublishAsync(new Order("1", "a"), TestContext.Current.CancellationToken);
        await queue.WaitForIdleAsync(TestContext.Current.CancellationToken);
        await queue.UnsubscribeAsync(TestContext.Current.CancellationToken);

        telemetry.RetriedCalls.Should().Be(1, "it should have tried to record the retry");
        queue.ProcessedMessages.Should().ContainSingle("it should still deliver and retry the message when telemetry fails");
        queue.PendingCount.Should().Be(0, "it should settle each message exactly once");
    }

    [Fact]
    public async Task When_The_Retry_Policy_Has_A_Backoff()
    {
        var logger = new RecordingLogger();

        await using var queue = new InMemoryMessageQueue<Order>(new InMemoryConfig { QueueName = "orders" }, logger: logger);

        logger.Entries.Should().NotContain(e => e.Level == LogLevel.Warning, "it should not warn: it honours the backoff");
    }

    static void InterlockedMax(ref int target, int value)
    {
        var current = Volatile.Read(ref target);
        while (value > current)
        {
            var observed = Interlocked.CompareExchange(ref target, value, current);
            if (observed == current)
                return;
            current = observed;
        }
    }

    [Fact(Timeout = 10_000)]
    public async Task When_A_Handler_Throws_An_Exception_With_A_Very_Long_Type_Name()
    {
        var queue = Create(new InMemoryConfig { QueueName = "orders", Retry = RetryPolicy.None });
        await queue.SubscribeAsync((_, _, _) => throw LongNamedExceptions.Create(), TestContext.Current.CancellationToken);

        await queue.PublishAsync(new Order("1", "throws"), TestContext.Current.CancellationToken);
        await queue.WaitForIdleAsync(TestContext.Current.CancellationToken);

        queue.DeadLetters.Should().ContainSingle("it should dead-letter the message").Which.Detail.Should()
            .HaveLength(PublishOptions.MaxHeaderValueLength, "it should cut the recorded exception type to the shared limit");
    }

    [Fact]
    public async Task When_The_Queue_Has_Json_Metadata()
    {
        var resolver = new RecordingJsonResolver();
        var queue = Create<TestMessage>(new InMemoryConfig { QueueName = "orders", JsonTypeInfoResolver = resolver }, null);

        await queue.PublishAsync(new TestMessage("1", "x"), TestContext.Current.CancellationToken);

        resolver.Requested.Should().Contain(typeof(TestMessage), "it should serialize with the queue's JSON metadata");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_A_Message_With_Json_Metadata_Is_Dead_Lettered()
    {
        // The metadata names the "content" property "body": only a deserializer that uses it reads Content back.
        var resolver = TestJsonContext.Default.WithAddedModifier(info =>
        {
            if (info.Type == typeof(TestMessage))
                info.Properties.Single(p => p.Name == "content").Name = "body";
        });
        var queue = Create<TestMessage>(new InMemoryConfig { QueueName = "orders", JsonTypeInfoResolver = resolver }, null);
        await queue.SubscribeAsync((_, _, _) => Task.FromResult(MessageResult.DeadLetter), TestContext.Current.CancellationToken);

        await queue.PublishAsync(new TestMessage("1", "x"), TestContext.Current.CancellationToken);
        await queue.WaitForIdleAsync(TestContext.Current.CancellationToken);

        queue.DeadLetters.Should().ContainSingle().Which.Message.Should().Be(new TestMessage("1", "x"),
            "it should deserialize the dead-lettered body with the queue's JSON metadata");
    }
}
