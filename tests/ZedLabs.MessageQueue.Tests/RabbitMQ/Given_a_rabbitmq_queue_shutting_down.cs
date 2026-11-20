using System.Collections.Concurrent;
using System.Diagnostics;
using AwesomeAssertions;
using ZedLabs.MessageQueue.Abstractions;
using ZedLabs.MessageQueue.Retry;

namespace ZedLabs.MessageQueue.Tests.RabbitMQ;

// UnsubscribeAsync / DisposeAsync: bounded by ShutdownTimeout, safe from inside a handler, and never the cause of a
// duplicate or of attempts burnt on messages that were never handled (ADR 0008).
[Trait("Category", "Integration")]
[Collection(RabbitMqCollection.Name)]
public sealed class Given_a_rabbitmq_queue_shutting_down : IAsyncLifetime
{
    readonly RabbitMqBrokerFixture broker;
    readonly RabbitMqTestQueues queues;

    public Given_a_rabbitmq_queue_shutting_down(RabbitMqBrokerFixture broker)
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

    public static TheoryData<bool, MessageResult> LateResults => new()
    {
        { true, MessageResult.Acknowledge },
        { true, MessageResult.Retry },
        { true, MessageResult.DeadLetter },
        { false, MessageResult.Retry }
    };

    [Theory(Timeout = 60_000)]
    [MemberData(nameof(LateResults))]
    public async Task When_A_Handler_Outlives_The_Shutdown_Timeout(bool quorum, MessageResult lateResult)
    {
        var config = queues.NewConfig("late-handler-test") with
        {
            UseQuorumQueues = quorum,
            ShutdownTimeout = TimeSpan.FromSeconds(1),
            Retry = new RetryPolicy { MaxRetries = 5, InitialDelay = TimeSpan.FromMilliseconds(50), MaxDelay = TimeSpan.FromMilliseconds(50), UseJitter = false }
        };
        var queue = queues.Create(config);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finished = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await queue.SubscribeAsync(async (_, _, _) =>
        {
            started.TrySetResult();
            await Task.Delay(3000, CancellationToken.None);
            finished.TrySetResult();
            return lateResult;
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("late", "x"), TestContext.Current.CancellationToken);
        await started.Task.WaitAsync(Wait.DeliveryTimeout, TestContext.Current.CancellationToken);

        var watch = Stopwatch.StartNew();
        await queue.UnsubscribeAsync(TestContext.Current.CancellationToken);
        var took = watch.Elapsed;
        await finished.Task.WaitAsync(Wait.DeliveryTimeout, TestContext.Current.CancellationToken);
        await Task.Delay(1500, TestContext.Current.CancellationToken);
        var onMainQueue = new List<IReadOnlyDictionary<string, string>>();
        while (await broker.TakeMessageHeadersAsync(config.QueueName) is { } headers)
        {
            onMainQueue.Add(headers);
        }

        took.Should().BeLessThan(TimeSpan.FromSeconds(2.5), "it should return once ShutdownTimeout (1 s) has passed");
        switch (lateResult)
        {
            case MessageResult.Acknowledge:
                onMainQueue.Should().BeEmpty("it should let the late handler acknowledge the message");
                break;
            case MessageResult.Retry:
                onMainQueue.Should().ContainSingle("it should end with the retry copy only, not the original as well")
                    .Which.Should().Contain("x-delivery-attempt", "2", "it should carry the next attempt");
                break;
            default:
                onMainQueue.Should().BeEmpty("it should not requeue a message the late handler dead-lettered");
                (await broker.GetMessageCountAsync(config.EffectiveDeadLetterQueueName)).Should().Be(1u, "it should dead-letter it once");
                break;
        }
    }

    [Fact(Timeout = 60_000)]
    public async Task When_Publishing_During_A_Slow_Unsubscribe()
    {
        var config = queues.NewConfig("slow-unsubscribe-test") with { ShutdownTimeout = TimeSpan.FromSeconds(5) };
        var queue = queues.Create(config);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await queue.SubscribeAsync(async (_, _, _) =>
        {
            started.TrySetResult();
            await Task.Delay(4000, CancellationToken.None);
            return MessageResult.Acknowledge;
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("slow", "x"), TestContext.Current.CancellationToken);
        await started.Task.WaitAsync(Wait.DeliveryTimeout, TestContext.Current.CancellationToken);

        var unsubscribe = queue.UnsubscribeAsync(TestContext.Current.CancellationToken);
        await Task.Delay(500, TestContext.Current.CancellationToken);
        var watch = Stopwatch.StartNew();
        await queue.PublishAsync(new TestMessage("meanwhile", "x"), TestContext.Current.CancellationToken);
        var publishTook = watch.Elapsed;
        await unsubscribe;

        publishTook.Should().BeLessThan(TimeSpan.FromSeconds(1), "it should not stall publishing while a subscription drains");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_Unsubscribing_From_Inside_A_Handler()
    {
        var config = queues.NewConfig("reentrant-unsubscribe-test") with { ShutdownTimeout = TimeSpan.FromSeconds(10) };
        var queue = queues.Create(config);
        var took = new TaskCompletionSource<TimeSpan>(TaskCreationOptions.RunContinuationsAsynchronously);
        await queue.SubscribeAsync(async (_, _, _) =>
        {
            var watch = Stopwatch.StartNew();
            await queue.UnsubscribeAsync(CancellationToken.None);
            took.TrySetResult(watch.Elapsed);
            return MessageResult.Acknowledge;
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("self", "x"), TestContext.Current.CancellationToken);

        var elapsed = await took.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        await Task.Delay(1000, TestContext.Current.CancellationToken);
        await queue.DisposeAsync();

        elapsed.Should().BeLessThan(TimeSpan.FromSeconds(1), "it should not wait for the very handler that is unsubscribing");
        (await broker.GetMessageCountAsync(config.QueueName)).Should().Be(0u, "it should still settle that handler's message");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_Disposing_From_Inside_A_Handler()
    {
        var config = queues.NewConfig("reentrant-dispose-test") with { ShutdownTimeout = TimeSpan.FromSeconds(10) };
        var queue = queues.Create(config);
        var took = new TaskCompletionSource<TimeSpan>(TaskCreationOptions.RunContinuationsAsynchronously);
        await queue.SubscribeAsync(async (_, _, _) =>
        {
            var watch = Stopwatch.StartNew();
            await queue.DisposeAsync();
            took.TrySetResult(watch.Elapsed);
            return MessageResult.Acknowledge;
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("self", "x"), TestContext.Current.CancellationToken);

        var elapsed = await took.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

        elapsed.Should().BeLessThan(TimeSpan.FromSeconds(1), "it should not wait for the very handler that is disposing the queue");
    }

    [Theory(Timeout = 90_000)]
    [InlineData(true)]
    [InlineData(false)]
    public async Task When_Restarting_With_Prefetched_Messages(bool quorum)
    {
        var config = queues.NewConfig("prefetched-restart-test") with
        {
            UseQuorumQueues = quorum,
            PrefetchCount = 10,
            MaxConcurrentCalls = 1,
            ShutdownTimeout = TimeSpan.FromSeconds(5)
        };
        var queue = queues.Create(config);
        for (var i = 0; i < 10; i++)
        {
            await queue.PublishAsync(new TestMessage($"m-{i}", "x"), TestContext.Current.CancellationToken);
        }

        for (var round = 0; round < 4; round++)
        {
            var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            await queue.SubscribeAsync(async (_, _, ct) =>
            {
                started.TrySetResult();
                await Task.Delay(Timeout.Infinite, ct);
                return MessageResult.Acknowledge;
            }, TestContext.Current.CancellationToken);
            await started.Task.WaitAsync(Wait.DeliveryTimeout, TestContext.Current.CancellationToken);
            await Task.Delay(200, TestContext.Current.CancellationToken);
            await queue.UnsubscribeAsync(TestContext.Current.CancellationToken);
        }
        var attempts = new ConcurrentDictionary<string, int>();
        await queue.SubscribeAsync((message, context, _) =>
        {
            attempts[message.Id] = context.DeliveryAttempt;
            return Task.FromResult(MessageResult.Acknowledge);
        }, TestContext.Current.CancellationToken);
        await Wait.ForCountAsync(() => attempts.Count, 10, Wait.DeliveryTimeout);

        attempts.Should().HaveCount(10, "it should deliver every message after the restarts");
        attempts.Values.Count(a => a > 1).Should().BeLessThanOrEqualTo(4,
            "it should charge an attempt only to messages whose handler was interrupted, not to prefetched ones it never handled");
        attempts.Values.Should().AllSatisfy(a => a.Should().BeLessThanOrEqualTo(2, "it should charge an interruption at most once"));
    }

    [Fact(Timeout = 90_000)]
    public async Task When_Disposing_With_A_Hung_Handler()
    {
        var config = queues.NewConfig("hung-dispose-test") with { ShutdownTimeout = TimeSpan.FromSeconds(1) };
        var queue = queues.Create(config);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await queue.SubscribeAsync(async (_, _, _) =>
        {
            started.TrySetResult();
            await Task.Delay(TimeSpan.FromSeconds(40), CancellationToken.None);
            return MessageResult.Acknowledge;
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("hung", "x"), TestContext.Current.CancellationToken);
        await started.Task.WaitAsync(Wait.DeliveryTimeout, TestContext.Current.CancellationToken);

        var watch = Stopwatch.StartNew();
        await queue.DisposeAsync();

        watch.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(4), "it should give a hung handler ShutdownTimeout (1 s), not wait for it");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_A_Late_Handler_Finishes_After_Unsubscribe()
    {
        var config = queues.NewConfig("late-close-test") with { ShutdownTimeout = TimeSpan.FromSeconds(1) };
        var queue = queues.Create(config);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finished = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await queue.SubscribeAsync(async (_, _, _) =>
        {
            started.TrySetResult();
            await Task.Delay(3000, CancellationToken.None);
            finished.TrySetResult();
            return MessageResult.Acknowledge;
        }, TestContext.Current.CancellationToken);
        var channel = queue.LatestSubscriptionChannel!;
        await queue.PublishAsync(new TestMessage("late", "x"), TestContext.Current.CancellationToken);
        await started.Task.WaitAsync(Wait.DeliveryTimeout, TestContext.Current.CancellationToken);

        await queue.UnsubscribeAsync(TestContext.Current.CancellationToken);
        var openWhileDraining = channel.IsOpen;
        await finished.Task.WaitAsync(Wait.DeliveryTimeout, TestContext.Current.CancellationToken);
        await Wait.UntilAsync(() => !channel.IsOpen, TimeSpan.FromSeconds(5));

        openWhileDraining.Should().BeTrue("it should keep the channel open so the late handler can still settle");
        channel.IsOpen.Should().BeFalse("it should close the subscription's channel once the late handler has finished");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_Disposing_While_A_Late_Handler_Drains()
    {
        var config = queues.NewConfig("dispose-drain-test") with
        {
            ShutdownTimeout = TimeSpan.FromSeconds(5),
            Retry = new RetryPolicy { MaxRetries = 5, InitialDelay = TimeSpan.FromMilliseconds(50), MaxDelay = TimeSpan.FromMilliseconds(50), UseJitter = false }
        };
        var queue = queues.Create(config);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await queue.SubscribeAsync(async (_, _, _) =>
        {
            started.TrySetResult();
            await Task.Delay(7000, CancellationToken.None);
            return MessageResult.Retry;
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("late", "x"), TestContext.Current.CancellationToken);
        await started.Task.WaitAsync(Wait.DeliveryTimeout, TestContext.Current.CancellationToken);

        await queue.UnsubscribeAsync(TestContext.Current.CancellationToken);
        await queue.DisposeAsync();
        await Task.Delay(1000, TestContext.Current.CancellationToken);
        var headers = await broker.TakeMessageHeadersAsync(config.QueueName);

        headers.Should().NotBeNull("it should leave the message on the queue");
        headers.Should().Contain("x-delivery-attempt", "2",
            "it should let the draining handler finish within ShutdownTimeout, so its retry copy replaces the original");
        (await broker.GetMessageCountAsync(config.QueueName)).Should().Be(0u, "it should leave exactly one copy");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_A_Task_Spawned_By_A_Handler_Unsubscribes()
    {
        var config = queues.NewConfig("spawned-unsubscribe-test") with { ShutdownTimeout = TimeSpan.FromSeconds(10), MaxConcurrentCalls = 2, PrefetchCount = 2 };
        var queue = queues.Create(config);
        var slowStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var slowFinished = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var unsubscribed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        await queue.SubscribeAsync(async (message, _, _) =>
        {
            if (message.Id == "slow")
            {
                slowStarted.TrySetResult();
                await Task.Delay(3000, CancellationToken.None);
                slowFinished.TrySetResult();
                return MessageResult.Acknowledge;
            }
            _ = Task.Run(async () =>
            {
                await Task.Delay(500, CancellationToken.None);
                await queue.UnsubscribeAsync(CancellationToken.None);
                unsubscribed.TrySetResult(slowFinished.Task.IsCompleted);
            }, CancellationToken.None);
            return MessageResult.Acknowledge;
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("slow", "x"), TestContext.Current.CancellationToken);
        await slowStarted.Task.WaitAsync(Wait.DeliveryTimeout, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("trigger", "x"), TestContext.Current.CancellationToken);

        var slowDoneFirst = await unsubscribed.Task.WaitAsync(TimeSpan.FromSeconds(20), TestContext.Current.CancellationToken);

        slowDoneFirst.Should().BeTrue("it should treat a task the handler spawned, running after that handler returned, as an ordinary caller that waits for running handlers");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_The_Unsubscribe_Token_Is_Cancelled()
    {
        var config = queues.NewConfig("cancelled-unsubscribe-test") with { ShutdownTimeout = TimeSpan.FromSeconds(10) };
        var queue = queues.Create(config);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await queue.SubscribeAsync(async (_, _, _) =>
        {
            started.TrySetResult();
            await Task.Delay(6000, CancellationToken.None);
            return MessageResult.Acknowledge;
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("slow", "x"), TestContext.Current.CancellationToken);
        await started.Task.WaitAsync(Wait.DeliveryTimeout, TestContext.Current.CancellationToken);
        using var cancel = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
        var stop = () => queue.UnsubscribeAsync(cancel.Token);
        await stop.Should().NotThrowAsync("it should stop waiting when the caller's token is cancelled, with the consumer already cancelled");

        var watch = Stopwatch.StartNew();
        await queue.PublishAsync(new TestMessage("meanwhile", "x"), TestContext.Current.CancellationToken);

        watch.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(1), "it should not stall the connection by closing a channel whose handler still runs");
    }

    // A handler's own cancellation callback throws when shutdown cancels its token: shutdown still completes.
    [Fact(Timeout = 60_000)]
    public async Task When_A_Handler_Cancellation_Callback_Throws_On_Unsubscribe()
    {
        var config = queues.NewConfig("throwing-callback-test");
        var queue = queues.Create(config);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await queue.SubscribeAsync(async (_, _, token) =>
        {
            token.Register(() => throw new InvalidOperationException("handler's cancellation callback"));
            started.TrySetResult();
            await Task.Delay(Timeout.Infinite, token);
            return MessageResult.Acknowledge;
        }, TestContext.Current.CancellationToken);
        var channel = queue.LatestSubscriptionChannel!;
        await queue.PublishAsync(new TestMessage("callback", "x"), TestContext.Current.CancellationToken);
        await started.Task.WaitAsync(Wait.DeliveryTimeout, TestContext.Current.CancellationToken);

        var act = () => queue.UnsubscribeAsync(TestContext.Current.CancellationToken);

        await act.Should().NotThrowAsync("it should not let a handler's callback break the shutdown");
        await Wait.UntilAsync(() => !channel.IsOpen, TimeSpan.FromSeconds(5));
        channel.IsOpen.Should().BeFalse("it should still close the subscription's channel");
        var redelivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await queue.SubscribeAsync((_, _, _) =>
        {
            redelivered.TrySetResult();
            return Task.FromResult(MessageResult.Acknowledge);
        }, TestContext.Current.CancellationToken);
        await redelivered.Task.WaitAsync(Wait.DeliveryTimeout, TestContext.Current.CancellationToken);
    }

    [Fact(Timeout = 60_000)]
    public async Task When_A_Handler_Cancellation_Callback_Throws_On_Dispose()
    {
        var config = queues.NewConfig("throwing-callback-dispose-test");
        var queue = queues.Create(config);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await queue.SubscribeAsync(async (_, _, token) =>
        {
            token.Register(() => throw new InvalidOperationException("handler's cancellation callback"));
            started.TrySetResult();
            await Task.Delay(Timeout.Infinite, token);
            return MessageResult.Acknowledge;
        }, TestContext.Current.CancellationToken);
        var channel = queue.LatestSubscriptionChannel!;
        await queue.PublishAsync(new TestMessage("callback", "x"), TestContext.Current.CancellationToken);
        await started.Task.WaitAsync(Wait.DeliveryTimeout, TestContext.Current.CancellationToken);

        var act = async () => await queue.DisposeAsync();

        await act.Should().NotThrowAsync("it should not let a handler's callback break dispose");
        channel.IsOpen.Should().BeFalse("it should still close the channel and connection");
    }
}
