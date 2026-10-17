using System.Diagnostics;
using AwesomeAssertions;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.AzureServiceBus;
using ESE.MessageQueue.Providers;
using Microsoft.Extensions.Logging;

namespace ESE.MessageQueue.Tests.AzureServiceBus;

// B7: idempotent, bounded shutdown; cancelled handlers abandon their message; re-entrant calls do not deadlock.
[Trait("Category", "Unit")]
public class Given_an_azure_service_bus_queue_shutting_down : IAsyncDisposable
{
    readonly FakeServiceBus bus = new();

    // The fake stands in for the SDK client; disposing it with the test class keeps CA1001 honest.
    public ValueTask DisposeAsync() => bus.DisposeAsync();

    [Fact]
    public async Task When_Disposed_Twice()
    {
        var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        await queue.DisposeAsync();

        var act = async () => await queue.DisposeAsync();

        await act.Should().NotThrowAsync("it should make dispose idempotent, because DI disposes every alias registration");
        bus.DisposeCount.Should().Be(1, "it should close the client only once");
    }

    [Fact]
    public async Task When_Disposed_Asynchronously_And_Then_Synchronously()
    {
        var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        await queue.DisposeAsync();

        queue.Dispose();

        bus.DisposeCount.Should().Be(1, "it should not repeat the shutdown when a container disposes both ways");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Disposed()
    {
        var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        await queue.SubscribeAsync(AzureServiceBusTestConfigs.Returns(MessageResult.Acknowledge), TestContext.Current.CancellationToken);

        await queue.DisposeAsync();

        bus.Processor.Disposed.Should().BeTrue("it should dispose the processor");
        bus.Sender.Disposed.Should().BeTrue("it should dispose the sender");
        bus.Disposed.Should().BeTrue("it should dispose the client");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Disposed_Synchronously()
    {
        var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        await queue.SubscribeAsync(AzureServiceBusTestConfigs.Returns(MessageResult.Acknowledge), TestContext.Current.CancellationToken);

        queue.Dispose();

        bus.Disposed.Should().BeTrue("it should release the client for callers that only know IDisposable");
    }

    [Fact]
    public async Task When_Publishing_After_Dispose()
    {
        var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        await queue.DisposeAsync();

        var act = () => queue.PublishAsync(new TestMessage("1", "x"), TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<ObjectDisposedException>("it should refuse to publish once disposed");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Unsubscribing_With_A_Handler_Running()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await queue.SubscribeAsync(async (_, _, token) =>
        {
            started.TrySetResult();
            await Task.Delay(Timeout.Infinite, token);
            return MessageResult.Acknowledge;
        }, TestContext.Current.CancellationToken);
        var delivery = bus.Processor.Deliver(FakeServiceBus.Message("order-1", deliveryCount: 1));
        await started.Task;

        await queue.UnsubscribeAsync(TestContext.Current.CancellationToken);

        delivery.Handled.IsCompleted.Should().BeTrue("it should wait for the in-flight handler");
        delivery.Settlement.Should().Be("abandon", "it should abandon a message whose handler was cancelled by shutdown");
        delivery.DeadLetterReason.Should().BeNull("it should not dead-letter because of a shutdown");
        delivery.Properties.Should().Contain(ServiceBusMessages.AttemptProperty, 1, "it should not use up an attempt for a shutdown");
        delivery.Properties.Should().Contain(ServiceBusMessages.DeliveryCountProperty, 2, "it should account for the broker's redelivery");
        bus.Processor.Disposed.Should().BeTrue("it should dispose the processor");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_A_Handler_Ignores_Its_Token()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue() with { ShutdownTimeout = TimeSpan.FromMilliseconds(300) });
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await queue.SubscribeAsync(async (_, _, _) =>
        {
            started.TrySetResult();
            await release.Task;
            return MessageResult.Acknowledge;
        }, TestContext.Current.CancellationToken);
        var delivery = bus.Processor.Deliver(FakeServiceBus.Message("order-1"));
        await started.Task;
        var watch = Stopwatch.StartNew();

        await queue.UnsubscribeAsync(TestContext.Current.CancellationToken);
        var elapsed = watch.Elapsed;
        release.SetResult();
        await delivery.Handled.WaitAsync(TestContext.Current.CancellationToken);
        await Wait.UntilAsync(() => bus.Processor.Disposed, Wait.DeliveryTimeout);

        elapsed.Should().BeLessThan(TimeSpan.FromSeconds(2), "it should return after ShutdownTimeout");
        delivery.Attempts.Should().Equal(["complete"], "it should let the late handler settle its message normally, once");
        bus.Processor.Disposed.Should().BeTrue("it should dispose the processor once the handler finished");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Disposing_With_A_Handler_That_Ignores_Its_Token()
    {
        var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue() with { ShutdownTimeout = TimeSpan.FromMilliseconds(300) });
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await queue.SubscribeAsync(async (_, _, _) =>
        {
            started.TrySetResult();
            await release.Task;
            return MessageResult.Acknowledge;
        }, TestContext.Current.CancellationToken);
        bus.Processor.Deliver(FakeServiceBus.Message("order-1"));
        await started.Task;
        var watch = Stopwatch.StartNew();

        await queue.DisposeAsync();
        var elapsed = watch.Elapsed;
        release.SetResult();

        elapsed.Should().BeLessThan(TimeSpan.FromSeconds(3), "it should bound dispose by ShutdownTimeout");
        bus.Disposed.Should().BeTrue("it should close the client even though a handler is still running");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Unsubscribing_From_Inside_A_Handler()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue() with { ShutdownTimeout = TimeSpan.FromSeconds(30) });
        var unsubscribeTook = TimeSpan.MaxValue;
        await queue.SubscribeAsync(async (_, _, _) =>
        {
            var watch = Stopwatch.StartNew();
            await queue.UnsubscribeAsync(CancellationToken.None);
            unsubscribeTook = watch.Elapsed;
            return MessageResult.Acknowledge;
        }, TestContext.Current.CancellationToken);

        var delivery = bus.Processor.Deliver(FakeServiceBus.Message("order-1"));
        await delivery.Handled.WaitAsync(TestContext.Current.CancellationToken);
        await Wait.UntilAsync(() => bus.Processor.Disposed, Wait.DeliveryTimeout);

        unsubscribeTook.Should().BeLessThan(TimeSpan.FromSeconds(2), "it should not wait for the very handler that called it");
        delivery.Settlement.Should().Be("complete", "it should finish the handler that unsubscribed without deadlocking");
        bus.Processor.Disposed.Should().BeTrue("it should finish the shutdown in the background");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_A_Task_Spawned_By_A_Handler_Unsubscribes_After_The_Handler_Returned()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        var firstReturned = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task? unsubscribing = null;
        await queue.SubscribeAsync(async (message, _, token) =>
        {
            if (message.Id == "first")
            {
                // Inherits the handler's async flow, but runs its unsubscribe only after the handler has returned.
                unsubscribing = Task.Run(async () =>
                {
                    await firstReturned.Task;
                    await queue.UnsubscribeAsync(CancellationToken.None);
                }, CancellationToken.None);
                return MessageResult.Acknowledge;
            }

            secondStarted.TrySetResult();
            try
            {
                await Task.Delay(Timeout.Infinite, token);
            }
            catch (OperationCanceledException)
            {
                await Task.Delay(300, CancellationToken.None);
            }
            return MessageResult.Acknowledge;
        }, TestContext.Current.CancellationToken);
        var second = bus.Processor.Deliver(FakeServiceBus.Message("second", new TestMessage("second", "x")));
        await secondStarted.Task.WaitAsync(TestContext.Current.CancellationToken);
        var first = bus.Processor.Deliver(FakeServiceBus.Message("first", new TestMessage("first", "x")));
        await first.Handled.WaitAsync(TestContext.Current.CancellationToken);

        firstReturned.SetResult();
        await Wait.UntilAsync(() => unsubscribing is not null, Wait.DeliveryTimeout);
        await unsubscribing!.WaitAsync(TestContext.Current.CancellationToken);

        second.Handled.IsCompleted.Should().BeTrue(
            "it should treat a spawned task as the handler only while the handler runs, so a later unsubscribe waits for in-flight handlers");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Disposing_From_Inside_A_Handler()
    {
        var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        await queue.SubscribeAsync(async (_, _, _) =>
        {
            await queue.DisposeAsync();
            return MessageResult.Acknowledge;
        }, TestContext.Current.CancellationToken);

        var delivery = bus.Processor.Deliver(FakeServiceBus.Message("order-1"));
        await delivery.Handled.WaitAsync(TestContext.Current.CancellationToken);
        await Wait.UntilAsync(() => bus.Disposed, Wait.DeliveryTimeout);

        delivery.Settlement.Should().Be("complete", "it should finish the handler that disposed the queue");
        bus.Disposed.Should().BeTrue("it should finish disposing in the background");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_A_Message_Arrives_After_Shutdown_Began()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handled = 0;
        await queue.SubscribeAsync(async (_, _, _) =>
        {
            Interlocked.Increment(ref handled);
            started.TrySetResult();
            await release.Task;
            return MessageResult.Acknowledge;
        }, TestContext.Current.CancellationToken);
        var processor = bus.Processor;
        processor.Deliver(FakeServiceBus.Message("first"));
        await started.Task;

        var unsubscribing = queue.UnsubscribeAsync(TestContext.Current.CancellationToken);
        var late = processor.Deliver(FakeServiceBus.Message("late", deliveryCount: 1));
        await late.Handled.WaitAsync(TestContext.Current.CancellationToken);
        release.SetResult();
        await unsubscribing;

        Volatile.Read(ref handled).Should().Be(1, "it should not start new handlers once shutdown began");
        late.Settlement.Should().Be("abandon", "it should hand the late message back");
        late.Properties.Should().Contain(ServiceBusMessages.AttemptProperty, 1, "it should not use up an attempt for a message it never handled");
    }

    async Task<AzureServiceBusMessageQueue<TestMessage>> WithThrowingCancellationCallbackAsync(RecordingLogger logger)
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue(), logger);
        await queue.SubscribeAsync(async (_, _, token) =>
        {
            token.Register(() => throw new InvalidOperationException("handler's cancellation callback"));
            started.TrySetResult();
            await Task.Delay(Timeout.Infinite, token);
            return MessageResult.Acknowledge;
        }, TestContext.Current.CancellationToken);
        bus.Processor.Deliver(FakeServiceBus.Message("order-1"));
        await started.Task.WaitAsync(TestContext.Current.CancellationToken);
        return queue;
    }

    // P2: a handler's own cancellation callback throws when shutdown cancels its token.
    [Fact(Timeout = 20_000)]
    public async Task When_A_Handler_Cancellation_Callback_Throws_On_Unsubscribe()
    {
        var logger = new RecordingLogger();
        var queue = await WithThrowingCancellationCallbackAsync(logger);

        var act = () => queue.UnsubscribeAsync(TestContext.Current.CancellationToken);

        await act.Should().NotThrowAsync("it should not let a handler's callback break the shutdown");
        await Wait.UntilAsync(() => bus.Processor.Disposed, Wait.DeliveryTimeout);
        bus.Processor.StopCalls.Should().BeGreaterThan(0, "it should still stop the processor");
        bus.Processor.Disposed.Should().BeTrue("it should still dispose the processor");
        logger.Entries.Should().Contain(e => e.Level == LogLevel.Warning && e.Message.Contains("cancellation"), "it should log the failing callback");
        await queue.DisposeAsync();
    }

    // P2b: the same through DisposeAsync.
    [Fact(Timeout = 20_000)]
    public async Task When_A_Handler_Cancellation_Callback_Throws_On_Dispose()
    {
        var queue = await WithThrowingCancellationCallbackAsync(new RecordingLogger());

        var act = async () => await queue.DisposeAsync().AsTask().WaitAsync(TestContext.Current.CancellationToken);

        await act.Should().NotThrowAsync("it should not let a handler's callback break dispose");
        bus.Processor.StopCalls.Should().BeGreaterThan(0, "it should still stop the processor");
        bus.Sender.Disposed.Should().BeTrue("it should still dispose the sender");
        bus.Disposed.Should().BeTrue("it should still close the client");
    }

    // A subscribe that waited for the subscription lock while the queue was being disposed must not start a processor.
    [Fact(Timeout = 20_000)]
    public async Task When_Subscribing_While_The_Queue_Is_Being_Disposed()
    {
        var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue() with { ShutdownTimeout = TimeSpan.FromSeconds(1) });
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await queue.SubscribeAsync(async (_, _, _) =>
        {
            started.TrySetResult();
            await release.Task;
            return MessageResult.Acknowledge;
        }, TestContext.Current.CancellationToken);
        bus.Processor.Deliver(FakeServiceBus.Message("order-1"));
        await started.Task.WaitAsync(TestContext.Current.CancellationToken);
        var unsubscribing = queue.UnsubscribeAsync(TestContext.Current.CancellationToken);
        var subscribing = queue.SubscribeAsync(AzureServiceBusTestConfigs.Returns(MessageResult.Acknowledge), TestContext.Current.CancellationToken);
        await Task.Delay(100, TestContext.Current.CancellationToken);

        var disposing = queue.DisposeAsync().AsTask();
        await unsubscribing;
        var act = () => subscribing;

        await act.Should().ThrowAsync<ObjectDisposedException>("it should not start a subscription once the queue is being disposed");
        release.SetResult();
        await disposing.WaitAsync(TestContext.Current.CancellationToken);
        bus.Processors.Should().ContainSingle("it should never create a processor for the late subscribe");
    }
}
