using AwesomeAssertions;
using ZedLabs.MessageQueue.Abstractions;
using ZedLabs.MessageQueue.InMemory;
using ZedLabs.MessageQueue.Providers;
using ZedLabs.MessageQueue.Retry;
using ZedLabs.MessageQueue.Serialization;

namespace ZedLabs.MessageQueue.Tests.InMemory;

[Trait("Category", "Unit")]
public sealed class Given_an_in_memory_queue_shutting_down : IAsyncDisposable
{
    public sealed record Order(string Id);

    static readonly TimeSpan Bound = TimeSpan.FromSeconds(5);

    readonly TaskCompletionSource started = new(TaskCreationOptions.RunContinuationsAsynchronously);
    readonly TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
    readonly InMemoryMessageQueue<Order> queue = new(new InMemoryConfig
    {
        QueueName = "orders",
        ShutdownTimeout = TimeSpan.FromMilliseconds(200),
        Retry = new RetryPolicy { MaxRetries = 2, InitialDelay = TimeSpan.FromMinutes(1), MaxDelay = TimeSpan.FromMinutes(1), UseJitter = false }
    });

    public async ValueTask DisposeAsync()
    {
        release.TrySetResult();
        await queue.DisposeAsync();
    }

    async Task SubscribeIgnoringCancellation()
    {
        await queue.SubscribeAsync(async (_, _, _) =>
        {
            started.TrySetResult();
            await release.Task;
            return MessageResult.Acknowledge;
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new Order("1"), TestContext.Current.CancellationToken);
        await started.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Waiting_For_Idle_After_Dispose_With_A_Retry_Pending()
    {
        var rescheduled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await queue.SubscribeAsync((_, _, _) =>
        {
            rescheduled.TrySetResult();
            return Task.FromResult(MessageResult.Retry);
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new Order("1"), TestContext.Current.CancellationToken);
        await rescheduled.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);
        await queue.UnsubscribeAsync(TestContext.Current.CancellationToken);

        await queue.DisposeAsync();
        var act = () => queue.WaitForIdleAsync(TestContext.Current.CancellationToken).WaitAsync(Bound, TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<ObjectDisposedException>(
            "it should end the wait when the queue is disposed with a retry still scheduled, instead of hanging");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Waiting_For_Idle_Before_Dispose()
    {
        await queue.PublishAsync(new Order("1"), TestContext.Current.CancellationToken);
        var idle = queue.WaitForIdleAsync(TestContext.Current.CancellationToken);

        await queue.DisposeAsync();
        var act = () => idle.WaitAsync(Bound, TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<ObjectDisposedException>("it should release waiters when the queue is disposed");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Disposing_While_A_Handler_Ignores_Cancellation()
    {
        await SubscribeIgnoringCancellation();

        var act = () => queue.DisposeAsync().AsTask().WaitAsync(Bound, TestContext.Current.CancellationToken);

        await act.Should().NotThrowAsync("it should stop waiting for a stuck handler after ShutdownTimeout");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Unsubscribing_While_A_Handler_Ignores_Cancellation()
    {
        await SubscribeIgnoringCancellation();

        var act = () => queue.UnsubscribeAsync(TestContext.Current.CancellationToken).WaitAsync(Bound, TestContext.Current.CancellationToken);

        await act.Should().NotThrowAsync("it should stop waiting for a stuck handler after ShutdownTimeout");
        queue.IsSubscribed.Should().BeFalse("it should still end the subscription");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Unsubscribing_While_A_Handler_Finishes_Its_Work()
    {
        var finished = false;
        await queue.SubscribeAsync(async (_, _, _) =>
        {
            started.TrySetResult();
            await Task.Delay(50, CancellationToken.None);
            finished = true;
            return MessageResult.Acknowledge;
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new Order("1"), TestContext.Current.CancellationToken);
        await started.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);

        await queue.UnsubscribeAsync(TestContext.Current.CancellationToken);

        finished.Should().BeTrue("it should wait for in-flight handlers before returning");
        queue.ProcessedMessages.Should().ContainSingle("it should let the in-flight handler settle its message");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Unsubscribing_Cancels_A_Running_Handler()
    {
        await queue.SubscribeAsync(async (_, _, ct) =>
        {
            started.TrySetResult();
            await Task.Delay(Timeout.Infinite, ct);
            return MessageResult.Acknowledge;
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new Order("1"), TestContext.Current.CancellationToken);
        await started.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);

        await queue.UnsubscribeAsync(TestContext.Current.CancellationToken);

        queue.PendingCount.Should().Be(1, "it should keep the interrupted message");
        var redelivered = new TaskCompletionSource<MessageContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        await queue.SubscribeAsync((_, context, _) =>
        {
            redelivered.TrySetResult(context);
            return Task.FromResult(MessageResult.Acknowledge);
        }, TestContext.Current.CancellationToken);
        var context = await redelivered.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);
        context.DeliveryAttempt.Should().Be(1, "it should put the interrupted message back unchanged, like an unacked broker delivery");
    }

    [Fact]
    public async Task When_A_Delivery_Is_Rescheduled()
    {
        var envelope = new InMemoryMessageQueue<Order>.Envelope(
            MessageSerializer.Serialize(new Order("1"), SerializationFormat.Json), new MessageContext { MessageId = "m-1" });
        var actions = new InMemoryMessageQueue<Order>.DeliveryActions(queue, envelope);

        await actions.RescheduleAsync(TimeSpan.FromMinutes(1), 2, TestContext.Current.CancellationToken);

        actions.Handled.Should().BeTrue("it should mark a rescheduled delivery as handled, so a later failure cannot settle it again");
    }
}
