using AwesomeAssertions;
using ZedLabs.MessageQueue.Exceptions;
using ZedLabs.MessageQueue.RabbitMQ;

namespace ZedLabs.MessageQueue.Tests.RabbitMQ;

// PublishAsync(IEnumerable<T>) starts every publish up to the window and awaits the confirms together (§4.2).
// Driven with a fake publish, so the concurrency is observed directly instead of timed.
[Trait("Category", "Unit")]
public class Given_a_batch_publish
{
    sealed class FakePublish
    {
        readonly TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int started;
        int completed;

        public int Started => Volatile.Read(ref started);
        public int Completed => Volatile.Read(ref completed);
        public Func<int, Exception?> FailWith { get; init; } = _ => null;

        public void Release() => gate.TrySetResult();

        public async Task PublishAsync(int item, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref started);
            try
            {
                await gate.Task;
                if (FailWith(item) is { } failure)
                    throw failure;
            }
            finally
            {
                Interlocked.Increment(ref completed);
            }
        }
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Publishing_A_Batch()
    {
        var fake = new FakePublish();

        var batch = BatchPublisher.PublishAllAsync(Enumerable.Range(0, 20).ToList(), window: 10, fake.PublishAsync,
            TestContext.Current.CancellationToken);
        await Wait.UntilAsync(() => fake.Started == 10, TimeSpan.FromSeconds(5));
        var outstanding = fake.Started;
        fake.Release();
        await batch;

        outstanding.Should().Be(10, "it should have a whole window of publishes awaiting confirms at once, not one at a time");
        fake.Completed.Should().Be(20, "it should publish every message");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Batch_Is_Cancelled_Midway()
    {
        var fake = new FakePublish { FailWith = _ => new ObjectDisposedException("channel") };
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);

        var batch = BatchPublisher.PublishAllAsync(Enumerable.Range(0, 10).ToList(), window: 2, fake.PublishAsync, cancel.Token);
        await Wait.UntilAsync(() => fake.Started == 2, TimeSpan.FromSeconds(5));
        await cancel.CancelAsync();
        await Task.Delay(100, TestContext.Current.CancellationToken);
        var returnedEarly = batch.IsCompleted;
        fake.Release();
        var act = () => batch;

        await act.Should().ThrowAsync<OperationCanceledException>("it should report the caller's cancellation");
        returnedEarly.Should().BeFalse("it should wait for the publishes it already started, so none fails unobserved");
        fake.Completed.Should().Be(fake.Started, "it should have observed every publish it started");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Some_Publishes_Fail()
    {
        var fake = new FakePublish { FailWith = item => item == 3 ? new MessageQueueException("nacked") : null };
        fake.Release();

        var act = () => BatchPublisher.PublishAllAsync(Enumerable.Range(0, 5).ToList(), window: 5, fake.PublishAsync,
            TestContext.Current.CancellationToken);

        (await act.Should().ThrowAsync<MessageQueueException>("it should fail the batch when a message is not confirmed"))
            .WithMessage("1 of 5*").WithInnerException<MessageQueueException>();
    }
}
