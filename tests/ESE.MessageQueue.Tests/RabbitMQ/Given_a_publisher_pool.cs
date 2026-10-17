using System.Collections.Concurrent;
using AwesomeAssertions;
using ESE.MessageQueue.Exceptions;
using ESE.MessageQueue.RabbitMQ;
using NSubstitute;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace ESE.MessageQueue.Tests.RabbitMQ;

// RabbitMQ.Client 7 tracks confirms itself: with confirmation tracking on, BasicPublishAsync completes on the broker's
// ack and throws PublishException on a nack or return. The pool bounds that wait and maps the outcomes.
[Trait("Category", "Unit")]
public class Given_a_publisher_pool
{
    static readonly byte[] Body = [1, 2, 3];

    static IChannel Channel(Func<CancellationToken, ValueTask> publish)
    {
        var channel = Substitute.For<IChannel>();
        // Configuring the substitute calls the method; that ValueTask is NSubstitute's, never awaited.
#pragma warning disable CA2012
        channel.BasicPublishAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<BasicProperties>(),
                Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<CancellationToken>())
            .Returns(call => publish(call.ArgAt<CancellationToken>(5)));
#pragma warning restore CA2012
        return channel;
    }

    static PublisherPool Pool(TimeSpan confirmTimeout, params IChannel[] channels) => new(channels, confirms: true, confirmTimeout);

    static Task Publish(PublisherPool pool, CancellationToken cancellationToken) =>
        pool.PublishAsync("exchange", "routing-key", mandatory: false, new BasicProperties(), Body, cancellationToken);

    [Fact]
    public async Task When_The_Broker_Confirms()
    {
        await using var pool = Pool(TimeSpan.FromSeconds(5), Channel(_ => ValueTask.CompletedTask));

        var act = () => Publish(pool, TestContext.Current.CancellationToken);

        await act.Should().NotThrowAsync("it should complete once the broker confirms the message");
    }

    [Fact]
    public async Task When_The_Broker_Nacks()
    {
        await using var pool = Pool(TimeSpan.FromSeconds(5), Channel(_ => ValueTask.FromException(new PublishException(1, isReturn: false))));

        var act = () => Publish(pool, TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<PublishRefusedException>("it should fail the publish when the broker nacks it, as safe to publish again")
            .WithMessage("*nacked*");
    }

    [Fact]
    public async Task When_The_Broker_Returns_The_Message()
    {
        await using var pool = Pool(TimeSpan.FromSeconds(5), Channel(_ => ValueTask.FromException(new PublishException(1, isReturn: true))));

        var act = () => Publish(pool, TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<PublishRefusedException>("it should fail a mandatory publish nobody can receive, as safe to publish again")
            .WithMessage("*returned*");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_No_Confirm_Arrives_In_Time()
    {
        await using var pool = Pool(TimeSpan.FromMilliseconds(100), Channel(token => new ValueTask(Task.Delay(Timeout.Infinite, token))));

        var act = () => Publish(pool, TestContext.Current.CancellationToken);

        (await act.Should().ThrowAsync<MessageQueueException>("it should give up after ConfirmTimeout").WithMessage("*timed out*"))
            .Which.Should().NotBeOfType<PublishRefusedException>("it should not call a timeout a refusal: the broker may have stored the message");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Caller_Cancels()
    {
        await using var pool = Pool(TimeSpan.FromSeconds(30), Channel(token => new ValueTask(Task.Delay(Timeout.Infinite, token))));
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cancel.CancelAfter(TimeSpan.FromMilliseconds(100));

        var act = () => Publish(pool, cancel.Token);

        await act.Should().ThrowAsync<OperationCanceledException>("it should honor the caller's cancellation rather than report a timeout");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Publishing_Concurrently()
    {
        var calls = new ConcurrentDictionary<int, int>();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var channels = Enumerable.Range(0, 3).Select(i => Channel(async token =>
        {
            calls.AddOrUpdate(i, 1, (_, n) => n + 1);
            await release.Task.WaitAsync(token);
        })).ToArray();
        await using var pool = Pool(TimeSpan.FromSeconds(5), channels);

        var publishes = Enumerable.Range(0, 99).Select(_ => Publish(pool, TestContext.Current.CancellationToken)).ToList();
        await Wait.UntilAsync(() => calls.Values.Sum() == 99, TimeSpan.FromSeconds(5));
        release.SetResult();
        var act = () => Task.WhenAll(publishes);

        await act.Should().NotThrowAsync("it should complete every concurrent publish once confirmed");
        calls.Values.Sum().Should().Be(99, "it should have all publishes outstanding at once instead of awaiting each confirm in turn");
        calls.Values.Should().AllSatisfy(n => n.Should().Be(33, "it should spread publishes evenly over the channels"));
    }

    [Fact]
    public async Task When_Confirms_Are_Disabled()
    {
        var seen = CancellationToken.None;
        await using var pool = new PublisherPool([Channel(token =>
        {
            seen = token;
            return ValueTask.CompletedTask;
        })], confirms: false, TimeSpan.FromMilliseconds(1));

        await Publish(pool, TestContext.Current.CancellationToken);

        seen.Should().Be(TestContext.Current.CancellationToken, "it should not put a confirm timeout on a publish that has no confirm");
    }

    [Fact]
    public async Task When_Disposed()
    {
        var channel = Channel(_ => ValueTask.CompletedTask);
        var pool = Pool(TimeSpan.FromSeconds(1), channel);

        await pool.DisposeAsync();

        await channel.Received(1).CloseAsync(Arg.Any<ushort>(), Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
        await channel.Received(1).DisposeAsync();
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Deciding_Whether_A_Failed_Copy_May_Be_Published_Again()
    {
        await using var timingOut = Pool(TimeSpan.FromMilliseconds(50), Channel(token => new ValueTask(Task.Delay(Timeout.Infinite, token))));
        await using var refusing = Pool(TimeSpan.FromSeconds(5), Channel(_ => ValueTask.FromException(new PublishException(1, isReturn: true))));
        var timeout = await Record.ExceptionAsync(() => Publish(timingOut, TestContext.Current.CancellationToken));
        var refusal = await Record.ExceptionAsync(() => Publish(refusing, TestContext.Current.CancellationToken));

        RabbitMqMessageQueue<TestMessage>.IsSafeToRepublish(timeout!).Should()
            .BeFalse("it should not publish a retry or dead-letter copy again after a confirm timeout: the broker may have stored it");
        RabbitMqMessageQueue<TestMessage>.IsSafeToRepublish(refusal!).Should()
            .BeTrue("it should publish again after a nack or return, which prove the copy was not stored");
    }
}
