using AwesomeAssertions;
using ESE.MessageQueue.Exceptions;
using ESE.MessageQueue.RabbitMQ;
using NSubstitute;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ESE.MessageQueue.Tests.RabbitMQ;

[Trait("Category", "Unit")]
public class Given_a_publisher_channel
{
    readonly IConnection mockConnection;
    readonly IModel mockChannel;

    public Given_a_publisher_channel()
    {
        mockConnection = Substitute.For<IConnection>();
        mockChannel = Substitute.For<IModel>();
        mockConnection.CreateModel().Returns(mockChannel);
    }

    [Fact]
    public void When_Creating_With_Confirms_Enabled()
    {
        using var publisherChannel = new PublisherChannel(
            mockConnection,
            enableConfirms: true,
            confirmTimeout: TimeSpan.FromSeconds(5),
            maxOutstandingConfirms: 100);

        mockChannel.Received(1).ConfirmSelect();
    }

    [Fact]
    public void When_Creating_With_Confirms_Disabled()
    {
        using var publisherChannel = new PublisherChannel(
            mockConnection,
            enableConfirms: false,
            confirmTimeout: TimeSpan.FromSeconds(5),
            maxOutstandingConfirms: 100);

        mockChannel.DidNotReceive().ConfirmSelect();
    }

    [Fact]
    public async Task When_Publishing_Without_Confirms()
    {
        using var publisherChannel = new PublisherChannel(
            mockConnection,
            enableConfirms: false,
            confirmTimeout: TimeSpan.FromSeconds(5),
            maxOutstandingConfirms: 100);

        var properties = Substitute.For<IBasicProperties>();
        mockChannel.CreateBasicProperties().Returns(properties);

        var body = new byte[] { 1, 2, 3 };

        await publisherChannel.PublishAsync(
            body,
            properties,
            "test-exchange",
            "test-routing-key",
            TestContext.Current.CancellationToken);

        mockChannel.Received(1).BasicPublish(
            exchange: "test-exchange",
            routingKey: "test-routing-key",
            basicProperties: properties,
            body: Arg.Any<ReadOnlyMemory<byte>>());
    }

    [Fact]
    public async Task When_Publishing_With_Confirms_And_Ack_Received()
    {
        mockChannel.NextPublishSeqNo.Returns(1UL);

        EventHandler<BasicAckEventArgs>? ackHandler = null;
        mockChannel.When(x => x.BasicAcks += Arg.Any<EventHandler<BasicAckEventArgs>>())
            .Do(x => ackHandler = x.Arg<EventHandler<BasicAckEventArgs>>());

        using var publisherChannel = new PublisherChannel(
            mockConnection,
            enableConfirms: true,
            confirmTimeout: TimeSpan.FromSeconds(5),
            maxOutstandingConfirms: 100);

        var properties = Substitute.For<IBasicProperties>();
        mockChannel.CreateBasicProperties().Returns(properties);

        mockChannel.When(x => x.BasicPublish(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<bool>(),
                Arg.Any<IBasicProperties>(),
                Arg.Any<ReadOnlyMemory<byte>>()))
            .Do(_ => Task.Run(() =>
            {
                Thread.Sleep(10);
                ackHandler?.Invoke(mockChannel, new BasicAckEventArgs { DeliveryTag = 1UL, Multiple = false });
            }));

        var body = new byte[] { 1, 2, 3 };

        var action = () => publisherChannel.PublishAsync(body, properties, "test-exchange", "test-routing-key", TestContext.Current.CancellationToken);

        await action.Should().NotThrowAsync("it should complete the publish once the broker acks it");
    }

    [Fact]
    public async Task When_Publishing_With_Confirms_And_Nack_Received()
    {
        mockChannel.NextPublishSeqNo.Returns(1UL);

        EventHandler<BasicNackEventArgs>? nackHandler = null;
        mockChannel.When(x => x.BasicNacks += Arg.Any<EventHandler<BasicNackEventArgs>>())
            .Do(x => nackHandler = x.Arg<EventHandler<BasicNackEventArgs>>());

        using var publisherChannel = new PublisherChannel(
            mockConnection,
            enableConfirms: true,
            confirmTimeout: TimeSpan.FromSeconds(5),
            maxOutstandingConfirms: 100);

        var properties = Substitute.For<IBasicProperties>();
        mockChannel.CreateBasicProperties().Returns(properties);

        mockChannel.When(x => x.BasicPublish(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<bool>(),
                Arg.Any<IBasicProperties>(),
                Arg.Any<ReadOnlyMemory<byte>>()))
            .Do(_ => Task.Run(() =>
            {
                Thread.Sleep(10);
                nackHandler?.Invoke(mockChannel, new BasicNackEventArgs { DeliveryTag = 1UL, Multiple = false, Requeue = false });
            }));

        var body = new byte[] { 1, 2, 3 };

        var action = () => publisherChannel.PublishAsync(body, properties, "test-exchange", "test-routing-key", TestContext.Current.CancellationToken);

        await action.Should().ThrowAsync<MessageQueueException>("it should fail the publish when the broker nacks it").WithMessage("*nacked*");
    }

    [Fact]
    public async Task When_Confirm_Times_Out()
    {
        mockChannel.NextPublishSeqNo.Returns(1UL);

        using var publisherChannel = new PublisherChannel(
            mockConnection,
            enableConfirms: true,
            confirmTimeout: TimeSpan.FromMilliseconds(100),
            maxOutstandingConfirms: 100);

        var properties = Substitute.For<IBasicProperties>();
        mockChannel.CreateBasicProperties().Returns(properties);

        var body = new byte[] { 1, 2, 3 };

        var action = () => publisherChannel.PublishAsync(body, properties, "test-exchange", "test-routing-key", TestContext.Current.CancellationToken);

        await action.Should().ThrowAsync<MessageQueueException>("it should fail the publish when no confirm arrives in time").WithMessage("*timed out*");
    }

    [Fact]
    public async Task When_Cancellation_Is_Requested()
    {
        mockChannel.NextPublishSeqNo.Returns(1UL);

        using var publisherChannel = new PublisherChannel(
            mockConnection,
            enableConfirms: true,
            confirmTimeout: TimeSpan.FromSeconds(30),
            maxOutstandingConfirms: 100);

        var properties = Substitute.For<IBasicProperties>();
        mockChannel.CreateBasicProperties().Returns(properties);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var body = new byte[] { 1, 2, 3 };

        var action = () => publisherChannel.PublishAsync(body, properties, "test-exchange", "test-routing-key", cts.Token);

        await action.Should().ThrowAsync<OperationCanceledException>("it should honor the caller's cancellation");
    }

    [Fact]
    public async Task When_Multiple_Ack_Received()
    {
        var seqNo = 0UL;
        mockChannel.NextPublishSeqNo.Returns(_ => ++seqNo);

        EventHandler<BasicAckEventArgs>? ackHandler = null;
        mockChannel.When(x => x.BasicAcks += Arg.Any<EventHandler<BasicAckEventArgs>>())
            .Do(x => ackHandler = x.Arg<EventHandler<BasicAckEventArgs>>());

        using var publisherChannel = new PublisherChannel(
            mockConnection,
            enableConfirms: true,
            confirmTimeout: TimeSpan.FromSeconds(5),
            maxOutstandingConfirms: 100);

        var properties = Substitute.For<IBasicProperties>();
        mockChannel.CreateBasicProperties().Returns(properties);

        var body = new byte[] { 1, 2, 3 };

        var task1 = publisherChannel.PublishAsync(body, properties, "ex", "rk", TestContext.Current.CancellationToken);
        var task2 = publisherChannel.PublishAsync(body, properties, "ex", "rk", TestContext.Current.CancellationToken);
        var task3 = publisherChannel.PublishAsync(body, properties, "ex", "rk", TestContext.Current.CancellationToken);

        await Task.Delay(50, TestContext.Current.CancellationToken);

        ackHandler?.Invoke(mockChannel, new BasicAckEventArgs { DeliveryTag = 3UL, Multiple = true });

        var action = () => Task.WhenAll(task1, task2, task3);
        await action.Should().NotThrowAsync("it should complete every publish covered by a multiple ack");
    }

    [Fact]
    public void When_Disposed()
    {
        var publisherChannel = new PublisherChannel(
            mockConnection,
            enableConfirms: false,
            confirmTimeout: TimeSpan.FromSeconds(5),
            maxOutstandingConfirms: 100);

        publisherChannel.Dispose();

        mockChannel.Received(1).Dispose();
    }

    [Fact]
    public async Task When_Disposed_While_Confirms_Are_Pending()
    {
        mockChannel.NextPublishSeqNo.Returns(1UL);

        var publisherChannel = new PublisherChannel(
            mockConnection,
            enableConfirms: true,
            confirmTimeout: TimeSpan.FromSeconds(30),
            maxOutstandingConfirms: 100);

        var properties = Substitute.For<IBasicProperties>();
        mockChannel.CreateBasicProperties().Returns(properties);

        var body = new byte[] { 1, 2, 3 };

        var publishTask = publisherChannel.PublishAsync(body, properties, "test-exchange", "test-routing-key", TestContext.Current.CancellationToken);

        await Task.Delay(50, TestContext.Current.CancellationToken);

        publisherChannel.Dispose();

        var action = () => publishTask;

        await action.Should().ThrowAsync<ObjectDisposedException>("it should fail publishes still waiting for a confirm");
    }

    [Fact]
    public async Task When_Max_Outstanding_Confirms_Reached()
    {
        var seqNo = 0UL;
        mockChannel.NextPublishSeqNo.Returns(_ => ++seqNo);

        EventHandler<BasicAckEventArgs>? ackHandler = null;
        mockChannel.When(x => x.BasicAcks += Arg.Any<EventHandler<BasicAckEventArgs>>())
            .Do(x => ackHandler = x.Arg<EventHandler<BasicAckEventArgs>>());

        using var publisherChannel = new PublisherChannel(
            mockConnection,
            enableConfirms: true,
            confirmTimeout: TimeSpan.FromSeconds(5),
            maxOutstandingConfirms: 2);

        var properties = Substitute.For<IBasicProperties>();
        mockChannel.CreateBasicProperties().Returns(properties);

        var body = new byte[] { 1, 2, 3 };

        var task1 = publisherChannel.PublishAsync(body, properties, "ex", "rk", TestContext.Current.CancellationToken);
        var task2 = publisherChannel.PublishAsync(body, properties, "ex", "rk", TestContext.Current.CancellationToken);

        await Task.Delay(50, TestContext.Current.CancellationToken);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        var task3 = publisherChannel.PublishAsync(body, properties, "ex", "rk", cts.Token);

        var action = () => task3;
        await action.Should().ThrowAsync<OperationCanceledException>("it should block a publish beyond the outstanding-confirm limit");

        ackHandler?.Invoke(mockChannel, new BasicAckEventArgs { DeliveryTag = 2UL, Multiple = true });
        var confirmed = () => Task.WhenAll(task1, task2);
        await confirmed.Should().NotThrowAsync("it should complete the publishes inside the limit once acked");
    }
}
