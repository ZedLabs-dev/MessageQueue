using ESE.MessageQueue.Exceptions;
using ESE.MessageQueue.RabbitMQ;
using FluentAssertions;
using NSubstitute;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ESE.MessageQueue.Tests;

public class PublisherChannelTests
{
    readonly IConnection mockConnection;
    readonly IModel mockChannel;

    public PublisherChannelTests()
    {
        mockConnection = Substitute.For<IConnection>();
        mockChannel = Substitute.For<IModel>();
        mockConnection.CreateModel().Returns(mockChannel);
    }

    [Fact]
    public void When_Creating_With_Confirms_Enabled_Should_Call_ConfirmSelect()
    {
        using var publisherChannel = new PublisherChannel(
            mockConnection,
            enableConfirms: true,
            confirmTimeout: TimeSpan.FromSeconds(5),
            maxOutstandingConfirms: 100);

        mockChannel.Received(1).ConfirmSelect();
    }

    [Fact]
    public void When_Creating_With_Confirms_Disabled_Should_Not_Call_ConfirmSelect()
    {
        using var publisherChannel = new PublisherChannel(
            mockConnection,
            enableConfirms: false,
            confirmTimeout: TimeSpan.FromSeconds(5),
            maxOutstandingConfirms: 100);

        mockChannel.DidNotReceive().ConfirmSelect();
    }

    [Fact]
    public async Task When_Publishing_Without_Confirms_Should_Complete_Immediately()
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
            CancellationToken.None);

        mockChannel.Received(1).BasicPublish(
            exchange: "test-exchange",
            routingKey: "test-routing-key",
            basicProperties: properties,
            body: Arg.Any<ReadOnlyMemory<byte>>());
    }

    [Fact]
    public async Task When_Publishing_With_Confirms_And_Ack_Received_Should_Complete_Successfully()
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

        await publisherChannel.PublishAsync(body, properties, "test-exchange", "test-routing-key", CancellationToken.None);
    }

    [Fact]
    public async Task When_Publishing_With_Confirms_And_Nack_Received_Should_Throw()
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

        var action = () => publisherChannel.PublishAsync(body, properties, "test-exchange", "test-routing-key", CancellationToken.None);

        await action.Should().ThrowAsync<MessageQueueException>().WithMessage("*nacked*");
    }

    [Fact]
    public async Task When_Confirm_Times_Out_Should_Throw()
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

        var action = () => publisherChannel.PublishAsync(body, properties, "test-exchange", "test-routing-key", CancellationToken.None);

        await action.Should().ThrowAsync<MessageQueueException>().WithMessage("*timed out*");
    }

    [Fact]
    public async Task When_Cancellation_Requested_Should_Throw_OperationCanceledException()
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

        await action.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task When_Multiple_Ack_Received_Should_Complete_All_Pending()
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

        var task1 = publisherChannel.PublishAsync(body, properties, "ex", "rk", CancellationToken.None);
        var task2 = publisherChannel.PublishAsync(body, properties, "ex", "rk", CancellationToken.None);
        var task3 = publisherChannel.PublishAsync(body, properties, "ex", "rk", CancellationToken.None);

        await Task.Delay(50);

        ackHandler?.Invoke(mockChannel, new BasicAckEventArgs { DeliveryTag = 3UL, Multiple = true });

        await Task.WhenAll(task1, task2, task3);
    }

    [Fact]
    public void When_Disposed_Should_Dispose_Channel()
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
    public async Task When_Disposed_While_Pending_Should_Fail_Pending_Tasks()
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

        var publishTask = publisherChannel.PublishAsync(body, properties, "test-exchange", "test-routing-key", CancellationToken.None);

        await Task.Delay(50);

        publisherChannel.Dispose();

        var action = () => publishTask;

        await action.Should().ThrowAsync<ObjectDisposedException>();
    }

    [Fact]
    public async Task When_Max_Outstanding_Confirms_Reached_Should_Block_Until_Confirmed()
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

        var task1 = publisherChannel.PublishAsync(body, properties, "ex", "rk", CancellationToken.None);
        var task2 = publisherChannel.PublishAsync(body, properties, "ex", "rk", CancellationToken.None);

        await Task.Delay(50);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        var task3 = publisherChannel.PublishAsync(body, properties, "ex", "rk", cts.Token);

        var action = () => task3;
        await action.Should().ThrowAsync<OperationCanceledException>();

        ackHandler?.Invoke(mockChannel, new BasicAckEventArgs { DeliveryTag = 2UL, Multiple = true });
        await Task.WhenAll(task1, task2);
    }
}
