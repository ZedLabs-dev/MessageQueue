using AwesomeAssertions;
using NSubstitute;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using ZedLabs.MessageQueue.RabbitMQ;

namespace ZedLabs.MessageQueue.Tests.RabbitMQ;

// The health check reports a subscription healthy while its consumer is registered. RabbitMQ.Client 7 returns the
// consumer tag from BasicConsumeAsync once the broker's basic.consume-ok arrived, but hands consume-ok to the consumer
// later, on its dispatcher. The channel here returns the tag without ever dispatching consume-ok, the window a health
// probe right after SubscribeAsync can land in. With ConsumerDispatchConcurrency above 1 (MaxConcurrentCalls, 10 by
// default) the client runs several dispatcher workers, so consume-ok, cancel and cancel-ok can be handled in any order.
[Trait("Category", "Unit")]
public sealed class Given_a_rabbitmq_consumer
{
    const string Tag = "ctag-1";

    readonly IChannel channel = Substitute.For<IChannel>();
    readonly RabbitMqConsumer consumer;
    int cancelledByBroker;

    public Given_a_rabbitmq_consumer()
    {
        channel.BasicConsumeAsync("orders", Arg.Is(false), Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<bool>(),
                Arg.Any<IDictionary<string, object?>?>(), Arg.Any<IAsyncBasicConsumer>(), Arg.Any<CancellationToken>())
            .Returns(Tag);
        consumer = new RabbitMqConsumer(channel, () =>
        {
            Interlocked.Increment(ref cancelledByBroker);
            return Task.CompletedTask;
        });
    }

    static ShutdownEventArgs Shutdown() => new(ShutdownInitiator.Peer, 320, "CONNECTION_FORCED");

    [Fact]
    public void When_It_Has_Not_Started()
    {
        consumer.IsConsuming.Should().BeFalse("it should not report a consumer the broker never registered");
    }

    [Fact]
    public async Task When_The_Broker_Returned_Its_Tag_Before_Consume_Ok_Was_Dispatched()
    {
        var tag = await consumer.StartAsync("orders", TestContext.Current.CancellationToken);

        tag.Should().Be(Tag, "it should return the consumer tag the broker assigned");
        consumer.IsConsuming.Should().BeTrue("it should count the consumer as registered once the broker returned its tag");
    }

    [Fact]
    public async Task When_Consume_Ok_Is_Dispatched_After_The_Tag_Returned()
    {
        await consumer.StartAsync("orders", TestContext.Current.CancellationToken);

        await consumer.HandleBasicConsumeOkAsync(Tag, TestContext.Current.CancellationToken);

        consumer.IsConsuming.Should().BeTrue("it should keep consuming when the client catches up with consume-ok");
    }

    [Fact]
    public async Task When_The_Broker_Cancels_It()
    {
        await consumer.StartAsync("orders", TestContext.Current.CancellationToken);
        await consumer.HandleBasicConsumeOkAsync(Tag, TestContext.Current.CancellationToken);

        await consumer.HandleBasicCancelAsync(Tag, TestContext.Current.CancellationToken);

        consumer.IsConsuming.Should().BeFalse("it should not report a consumer the broker cancelled");
        cancelledByBroker.Should().Be(1, "it should report the broker's cancel once");
    }

    [Fact]
    public async Task When_The_Broker_Cancels_It_Before_The_Tag_Returned()
    {
        channel.BasicConsumeAsync("orders", Arg.Is(false), Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<bool>(),
                Arg.Any<IDictionary<string, object?>?>(), Arg.Any<IAsyncBasicConsumer>(), Arg.Any<CancellationToken>())
            .Returns(async _ =>
            {
                await consumer.HandleBasicCancelAsync(Tag, TestContext.Current.CancellationToken);
                return Tag;
            });

        await consumer.StartAsync("orders", TestContext.Current.CancellationToken);

        consumer.IsConsuming.Should().BeFalse("it should not let a returning tag revive a consumer the broker already cancelled");
    }

    [Fact]
    public async Task When_Its_Own_Cancel_Is_Confirmed()
    {
        await consumer.StartAsync("orders", TestContext.Current.CancellationToken);

        await consumer.HandleBasicCancelOkAsync(Tag, TestContext.Current.CancellationToken);

        consumer.IsConsuming.Should().BeFalse("it should stop consuming once the broker confirmed the cancel");
        cancelledByBroker.Should().Be(0, "it should not report its own cancel as the broker's");
    }

    [Fact]
    public async Task When_Its_Channel_Shuts_Down()
    {
        await consumer.StartAsync("orders", TestContext.Current.CancellationToken);

        await consumer.HandleChannelShutdownAsync(channel, Shutdown());

        consumer.IsConsuming.Should().BeFalse("it should not report a consumer whose channel shut down");
    }

    [Fact]
    public async Task When_Recovery_Registers_It_Again()
    {
        await consumer.StartAsync("orders", TestContext.Current.CancellationToken);
        await consumer.HandleChannelShutdownAsync(channel, Shutdown());

        await consumer.HandleBasicConsumeOkAsync("ctag-2", TestContext.Current.CancellationToken);

        consumer.IsConsuming.Should().BeTrue("it should report the consumer that automatic recovery registered again");
    }

    [Fact]
    public async Task When_A_Broker_Cancel_Is_Handled_Before_Consume_Ok()
    {
        await consumer.StartAsync("orders", TestContext.Current.CancellationToken);
        await consumer.HandleBasicCancelAsync(Tag, TestContext.Current.CancellationToken);

        await consumer.HandleBasicConsumeOkAsync(Tag, TestContext.Current.CancellationToken);

        consumer.IsConsuming.Should().BeFalse("it should not let a late consume-ok revive a consumer the broker cancelled");
    }

    [Fact]
    public async Task When_A_Broker_Cancel_And_Consume_Ok_Are_Handled_Before_The_Tag_Returned()
    {
        channel.BasicConsumeAsync("orders", Arg.Is(false), Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<bool>(),
                Arg.Any<IDictionary<string, object?>?>(), Arg.Any<IAsyncBasicConsumer>(), Arg.Any<CancellationToken>())
            .Returns(async _ =>
            {
                await consumer.HandleBasicCancelAsync(Tag, TestContext.Current.CancellationToken);
                await consumer.HandleBasicConsumeOkAsync(Tag, TestContext.Current.CancellationToken);
                return Tag;
            });

        await consumer.StartAsync("orders", TestContext.Current.CancellationToken);

        consumer.IsConsuming.Should().BeFalse("it should not let a late consume-ok or the returning tag revive a cancelled consumer");
    }

    [Fact]
    public async Task When_Its_Own_Cancel_Ok_Is_Handled_Before_Consume_Ok()
    {
        await consumer.StartAsync("orders", TestContext.Current.CancellationToken);
        await consumer.HandleBasicCancelOkAsync(Tag, TestContext.Current.CancellationToken);

        await consumer.HandleBasicConsumeOkAsync(Tag, TestContext.Current.CancellationToken);

        consumer.IsConsuming.Should().BeFalse("it should not let a late consume-ok revive a consumer whose cancel was confirmed");
    }

    // A broker cancel removes the consumer from the client's dispatcher, so it gets no shutdown of its own when the
    // connection is lost; automatic recovery still registers it again. Only the channel's event tells it.
    [Fact]
    public async Task When_Recovery_Registers_It_Again_After_A_Broker_Cancel()
    {
        await consumer.StartAsync("orders", TestContext.Current.CancellationToken);
        await consumer.HandleBasicCancelAsync(Tag, TestContext.Current.CancellationToken);
        channel.ChannelShutdownAsync += Raise.Event<AsyncEventHandler<ShutdownEventArgs>>(channel, Shutdown());

        await consumer.HandleBasicConsumeOkAsync("ctag-2", TestContext.Current.CancellationToken);

        consumer.IsConsuming.Should().BeTrue("it should report the consumer that recovery registered again after its channel shut down");
    }

    [Fact]
    public async Task When_Its_Channel_Reports_A_Shutdown()
    {
        await consumer.StartAsync("orders", TestContext.Current.CancellationToken);

        channel.ChannelShutdownAsync += Raise.Event<AsyncEventHandler<ShutdownEventArgs>>(channel, Shutdown());

        consumer.IsConsuming.Should().BeFalse("it should not report a consumer whose channel shut down");
    }

    [Fact]
    public async Task When_Reporting_The_Broker_Cancel_Throws()
    {
        var throwing = new RabbitMqConsumer(channel, () => throw new InvalidOperationException("telemetry failed"));
        await throwing.StartAsync("orders", TestContext.Current.CancellationToken);

        var cancel = () => throwing.HandleBasicCancelAsync(Tag, TestContext.Current.CancellationToken);

        await cancel.Should().ThrowAsync<InvalidOperationException>("it should not hide the failure from the client");
        throwing.IsConsuming.Should().BeFalse("it should still end a consumer the broker cancelled");
    }

    [Fact]
    public async Task When_Starting_With_A_Token()
    {
        using var source = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);

        await consumer.StartAsync("orders", source.Token);

        await channel.Received(1).BasicConsumeAsync("orders", Arg.Is(false), Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<bool>(),
            Arg.Any<IDictionary<string, object?>?>(), Arg.Is<IAsyncBasicConsumer>(consumer), Arg.Is(source.Token));
    }

    [Fact]
    public async Task When_It_Is_Registered_On_An_Open_Channel()
    {
        channel.IsOpen.Returns(true);

        await consumer.StartAsync("orders", TestContext.Current.CancellationToken);

        consumer.IsHealthy.Should().BeTrue("it should report a registered consumer on an open channel as healthy");
    }

    [Fact]
    public async Task When_Its_Channel_Is_Closed_Before_It_Heard_Of_The_Shutdown()
    {
        channel.IsOpen.Returns(false);

        await consumer.StartAsync("orders", TestContext.Current.CancellationToken);

        consumer.IsHealthy.Should().BeFalse("it should not report a consumer whose channel is closed as healthy");
    }

    [Fact]
    public async Task When_A_Stale_Cancel_Is_Handled_After_Its_Channel_Shut_Down()
    {
        await consumer.StartAsync("orders", TestContext.Current.CancellationToken);
        channel.ChannelShutdownAsync += Raise.Event<AsyncEventHandler<ShutdownEventArgs>>(channel, Shutdown());
        await consumer.HandleBasicCancelOkAsync(Tag, TestContext.Current.CancellationToken);

        await consumer.HandleBasicConsumeOkAsync("ctag-2", TestContext.Current.CancellationToken);

        consumer.IsConsuming.Should().BeTrue("it should let recovery register a consumer whose cancel was handled after its channel shut down");
    }
}
