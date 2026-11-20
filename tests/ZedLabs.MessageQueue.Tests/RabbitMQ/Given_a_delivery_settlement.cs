using AwesomeAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using RabbitMQ.Client;
using ZedLabs.MessageQueue.RabbitMQ;

namespace ZedLabs.MessageQueue.Tests.RabbitMQ;

// A delivery tag may be settled once: a second ack or nack makes the broker close the whole channel (406), taking
// every other in-flight delivery with it.
[Trait("Category", "Unit")]
public class Given_a_delivery_settlement
{
    readonly IChannel channel = Substitute.For<IChannel>();

    Settlement Settlement() => new(channel, deliveryTag: 7, NullLogger.Instance, "orders");

    [Fact]
    public async Task When_Acknowledging_Twice()
    {
        var settlement = Settlement();

        await settlement.AckAsync();
        await settlement.AckAsync();

        await channel.Received(1).BasicAckAsync(7, false, Arg.Any<CancellationToken>());
        settlement.IsSettled.Should().BeTrue("it should remember that the delivery is settled");
    }

    [Fact]
    public async Task When_Acknowledging_A_Requeued_Delivery()
    {
        var settlement = Settlement();

        await settlement.RequeueAsync();
        await settlement.AckAsync();
        await settlement.RequeueAsync();

        await channel.Received(1).BasicNackAsync(7, false, true, Arg.Any<CancellationToken>());
        await channel.DidNotReceive().BasicAckAsync(Arg.Any<ulong>(), Arg.Any<bool>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task When_Requeueing_On_A_Closed_Channel()
    {
        // Configuring the substitute calls the method; that ValueTask is NSubstitute's, never awaited.
#pragma warning disable CA2012
        channel.BasicNackAsync(Arg.Any<ulong>(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(_ => ValueTask.FromException(new InvalidOperationException("closed")));
#pragma warning restore CA2012
        var settlement = Settlement();

        var act = () => settlement.RequeueAsync();

        await act.Should().NotThrowAsync("it should leave a delivery on a closed channel to the broker, which requeues it");
    }
}
