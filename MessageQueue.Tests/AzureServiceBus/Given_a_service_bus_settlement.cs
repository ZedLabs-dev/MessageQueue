using AwesomeAssertions;
using Azure.Messaging.ServiceBus;
using ESE.MessageQueue.AzureServiceBus;

namespace ESE.MessageQueue.Tests.AzureServiceBus;

// The once-only guard over one received message's settlement actions (the broker refuses a second settlement).
[Trait("Category", "Unit")]
public class Given_a_service_bus_settlement
{
    readonly FakeServiceBusDelivery delivery = new(new FakeServiceBus(), FakeServiceBus.Message("order-1"), CancellationToken.None);

    [Fact]
    public async Task When_Settled_Twice()
    {
        var settlement = new ServiceBusSettlement(delivery, TimeSpan.FromSeconds(5));
        await settlement.SettleAsync((d, token) => d.CompleteAsync(token));

        var act = () => settlement.SettleAsync((d, token) => d.AbandonAsync(null, token));

        await act.Should().NotThrowAsync("it should ignore a second settlement instead of sending it to the broker");
        delivery.Attempts.Should().Equal(["complete"], "it should settle the message exactly once");
    }

    [Fact]
    public async Task When_A_Settlement_Fails()
    {
        var settlement = new ServiceBusSettlement(delivery, TimeSpan.FromSeconds(5));
        delivery.Fail = action => action == "complete" ? new ServiceBusException("busy", ServiceBusFailureReason.ServiceBusy) : null;
        await settlement.Invoking(s => s.SettleAsync((d, token) => d.CompleteAsync(token))).Should().ThrowAsync<ServiceBusException>();

        await settlement.SettleAsync((d, token) => d.AbandonAsync(null, token));

        delivery.Settlement.Should().Be("abandon", "it should let the message be handed back after a failed settlement");
    }

    [Fact]
    public async Task When_The_Lock_Is_Lost()
    {
        var settlement = new ServiceBusSettlement(delivery, TimeSpan.FromSeconds(5));
        delivery.Fail = _ => new ServiceBusException("lock lost", ServiceBusFailureReason.MessageLockLost);
        await settlement.Invoking(s => s.SettleAsync((d, token) => d.CompleteAsync(token))).Should().ThrowAsync<ServiceBusException>();

        await settlement.SettleAsync((d, token) => d.AbandonAsync(null, token));

        delivery.Attempts.Should().Equal(["complete"], "it should not try to settle a message whose lock is gone");
    }
}
