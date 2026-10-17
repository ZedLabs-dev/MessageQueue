using AwesomeAssertions;
using Azure.Messaging.ServiceBus;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Exceptions;

namespace ESE.MessageQueue.Tests.AzureServiceBus;

// Publishing through one ServiceBusSender (§4.3), bounded by SendTimeout, with no client text in errors.
[Trait("Category", "Unit")]
public class Given_an_azure_service_bus_publisher
{
    readonly FakeServiceBus bus = new();

    [Fact]
    public async Task When_Publishing_A_Message()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue() with { MessageTimeToLive = TimeSpan.FromHours(1) });

        await queue.PublishAsync(new TestMessage("1", "x"), new PublishOptions
        {
            MessageId = "order-1",
            CorrelationId = "checkout-1",
            Headers = new Dictionary<string, string> { ["tenant"] = "acme" }
        }, TestContext.Current.CancellationToken);

        var sent = bus.Sender.Sent.Should().ContainSingle("it should send the message").Which;
        sent.MessageId.Should().Be("order-1", "it should use the publisher's idempotency key");
        sent.CorrelationId.Should().Be("checkout-1", "it should carry the correlation id");
        sent.ContentType.Should().Be("application/json", "it should name the serialization format");
        sent.Subject.Should().Be(nameof(TestMessage), "it should name the message type");
        sent.TimeToLive.Should().Be(TimeSpan.FromHours(1), "it should apply MessageTimeToLive");
        sent.ApplicationProperties.Should().Contain("tenant", "acme", "it should send headers as application properties");
        bus.Sender.EntityPath.Should().Be("orders", "it should publish to the queue");
    }

    [Fact]
    public async Task When_Publishing_Many_Times()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());

        for (var i = 0; i < 5; i++)
            await queue.PublishAsync(new TestMessage($"{i}", "x"), TestContext.Current.CancellationToken);

        bus.Senders.Should().ContainSingle("it should share one thread-safe ServiceBusSender instead of a pool");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Send_Times_Out()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue() with { SendTimeout = TimeSpan.FromMilliseconds(100) });
        bus.Sender.SendDelay = TimeSpan.FromSeconds(30);

        var act = () => queue.PublishAsync(new TestMessage("1", "x"), TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<MessageQueueException>("it should bound a send by SendTimeout").WithMessage("*timed out*");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Caller_Cancels()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        bus.Sender.SendDelay = TimeSpan.FromSeconds(30);
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cancel.CancelAfter(TimeSpan.FromMilliseconds(100));

        var act = () => queue.PublishAsync(new TestMessage("1", "x"), cancel.Token);

        await act.Should().ThrowAsync<OperationCanceledException>("it should honour the caller's cancellation");
    }

    [Fact]
    public async Task When_The_Send_Fails()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        bus.Sender.SendFailure = _ => new ServiceBusException("sb://secret-host.servicebus.windows.net refused", ServiceBusFailureReason.ServiceBusy);

        var act = () => queue.PublishAsync(new TestMessage("1", "x"), TestContext.Current.CancellationToken);

        var error = (await act.Should().ThrowAsync<MessageQueueException>("it should wrap client failures")).Which;
        error.Message.Should().Contain(nameof(ServiceBusException), "it should name the failure type")
            .And.NotContain("secret-host", "it should not echo the client's text, which can name the namespace");
        error.InnerException.Should().BeOfType<ServiceBusException>("it should keep the original for logging");
    }
}
