using AwesomeAssertions;
using Azure.Messaging.ServiceBus;
using NSubstitute;
using ZedLabs.MessageQueue.AzureServiceBus;

namespace ZedLabs.MessageQueue.Tests.AzureServiceBus;

// The SDK-backed seam implementations forward to the right SDK calls. The client connects lazily, so creating
// processors, senders and receivers needs no namespace; settlement goes through a substituted ServiceBusReceiver.
[Trait("Category", "Unit")]
public sealed class Given_a_service_bus_transport : IAsyncDisposable
{
    readonly ServiceBusClient client = new(AzureServiceBusTestConfigs.ConnectionString);
    readonly ServiceBusTransport transport;

    public Given_a_service_bus_transport()
    {
        transport = new ServiceBusTransport(client);
    }

    public async ValueTask DisposeAsync() => await client.DisposeAsync();

    [Fact]
    public async Task When_Creating_A_Processor_For_A_Subscription()
    {
        await using var processor = (ServiceBusTransport.Processor)transport.CreateProcessor("events", "billing", new ServiceBusProcessorOptions());

        processor.Inner.EntityPath.Should().Be("events/Subscriptions/billing", "it should receive from the subscription, not the topic");
    }

    [Fact]
    public async Task When_Creating_A_Processor_For_A_Queue()
    {
        await using var processor = (ServiceBusTransport.Processor)transport.CreateProcessor("orders", null, new ServiceBusProcessorOptions());

        processor.Inner.EntityPath.Should().Be("orders", "it should receive from the queue");
    }

    [Fact]
    public async Task When_A_Processor_Has_Not_Started()
    {
        await using var processor = transport.CreateProcessor("orders", null, new ServiceBusProcessorOptions());

        processor.IsProcessing.Should().BeFalse("it should report the SDK processor's state, so health checks see a stopped processor");
    }

    [Fact]
    public async Task When_Creating_A_Peeker_For_A_Subscription()
    {
        await using var peeker = (ServiceBusTransport.Peeker)transport.CreatePeeker("events", "billing");

        peeker.Inner.EntityPath.Should().Be("events/Subscriptions/billing", "it should peek the subscription it consumes from");
    }

    [Theory]
    [InlineData(null, "orders/$DeadLetterQueue")]
    [InlineData("billing", "orders/Subscriptions/billing/$DeadLetterQueue")]
    public async Task When_Creating_A_Dead_Letter_Receiver(string? subscription, string expectedPath)
    {
        await using var receiver = (ServiceBusTransport.DeadLetterReceiver)transport.CreateDeadLetterReceiver("orders", subscription);

        receiver.Inner.EntityPath.Should().Be(expectedPath, "it should read the entity's dead-letter subqueue");
        receiver.Inner.ReceiveMode.Should().Be(ServiceBusReceiveMode.PeekLock, "it should lock what it receives until it settles it");
        receiver.Inner.PrefetchCount.Should().Be(0, "it should not prefetch locks it may never use");
    }

    [Fact]
    public async Task When_Creating_A_Sender()
    {
        await using var sender = (ServiceBusTransport.Sender)transport.CreateSender("orders");

        sender.Inner.EntityPath.Should().Be("orders", "it should send to the given entity");
    }

    [Fact]
    public async Task When_Abandoning_With_Properties()
    {
        var receiver = Substitute.For<ServiceBusReceiver>();
        var message = FakeServiceBus.Message("order-1");
        var delivery = new ServiceBusTransport.Delivery(new ProcessMessageEventArgs(message, receiver, CancellationToken.None));
        var properties = new Dictionary<string, object> { [ServiceBusMessages.AttemptProperty] = 2 };

        await delivery.AbandonAsync(properties, TestContext.Current.CancellationToken);

        await receiver.Received(1).AbandonMessageAsync(message, Arg.Is<IDictionary<string, object>>(p => ReferenceEquals(p, properties)), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task When_Dead_Lettering()
    {
        var receiver = Substitute.For<ServiceBusReceiver>();
        var message = FakeServiceBus.Message("order-1");
        var delivery = new ServiceBusTransport.Delivery(new ProcessMessageEventArgs(message, receiver, CancellationToken.None));
        var properties = new Dictionary<string, object> { [ServiceBusMessages.DeadLetterReasonProperty] = "MaxRetriesExceeded" };

        await delivery.DeadLetterAsync(properties, "MaxRetriesExceeded", "System.TimeoutException", TestContext.Current.CancellationToken);

        await receiver.Received(1).DeadLetterMessageAsync(message, properties, "MaxRetriesExceeded", "System.TimeoutException", Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task When_Completing()
    {
        var receiver = Substitute.For<ServiceBusReceiver>();
        var message = FakeServiceBus.Message("order-1");
        var delivery = new ServiceBusTransport.Delivery(new ProcessMessageEventArgs(message, receiver, CancellationToken.None));

        await delivery.CompleteAsync(TestContext.Current.CancellationToken);

        await receiver.Received(1).CompleteMessageAsync(message, Arg.Any<CancellationToken>());
    }
}
