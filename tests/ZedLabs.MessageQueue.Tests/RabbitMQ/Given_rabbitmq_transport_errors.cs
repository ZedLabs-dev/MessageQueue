using AwesomeAssertions;
using Microsoft.Extensions.Logging;
using NSubstitute;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using ZedLabs.MessageQueue.RabbitMQ;
using ZedLabs.MessageQueue.Telemetry;
using ZedLabs.MessageQueue.Tests.Telemetry;

namespace ZedLabs.MessageQueue.Tests.RabbitMQ;

// Transport trouble outside a delivery is counted in zedlabs.messagequeue.transport.errors with a fixed error.type, as on
// Service Bus. Driven through the queue's client event handlers; the connection failure dials 127.0.0.1:1, which
// refuses at once (no broker, Docker or network).
[Trait("Category", "Unit")]
public sealed class Given_rabbitmq_transport_errors : IAsyncDisposable
{
    readonly TestTelemetryProvider telemetry = new();
    readonly RecordingLogger logger = new();
    readonly List<RabbitMqMessageQueue<TestMessage>> created = [];

    RabbitMqMessageQueue<TestMessage> Create(ITelemetryProvider? provider = null)
    {
        var queue = new RabbitMqMessageQueue<TestMessage>(
            new RabbitMqConfig { ConnectionString = "amqp://app:secret@127.0.0.1:1/", QueueName = "orders" }, provider ?? telemetry, logger);
        created.Add(queue);
        return queue;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var queue in created)
            await queue.DisposeAsync();
    }

    static ShutdownEventArgs Shutdown(ShutdownInitiator initiator) => new(initiator, 320, "CONNECTION_FORCED - secret-host closed it");

    List<string> Recorded() => telemetry.TransportErrors.Select(e => e.ErrorType).ToList();

    [Fact]
    public async Task When_Connecting_Fails()
    {
        var queue = Create();

        var act = () => queue.InitializeAsync(TestContext.Current.CancellationToken);
        await act.Should().ThrowAsync<Exceptions.MessageQueueException>();

        Recorded().Should().Equal(["ConnectionFailed"], "it should count a failed connection attempt");
        telemetry.TransportErrors.Single().Target.Destination.Should().Be("orders:orders", "it should name the queue's destination");
    }

    [Fact]
    public async Task When_A_Call_Fails_Fast_During_The_Connect_Backoff()
    {
        var queue = Create();
        var act = () => queue.InitializeAsync(TestContext.Current.CancellationToken);
        await act.Should().ThrowAsync<Exceptions.MessageQueueException>();

        await act.Should().ThrowAsync<Exceptions.MessageQueueException>();

        Recorded().Should().Equal(["ConnectionFailed"], "it should not count a call that did not dial the broker");
    }

    [Theory]
    [InlineData(ShutdownInitiator.Peer)]
    [InlineData(ShutdownInitiator.Library)]
    public async Task When_The_Connection_Is_Lost(ShutdownInitiator initiator)
    {
        await Create().OnConnectionShutdownAsync(Shutdown(initiator));

        Recorded().Should().Equal(["ConnectionShutdown"], "it should count a connection the library did not close");
    }

    [Fact]
    public async Task When_The_Library_Closes_The_Connection()
    {
        await Create().OnConnectionShutdownAsync(Shutdown(ShutdownInitiator.Application));

        Recorded().Should().BeEmpty("it should not count its own close");
    }

    [Fact]
    public async Task When_Connection_Recovery_Fails()
    {
        await Create().OnRecoveryFailedAsync();

        Recorded().Should().Equal(["RecoveryFailed"], "it should count every failed recovery attempt");
    }

    [Theory]
    [InlineData(ShutdownInitiator.Peer)]
    [InlineData(ShutdownInitiator.Library)]
    public async Task When_The_Consumer_Channel_Is_Closed_On_An_Open_Connection(ShutdownInitiator initiator)
    {
        await Create().OnConsumerChannelShutdownAsync(Shutdown(initiator), connectionOpen: true);

        Recorded().Should().Equal(["ChannelShutdown"], "it should count a consumer channel the broker or client closed");
    }

    [Fact]
    public async Task When_The_Consumer_Channel_Closes_With_Its_Connection()
    {
        await Create().OnConsumerChannelShutdownAsync(Shutdown(ShutdownInitiator.Peer), connectionOpen: false);

        Recorded().Should().BeEmpty("it should count a lost connection once, not again for its channel");
    }

    [Fact]
    public async Task When_The_Library_Closes_The_Consumer_Channel()
    {
        await Create().OnConsumerChannelShutdownAsync(Shutdown(ShutdownInitiator.Application), connectionOpen: true);

        Recorded().Should().BeEmpty("it should not count its own unsubscribe");
    }

    [Fact]
    public async Task When_The_Broker_Cancels_The_Consumer()
    {
        await Create().OnConsumerCancelledByBrokerAsync();

        Recorded().Should().Equal(["ConsumerCancelled"], "it should count a consumer the broker cancelled");
    }

    [Fact]
    public async Task When_Telemetry_Throws()
    {
        var queue = Create(new ThrowingTelemetryProvider());

        var act = () => queue.OnConnectionShutdownAsync(Shutdown(ShutdownInitiator.Peer));

        await act.Should().NotThrowAsync("it should never let telemetry break the client's event handling");
        logger.Entries.Should().Contain(e => e.Level == LogLevel.Warning && e.Message.Contains("telemetry", StringComparison.OrdinalIgnoreCase),
            "it should log the telemetry failure");
    }
    [Fact]
    public void When_The_Client_Reports_A_Lost_Connection_And_A_Failed_Recovery()
    {
        var connection = Substitute.For<IConnection>();
        Create().WatchConnection(connection);

        connection.ConnectionShutdownAsync += Raise.Event<AsyncEventHandler<ShutdownEventArgs>>(connection, Shutdown(ShutdownInitiator.Peer));
        connection.ConnectionRecoveryErrorAsync += Raise.Event<AsyncEventHandler<ConnectionRecoveryErrorEventArgs>>(
            connection, new ConnectionRecoveryErrorEventArgs(new IOException("refused"), CancellationToken.None));

        Recorded().Should().Equal(["ConnectionShutdown", "RecoveryFailed"], "it should listen to the connection's shutdown and recovery events");
    }

    [Theory]
    [InlineData(true, new[] { "ChannelShutdown" })]
    [InlineData(false, new string[0])]
    public void When_The_Client_Reports_A_Closed_Consumer_Channel(bool connectionOpen, string[] expected)
    {
        var connection = Substitute.For<IConnection>();
        connection.IsOpen.Returns(connectionOpen);
        var channel = Substitute.For<IChannel>();
        Create().WatchConsumerChannel(channel, connection);

        channel.ChannelShutdownAsync += Raise.Event<AsyncEventHandler<ShutdownEventArgs>>(channel, Shutdown(ShutdownInitiator.Peer));

        Recorded().Should().Equal(expected, "it should listen to the consumer channel and ask whether its connection is still open");
    }
    [Fact]
    public async Task When_Telemetry_And_The_Logger_Throw()
    {
        await using var queue = new RabbitMqMessageQueue<TestMessage>(
            new RabbitMqConfig { ConnectionString = "amqp://app:secret@127.0.0.1:1/", QueueName = "orders" }, new ThrowingTelemetryProvider(), new ThrowingLogger());

        var act = () => queue.OnConnectionShutdownAsync(Shutdown(ShutdownInitiator.Peer));

        await act.Should().NotThrowAsync("it should never throw back into the client's event handling, not even when logging fails");
    }
}
