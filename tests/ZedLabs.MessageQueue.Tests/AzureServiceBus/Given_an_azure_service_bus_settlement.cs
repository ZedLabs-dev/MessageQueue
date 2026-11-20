using System.Diagnostics;
using AwesomeAssertions;
using Azure.Core.Amqp;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using ZedLabs.MessageQueue.Abstractions;
using ZedLabs.MessageQueue.AzureServiceBus;
using ZedLabs.MessageQueue.Processing;
using ZedLabs.MessageQueue.Providers;
using ZedLabs.MessageQueue.Retry;

namespace ZedLabs.MessageQueue.Tests.AzureServiceBus;

// Every message is settled exactly once; poison messages and failing settlements never wedge the processor (S6 too).
[Trait("Category", "Unit")]
public class Given_an_azure_service_bus_settlement : IAsyncDisposable
{
    readonly FakeServiceBus bus = new();

    // The fake stands in for the SDK client; disposing it with the test class keeps CA1001 honest.
    public ValueTask DisposeAsync() => bus.DisposeAsync();
    readonly RecordingLogger logger = new();

    async Task<AzureServiceBusMessageQueue<TestMessage>> SubscribedAsync(
        Func<TestMessage, MessageContext, CancellationToken, Task<MessageResult>> handler, AzureServiceBusConfig? config = null)
    {
        var queue = bus.CreateQueue(config ?? AzureServiceBusTestConfigs.Queue(), logger);
        await queue.SubscribeAsync(handler, TestContext.Current.CancellationToken);
        return queue;
    }

    // An AMQP value body: ServiceBusReceivedMessage.Body throws for anything but a data body.
    static ServiceBusReceivedMessage UnreadableMessage()
    {
        var amqp = new AmqpAnnotatedMessage(AmqpMessageBody.FromValue(42));
        amqp.Properties.MessageId = new AmqpMessageId("poison");
        return ServiceBusReceivedMessage.FromAmqpMessage(amqp, BinaryData.FromBytes(Guid.NewGuid().ToByteArray()));
    }

    [Fact(Timeout = 10_000)]
    public async Task When_A_Message_Is_Acknowledged()
    {
        await using var queue = await SubscribedAsync(AzureServiceBusTestConfigs.Returns(MessageResult.Acknowledge));

        var delivery = bus.Processor.Deliver(FakeServiceBus.Message());
        await delivery.Handled.WaitAsync(TestContext.Current.CancellationToken);

        delivery.Attempts.Should().Equal(["complete"], "it should settle the message exactly once");
        delivery.HandlerException.Should().BeNull("it should never let an exception escape to the processor");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_A_Message_Body_Cannot_Be_Read()
    {
        var handled = 0;
        await using var queue = await SubscribedAsync((_, _, _) =>
        {
            Interlocked.Increment(ref handled);
            return Task.FromResult(MessageResult.Acknowledge);
        });

        var delivery = bus.Processor.Deliver(UnreadableMessage());
        await delivery.Handled.WaitAsync(TestContext.Current.CancellationToken);

        handled.Should().Be(0, "it should never hand an unreadable message to the handler");
        delivery.Settlement.Should().Be("deadletter", "it should dead-letter a poison message instead of redelivering it forever");
        delivery.DeadLetterReason.Should().Be(DeadLetterReasons.DeliveryFailed, "it should record why");
        delivery.DeadLetterDescription.Should().NotBeNullOrWhiteSpace().And.NotContain(" ", "it should record the exception type only");
        delivery.HandlerException.Should().BeNull("it should never let an exception escape to the processor");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Dead_Lettering_Is_Disabled_And_A_Message_Cannot_Be_Read()
    {
        await using var queue = await SubscribedAsync(AzureServiceBusTestConfigs.Returns(MessageResult.Acknowledge),
            AzureServiceBusTestConfigs.Queue() with { EnableDeadLetterQueue = false });

        var delivery = bus.Processor.Deliver(UnreadableMessage());
        await delivery.Handled.WaitAsync(TestContext.Current.CancellationToken);

        delivery.Settlement.Should().Be("complete", "it should drop the poison message");
        logger.Entries.Should().Contain(e => e.Level == LogLevel.Warning && e.Message.Contains("dropped"), "it should warn that it dropped it");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Handler_Throws()
    {
        await using var queue = await SubscribedAsync((_, _, _) => throw new InvalidOperationException("card 4111-1111 was declined"),
            AzureServiceBusTestConfigs.Queue() with { Retry = RetryPolicy.None });

        var delivery = bus.Processor.Deliver(FakeServiceBus.Message());
        await delivery.Handled.WaitAsync(TestContext.Current.CancellationToken);

        delivery.Settlement.Should().Be("deadletter", "it should dead-letter once retries are exhausted");
        delivery.DeadLetterDescription.Should().Be(typeof(InvalidOperationException).FullName, "it should record the exception type");
        delivery.Properties.Should().Contain(ServiceBusMessages.DeadLetterReasonProperty, DeadLetterReasons.MaxRetriesExceeded,
            "it should record the reason as an application property too");
        delivery.Properties.Should().Contain(ServiceBusMessages.DeadLetterDetailProperty, typeof(InvalidOperationException).FullName!,
            "it should record the exception type as an application property");
        delivery.Properties!.Keys.Should().NotContain("OriginalException", "it should not store exception text on the message");
        delivery.Properties.Values.Select(v => v.ToString()).Should().NotContain(v => v!.Contains("4111"),
            "it should never store the exception message on the broker");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Completing_Loses_The_Lock()
    {
        await using var queue = await SubscribedAsync(AzureServiceBusTestConfigs.Returns(MessageResult.Acknowledge));

        var delivery = bus.Processor.Deliver(FakeServiceBus.Message(),
            action => action == "complete" ? new ServiceBusException("lock lost", ServiceBusFailureReason.MessageLockLost) : null);
        await delivery.Handled.WaitAsync(TestContext.Current.CancellationToken);

        delivery.Attempts.Should().Equal(["complete"], "it should not try to settle a message whose lock is gone");
        delivery.HandlerException.Should().BeNull("it should not crash the processor");
        logger.Entries.Should().Contain(e => e.Level == LogLevel.Warning && e.Message.Contains("lock"), "it should log the lost lock");
    }

    [Fact(Timeout = 20_000)]
    public async Task When_Settlement_Keeps_Failing()
    {
        await using var queue = await SubscribedAsync(AzureServiceBusTestConfigs.Returns(MessageResult.Acknowledge));
        static Exception? Busy(string action) => action == "complete" ? new ServiceBusException("busy", ServiceBusFailureReason.ServiceBusy) : null;

        var watch = Stopwatch.StartNew();
        var first = bus.Processor.Deliver(FakeServiceBus.Message("order-1", deliveryCount: 1), Busy);
        await first.Handled.WaitAsync(TestContext.Current.CancellationToken);
        var firstElapsed = watch.Elapsed;
        watch.Restart();
        var second = bus.Processor.Deliver(FakeServiceBus.Message("order-1", deliveryCount: 2), Busy);
        await second.Handled.WaitAsync(TestContext.Current.CancellationToken);
        var secondElapsed = watch.Elapsed;

        first.Settlement.Should().Be("abandon", "it should hand the message back after the failure");
        firstElapsed.Should().BeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(90), "it should back off before handing the message back");
        secondElapsed.Should().BeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(190), "it should back off longer when the same message keeps failing");
        logger.Entries.Count(e => e.Level == LogLevel.Error).Should().Be(1, "it should throttle the repeated error");
    }

    [Fact(Timeout = 20_000)]
    public async Task When_Every_Settlement_Fails()
    {
        await using var queue = await SubscribedAsync(AzureServiceBusTestConfigs.Returns(MessageResult.Acknowledge));

        var delivery = bus.Processor.Deliver(FakeServiceBus.Message(),
            _ => new ServiceBusException("down", ServiceBusFailureReason.ServiceCommunicationProblem));
        await delivery.Handled.WaitAsync(TestContext.Current.CancellationToken);

        delivery.HandlerException.Should().BeNull("it should never let a settlement failure escape to the processor");
        delivery.Attempts.Should().Equal(["complete", "abandon"], "it should try once more to hand the message back, then leave it to the lock expiry");
    }

    // P8: senders other than the Service Bus SDKs may leave MessageId unset.
    [Fact(Timeout = 10_000)]
    public async Task When_A_Message_Has_No_MessageId()
    {
        MessageContext? seen = null;
        await using var queue = await SubscribedAsync((_, context, _) =>
        {
            seen = context;
            return Task.FromResult(MessageResult.Acknowledge);
        });
        var message = ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString("""{"id":"1","content":"x"}"""), messageId: null,
            deliveryCount: 1, lockedUntil: DateTimeOffset.UtcNow.AddMinutes(1));

        var delivery = bus.Processor.Deliver(message);
        await delivery.Handled.WaitAsync(TestContext.Current.CancellationToken);

        delivery.Attempts.Should().Equal(["complete"], "it should complete the message once");
        seen!.MessageId.Should().BeEmpty("it should expose a missing id as empty, never null");
        logger.Entries.Should().NotContain(e => e.Level >= LogLevel.Warning, "it should not treat a missing id as a failure");
    }
}
