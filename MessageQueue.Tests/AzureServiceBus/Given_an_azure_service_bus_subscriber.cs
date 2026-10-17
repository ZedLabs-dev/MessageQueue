using AwesomeAssertions;
using Azure.Messaging.ServiceBus;
using ESE.MessageQueue.Abstractions;
using Microsoft.Extensions.Logging;

namespace ESE.MessageQueue.Tests.AzureServiceBus;

// B6: one subscriber at a time, re-subscribing works, and processor errors are logged instead of swallowed.
[Trait("Category", "Unit")]
public class Given_an_azure_service_bus_subscriber
{
    readonly FakeServiceBus bus = new();
    readonly RecordingLogger logger = new();

    [Fact]
    public async Task When_Subscribing_Twice()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        await queue.SubscribeAsync(AzureServiceBusTestConfigs.Returns(MessageResult.Acknowledge), TestContext.Current.CancellationToken);

        var act = () => queue.SubscribeAsync(AzureServiceBusTestConfigs.Returns(MessageResult.Acknowledge), TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<InvalidOperationException>("it should allow one subscriber at a time").WithMessage("*UnsubscribeAsync*");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Resubscribing_After_Unsubscribe()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        await queue.SubscribeAsync(AzureServiceBusTestConfigs.Returns(MessageResult.Acknowledge), TestContext.Current.CancellationToken);
        await queue.UnsubscribeAsync(TestContext.Current.CancellationToken);
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await queue.SubscribeAsync((_, _, _) =>
        {
            handled.TrySetResult();
            return Task.FromResult(MessageResult.Acknowledge);
        }, TestContext.Current.CancellationToken);
        bus.Processor.Deliver(FakeServiceBus.Message());
        await handled.Task;

        bus.Processors.Should().HaveCount(2, "it should create a fresh processor for the new subscription");
        bus.Processors[0].Disposed.Should().BeTrue("it should dispose the previous subscription's processor");
        bus.Processors[1].IsProcessing.Should().BeTrue("it should start the new processor");
    }

    [Fact]
    public async Task When_The_Processor_Reports_An_Error()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue(), logger);
        await queue.SubscribeAsync(AzureServiceBusTestConfigs.Returns(MessageResult.Acknowledge), TestContext.Current.CancellationToken);

        await bus.Processor.RaiseErrorAsync(new ServiceBusException("link detached", ServiceBusFailureReason.ServiceCommunicationProblem));

        logger.Entries.Should().ContainSingle(e => e.Level == LogLevel.Error, "it should log processor errors instead of swallowing them")
            .Which.Message.Should().Contain("Receive", "it should name the error source").And.Contain("orders", "it should name the entity");
    }

    [Fact]
    public async Task When_The_Same_Error_Repeats()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue(), logger);
        await queue.SubscribeAsync(AzureServiceBusTestConfigs.Returns(MessageResult.Acknowledge), TestContext.Current.CancellationToken);

        for (var i = 0; i < 5; i++)
            await bus.Processor.RaiseErrorAsync(new ServiceBusException("link detached", ServiceBusFailureReason.ServiceCommunicationProblem));

        logger.Entries.Where(e => e.Level >= LogLevel.Warning).Should().ContainSingle("it should throttle repeated identical errors");
    }

    [Fact]
    public async Task When_Different_Errors_Occur()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue(), logger);
        await queue.SubscribeAsync(AzureServiceBusTestConfigs.Returns(MessageResult.Acknowledge), TestContext.Current.CancellationToken);

        await bus.Processor.RaiseErrorAsync(new ServiceBusException("link detached", ServiceBusFailureReason.ServiceCommunicationProblem));
        await bus.Processor.RaiseErrorAsync(new UnauthorizedAccessException("no Listen claim"), ServiceBusErrorSource.Receive);

        logger.Entries.Where(e => e.Level >= LogLevel.Warning).Should().HaveCount(2, "it should log each distinct error");
    }

    [Fact]
    public async Task When_A_Lock_Renewal_Fails()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue(), logger);
        await queue.SubscribeAsync(AzureServiceBusTestConfigs.Returns(MessageResult.Acknowledge), TestContext.Current.CancellationToken);

        await bus.Processor.RaiseErrorAsync(new ServiceBusException("lock lost", ServiceBusFailureReason.MessageLockLost), ServiceBusErrorSource.RenewLock);

        logger.Entries.Should().ContainSingle(e => e.Level == LogLevel.Warning, "it should log a lost lock as a warning: the message is redelivered")
            .Which.Message.Should().Contain("RenewLock", "it should name the error source");
    }

    [Fact]
    public async Task When_Subscribing_The_Processor_Is_Configured()
    {
        var config = AzureServiceBusTestConfigs.Queue() with
        {
            MaxConcurrentCalls = 7,
            PrefetchCount = 13,
            MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(3)
        };
        await using var queue = bus.CreateQueue(config);

        await queue.SubscribeAsync(AzureServiceBusTestConfigs.Returns(MessageResult.Acknowledge), TestContext.Current.CancellationToken);

        var options = bus.Processor.Options;
        options.AutoCompleteMessages.Should().BeFalse("it should settle every message itself");
        options.ReceiveMode.Should().Be(Azure.Messaging.ServiceBus.ServiceBusReceiveMode.PeekLock, "it should never lose a message it has not settled");
        options.MaxConcurrentCalls.Should().Be(7, "it should apply MaxConcurrentCalls");
        options.PrefetchCount.Should().Be(13, "it should apply PrefetchCount");
        options.MaxAutoLockRenewalDuration.Should().Be(TimeSpan.FromMinutes(3), "it should renew locks while handlers run");
        bus.Processor.EntityPath.Should().Be("orders", "it should receive from the queue");
        bus.Processor.SubscriptionName.Should().BeNull("it should not use a subscription for a queue");
    }
}
