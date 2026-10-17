using System.Diagnostics;
using AwesomeAssertions;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.AzureServiceBus;
using ESE.MessageQueue.Retry;
using Microsoft.Extensions.Logging;

namespace ESE.MessageQueue.Tests.AzureServiceBus;

// Topic/subscription mode: a scheduled copy sent to the topic would reach every subscription, so a retry waits
// under the message lock (when the lock-renewal budget allows) and then abandons with the next attempt number.
[Trait("Category", "Unit")]
public class Given_an_azure_service_bus_subscription_retrying
{
    readonly FakeServiceBus bus = new();

    static AzureServiceBusConfig WithDelay(TimeSpan delay) => AzureServiceBusTestConfigs.Subscription() with
    {
        Retry = AzureServiceBusTestConfigs.FastRetry with { InitialDelay = delay, MaxDelay = delay }
    };

    [Fact]
    public async Task When_Subscribing()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Subscription());
        await queue.SubscribeAsync(AzureServiceBusTestConfigs.Returns(MessageResult.Acknowledge), TestContext.Current.CancellationToken);

        bus.Processor.EntityPath.Should().Be("events", "it should receive from the topic");
        bus.Processor.SubscriptionName.Should().Be("billing", "it should receive from the configured subscription");
        bus.Sender.EntityPath.Should().Be("events", "it should publish to the topic");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_A_Handler_Asks_For_A_Retry()
    {
        await using var queue = bus.CreateQueue(WithDelay(TimeSpan.FromMilliseconds(300)));
        await queue.SubscribeAsync(AzureServiceBusTestConfigs.Returns(MessageResult.Retry), TestContext.Current.CancellationToken);
        var watch = Stopwatch.StartNew();

        var delivery = bus.Processor.Deliver(FakeServiceBus.Message("order-1", deliveryCount: 1));
        await delivery.Handled.WaitAsync(TestContext.Current.CancellationToken);

        watch.Elapsed.Should().BeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(250), "it should wait out the backoff while it holds the lock");
        bus.Sender.Sent.Should().BeEmpty("it should not send a copy that every subscription would receive");
        delivery.Settlement.Should().Be("abandon", "it should abandon the message for redelivery to this subscription");
        delivery.Properties.Should().Contain(ServiceBusMessages.AttemptProperty, 2, "it should record the next attempt");
        delivery.Properties.Should().Contain(ServiceBusMessages.DeliveryCountProperty, 2, "it should record the delivery count the redelivery will have");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Delay_Exceeds_The_Lock_Budget()
    {
        var logger = new RecordingLogger();
        var config = WithDelay(TimeSpan.FromSeconds(30)) with { MaxAutoLockRenewalDuration = TimeSpan.Zero };
        await using var queue = bus.CreateQueue(config, logger);
        await queue.SubscribeAsync(AzureServiceBusTestConfigs.Returns(MessageResult.Retry), TestContext.Current.CancellationToken);
        var watch = Stopwatch.StartNew();

        var delivery = bus.Processor.Deliver(FakeServiceBus.Message("order-1", lockedUntil: DateTimeOffset.UtcNow.AddSeconds(5)));
        await delivery.Handled.WaitAsync(TestContext.Current.CancellationToken);

        watch.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(3), "it should not hold a message beyond its lock");
        delivery.Settlement.Should().Be("abandon", "it should abandon right away instead");
        logger.Entries.Should().Contain(e => e.Level == LogLevel.Warning && e.Message.Contains("lock"),
            "it should say that the backoff could not be honoured");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Shutting_Down_While_A_Retry_Waits()
    {
        await using var queue = bus.CreateQueue(WithDelay(TimeSpan.FromSeconds(20)));
        await queue.SubscribeAsync(AzureServiceBusTestConfigs.Returns(MessageResult.Retry), TestContext.Current.CancellationToken);
        var delivery = bus.Processor.Deliver(FakeServiceBus.Message("order-1"));
        await Task.Delay(200, TestContext.Current.CancellationToken);
        var watch = Stopwatch.StartNew();

        await queue.UnsubscribeAsync(TestContext.Current.CancellationToken);

        watch.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(3), "it should cut the wait short on shutdown");
        delivery.Settlement.Should().Be("abandon", "it should abandon the waiting retry");
        delivery.Properties.Should().Contain(ServiceBusMessages.AttemptProperty, 2, "it should still count the failed attempt");
    }

    [Fact]
    public async Task When_Reading_Capabilities()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Subscription());

        queue.Capabilities.DelayedRedelivery.Should().BeFalse("it should admit that a subscription retry holds the message and a handler slot");
    }

    [Fact]
    public async Task When_The_Retry_Policy_Has_A_Backoff()
    {
        var logger = new RecordingLogger();

        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Subscription() with { Retry = new RetryPolicy() }, logger);

        logger.Entries.Should().Contain(e => e.Level == LogLevel.Warning && e.Message.Contains("DelayedRedelivery"),
            "it should warn that a subscription cannot honour the backoff on the broker");
    }

    [Fact]
    public async Task When_The_Retries_Exceed_The_Default_MaxDeliveryCount()
    {
        var logger = new RecordingLogger();

        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Subscription() with { Retry = RetryPolicy.None with { MaxRetries = 10 } }, logger);

        logger.Entries.Should().Contain(e => e.Level == LogLevel.Warning && e.Message.Contains("MaxDeliveryCount"),
            "it should warn that the broker may dead-letter before the retry policy gives up");
    }

    [Fact]
    public async Task When_The_Retries_Fit_The_Default_MaxDeliveryCount()
    {
        var logger = new RecordingLogger();

        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Subscription() with { Retry = RetryPolicy.None with { MaxRetries = 9 } }, logger);

        logger.Entries.Should().NotContain(e => e.Message.Contains("MaxDeliveryCount"), "it should not warn when every attempt fits");
    }

    // B09: the lock budget is the later of the renewal deadline and the current lock, minus the safety margin.
    [Fact(Timeout = 10_000)]
    public async Task When_Only_The_Current_Lock_Leaves_Room_For_The_Delay()
    {
        var config = WithDelay(TimeSpan.FromSeconds(1)) with { MaxAutoLockRenewalDuration = TimeSpan.Zero };
        await using var queue = bus.CreateQueue(config);
        await queue.SubscribeAsync(AzureServiceBusTestConfigs.Returns(MessageResult.Retry), TestContext.Current.CancellationToken);
        var watch = Stopwatch.StartNew();

        var delivery = bus.Processor.Deliver(FakeServiceBus.Message("order-1", lockedUntil: DateTimeOffset.UtcNow.AddSeconds(4)));
        await delivery.Handled.WaitAsync(TestContext.Current.CancellationToken);

        watch.Elapsed.Should().BeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(900), "it should wait while the current lock still covers the delay");
    }

    // B10: the safety margin shrinks the budget; it never extends it.
    [Fact(Timeout = 10_000)]
    public async Task When_The_Delay_Only_Fits_Without_The_Safety_Margin()
    {
        var logger = new RecordingLogger();
        var config = WithDelay(TimeSpan.FromSeconds(2)) with { MaxAutoLockRenewalDuration = TimeSpan.Zero };
        await using var queue = bus.CreateQueue(config, logger);
        await queue.SubscribeAsync(AzureServiceBusTestConfigs.Returns(MessageResult.Retry), TestContext.Current.CancellationToken);
        var watch = Stopwatch.StartNew();

        var delivery = bus.Processor.Deliver(FakeServiceBus.Message("order-1", lockedUntil: DateTimeOffset.UtcNow.AddSeconds(3)));
        await delivery.Handled.WaitAsync(TestContext.Current.CancellationToken);

        watch.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(1), "it should keep two seconds of the lock in reserve and abandon at once");
        logger.Entries.Should().Contain(e => e.Level == LogLevel.Warning && e.Message.Contains("lock"), "it should say the backoff did not fit");
    }

    [Fact]
    public async Task When_A_Subscription_Prefetches()
    {
        var logger = new RecordingLogger();

        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Subscription() with { PrefetchCount = 10 }, logger);

        logger.Entries.Should().Contain(e => e.Level == LogLevel.Warning && e.Message.Contains("PrefetchCount"),
            "it should warn that prefetched messages' locks expire while retries hold handler slots");
    }

    [Fact]
    public async Task When_A_Subscription_Does_Not_Prefetch()
    {
        var logger = new RecordingLogger();

        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Subscription() with { PrefetchCount = 0 }, logger);

        logger.Entries.Should().NotContain(e => e.Message.Contains("PrefetchCount"), "it should not warn without prefetch");
    }
}
