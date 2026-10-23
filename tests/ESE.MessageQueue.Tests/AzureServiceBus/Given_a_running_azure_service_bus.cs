using System.Collections.Concurrent;
using System.Diagnostics;
using AwesomeAssertions;
using Azure.Messaging.ServiceBus;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.AzureServiceBus;
using ESE.MessageQueue.HealthChecks;
using ESE.MessageQueue.Processing;
using ESE.MessageQueue.Providers;
using ESE.MessageQueue.Retry;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ESE.MessageQueue.Tests.AzureServiceBus;

// Runs only when MQ_ASB_CONNECTION_STRING is set; otherwise every test is reported as skipped.
// All tests share one predeclared queue (the emulator cannot create entities), so each test tags its
// messages with a unique run id and ignores messages left behind by other tests.
[Trait("Category", "Integration")]
public sealed class Given_a_running_azure_service_bus : IAsyncLifetime
{
    readonly string runId = Guid.NewGuid().ToString("N");
    readonly List<AzureServiceBusMessageQueue<TestMessage>> queues = [];
    AzureServiceBusConfig config = null!;

    public ValueTask InitializeAsync()
    {
        config = new AzureServiceBusConfig
        {
            ConnectionString = AzureServiceBusEnvironment.RequireConnectionString(),
            QueueName = AzureServiceBusEnvironment.QueueName,
            Retry = new RetryPolicy { MaxRetries = 2, InitialDelay = TimeSpan.FromMilliseconds(20), UseJitter = false },
            EnableDeadLetterQueue = true,
            MessageTimeToLive = TimeSpan.FromMinutes(30),
            MaxConcurrentCalls = 2,
            PrefetchCount = 5,
            TransactionalRetries = AzureServiceBusEnvironment.TransactionalRetries
        };
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var queue in queues)
        {
            await queue.DisposeAsync();
        }
    }

    AzureServiceBusMessageQueue<TestMessage> CreateQueue()
    {
        var queue = new AzureServiceBusMessageQueue<TestMessage>(config);
        queues.Add(queue);
        return queue;
    }

    TestMessage NewMessage(string name) => new($"{runId}-{name}", "test-content");

    bool IsMine(TestMessage message) => message.Id.StartsWith(runId, StringComparison.Ordinal);

    [Fact]
    public async Task When_Publishing_Single_Message()
    {
        var queue = CreateQueue();

        var action = () => queue.PublishAsync(NewMessage("single"), TestContext.Current.CancellationToken);

        await action.Should().NotThrowAsync("it should publish message without error");
    }

    [Fact]
    public async Task When_Publishing_Multiple_Messages()
    {
        var queue = CreateQueue();
        var messages = new[] { NewMessage("multi-1"), NewMessage("multi-2") };

        var action = () => queue.PublishAsync(messages, TestContext.Current.CancellationToken);

        await action.Should().NotThrowAsync("it should publish multiple messages without error");
    }

    [Fact]
    public async Task When_Subscribing_With_Handler()
    {
        var queue = CreateQueue();

        var action = () => queue.SubscribeAsync(_ => Task.FromResult(true), TestContext.Current.CancellationToken);

        await action.Should().NotThrowAsync("it should subscribe without error");
    }

    [Fact]
    public async Task When_Publishing_And_Subscribing()
    {
        var queue = CreateQueue();
        var received = new ConcurrentQueue<TestMessage>();
        var message = NewMessage("round-trip");

        await queue.SubscribeAsync(msg =>
        {
            if (IsMine(msg))
                received.Enqueue(msg);
            return Task.FromResult(true);
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(message, TestContext.Current.CancellationToken);
        await Wait.ForCountAsync(() => received.Count, 1, Wait.DeliveryTimeout);

        received.Should().ContainSingle("it should receive the published message")
            .Which.Id.Should().Be(message.Id, "it should receive the correct message");
    }

    [Fact]
    public async Task When_Publishing_Batch()
    {
        var queue = CreateQueue();
        var received = new ConcurrentQueue<TestMessage>();
        var messages = new[] { NewMessage("batch-1"), NewMessage("batch-2"), NewMessage("batch-3") };

        await queue.SubscribeAsync(msg =>
        {
            if (IsMine(msg))
                received.Enqueue(msg);
            return Task.FromResult(true);
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(messages, TestContext.Current.CancellationToken);
        await Wait.ForCountAsync(() => received.Count, 3, Wait.DeliveryTimeout);

        received.Select(m => m.Id).Should().BeEquivalentTo(messages.Select(m => m.Id), "it should receive all batch messages");
    }

    // Queue-mode retries are scheduled copies (ADR 0009): each retry waits its backoff on the broker and the handler
    // sees attempts 1, 2, 3 with the original message id. The gaps are measured on this client's clock but compared
    // with a 2 s allowance below the 3 s backoff, for broker scheduling granularity and clock skew.
    [Fact(Timeout = 60_000)]
    public async Task When_Message_Processing_Fails()
    {
        config = config with { Retry = new RetryPolicy { MaxRetries = 2, InitialDelay = TimeSpan.FromSeconds(3), MaxDelay = TimeSpan.FromSeconds(3), BackoffMultiplier = 1, UseJitter = false } };
        var queue = CreateQueue();
        var attempts = new ConcurrentQueue<(int Attempt, string MessageId, TimeSpan At)>();
        var clock = Stopwatch.StartNew();
        var message = NewMessage("retry");

        await queue.SubscribeAsync((msg, context, _) =>
        {
            if (msg.Id != message.Id)
                return Task.FromResult(MessageResult.Acknowledge);
            attempts.Enqueue((context.DeliveryAttempt, context.MessageId, clock.Elapsed));
            return Task.FromResult(MessageResult.Retry);
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(message, new PublishOptions { MessageId = message.Id }, TestContext.Current.CancellationToken);
        await Wait.ForCountAsync(() => attempts.Count, config.Retry.MaxRetries + 1, TimeSpan.FromSeconds(45));

        var seen = attempts.ToArray();
        seen.Select(a => a.Attempt).Should().Equal([1, 2, 3], "it should count each retry once, up to MaxRetries");
        seen.Should().AllSatisfy(a => a.MessageId.Should().Be(message.Id, "it should keep the original message id on retry copies"));
        (seen[1].At - seen[0].At).Should().BeGreaterThanOrEqualTo(TimeSpan.FromSeconds(1), "it should wait the backoff before the retry");
        (seen[2].At - seen[1].At).Should().BeGreaterThanOrEqualTo(TimeSpan.FromSeconds(1), "it should wait the backoff before every retry");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_Retries_Are_Exhausted()
    {
        var queue = CreateQueue();
        var message = NewMessage("dead-letter");

        await queue.SubscribeAsync((msg, _, _) =>
            Task.FromResult(msg.Id == message.Id ? MessageResult.Retry : MessageResult.Acknowledge), TestContext.Current.CancellationToken);
        await queue.PublishAsync(message, new PublishOptions { MessageId = message.Id }, TestContext.Current.CancellationToken);

        await using var client = new ServiceBusClient(config.ConnectionString);
        await using var deadLetters = client.CreateReceiver(config.QueueName, new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
        ServiceBusReceivedMessage? deadLetter = null;
        var deadline = Stopwatch.StartNew();
        while (deadLetter is null && deadline.Elapsed < TimeSpan.FromSeconds(30))
        {
            var received = await deadLetters.ReceiveMessageAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
            if (received is null)
                continue;
            await deadLetters.CompleteMessageAsync(received, TestContext.Current.CancellationToken);
            if (received.ApplicationProperties.TryGetValue(ServiceBusMessages.OriginalMessageIdProperty, out var id) && Equals(id, message.Id))
                deadLetter = received;
        }

        deadLetter.Should().NotBeNull("it should dead-letter the message after the last retry");
        deadLetter!.DeadLetterReason.Should().Be(DeadLetterReasons.MaxRetriesExceeded, "it should record the reason code");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_Resubscribing()
    {
        var queue = CreateQueue();
        await queue.SubscribeAsync(_ => Task.FromResult(true), TestContext.Current.CancellationToken);
        await queue.UnsubscribeAsync(TestContext.Current.CancellationToken);
        var received = new ConcurrentQueue<TestMessage>();
        var message = NewMessage("resubscribe");

        await queue.SubscribeAsync(msg =>
        {
            if (IsMine(msg))
                received.Enqueue(msg);
            return Task.FromResult(true);
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(message, TestContext.Current.CancellationToken);
        await Wait.ForCountAsync(() => received.Count, 1, Wait.DeliveryTimeout);

        received.Should().ContainSingle("it should deliver to the new subscription");
    }

    [Fact]
    public async Task When_Checking_Health()
    {
        var queue = CreateQueue();
        var check = new AzureServiceBusHealthCheck(queue);

        var result = await check.CheckHealthAsync(new HealthCheckContext { Registration = new HealthCheckRegistration("asb", check, null, null) },
            TestContext.Current.CancellationToken);

        result.Status.Should().Be(HealthStatus.Healthy, "it should peek the queue with Listen rights only");
    }

    // Subscription mode (ADR 0009): publish to the topic, consume the subscription; a retry waits under the lock and
    // is abandoned, so the handler sees attempt 2 after about the backoff, and nothing is sent back to the topic.
    [Fact(Timeout = 60_000)]
    public async Task When_Retrying_On_A_Subscription()
    {
        config = config with
        {
            TopicName = AzureServiceBusEnvironment.TopicName,
            SubscriptionName = AzureServiceBusEnvironment.SubscriptionName,
            PrefetchCount = 0,
            Retry = new RetryPolicy { MaxRetries = 1, InitialDelay = TimeSpan.FromSeconds(3), MaxDelay = TimeSpan.FromSeconds(3), UseJitter = false }
        };
        var queue = CreateQueue();
        var attempts = new ConcurrentQueue<(int Attempt, TimeSpan At)>();
        var clock = Stopwatch.StartNew();
        var message = NewMessage("subscription-retry");

        await queue.SubscribeAsync((msg, context, _) =>
        {
            if (msg.Id != message.Id)
                return Task.FromResult(MessageResult.Acknowledge);
            attempts.Enqueue((context.DeliveryAttempt, clock.Elapsed));
            return Task.FromResult(context.DeliveryAttempt == 1 ? MessageResult.Retry : MessageResult.Acknowledge);
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(message, new PublishOptions { MessageId = message.Id }, TestContext.Current.CancellationToken);
        await Wait.ForCountAsync(() => attempts.Count, 2, TimeSpan.FromSeconds(30));

        var seen = attempts.ToArray();
        seen.Select(a => a.Attempt).Should().Equal([1, 2], "it should redeliver to the same subscription as the next attempt");
        (seen[1].At - seen[0].At).Should().BeGreaterThanOrEqualTo(TimeSpan.FromSeconds(1), "it should hold the message for the backoff");
    }

    // ADR 0023: the replay sends to the queue and completes on its dead-letter subqueue, in one transaction when the
    // entity accepts it (else the provider falls back to send-then-complete). Verified only here, against a namespace.
    [Fact(Timeout = 120_000)]
    public async Task When_A_Dead_Letter_Is_Replayed_With_Transactional_Retries()
    {
        config = config with { TransactionalRetries = true };
        var queue = CreateQueue();
        var message = NewMessage("dead-letter-replay");
        var handled = new ConcurrentQueue<MessageContext>();
        var accept = false;
        await queue.SubscribeAsync((msg, context, _) =>
        {
            if (!IsMine(msg))
                return Task.FromResult(MessageResult.Acknowledge);
            handled.Enqueue(context);
            return Task.FromResult(Volatile.Read(ref accept) ? MessageResult.Acknowledge : MessageResult.DeadLetter);
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(message, new PublishOptions { MessageId = message.Id }, TestContext.Current.CancellationToken);
        await Wait.UntilAsync(() => handled.Count == 1, TimeSpan.FromSeconds(30));
        Volatile.Write(ref accept, true);

        var result = await queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage>
        {
            MaxCount = 10,
            Filter = letter => letter.MessageId == message.Id
        }, TestContext.Current.CancellationToken);
        await Wait.UntilAsync(() => handled.Count == 2, TimeSpan.FromSeconds(30));

        result.Replayed.Should().Be(1, "it should replay the dead letter");
        handled.Last().MessageId.Should().Be(message.Id, "it should deliver the replay under the original message id");
        handled.Last().DeliveryAttempt.Should().Be(1, "it should deliver the replay as a first attempt");
        (await queue.PeekAsync(100, TestContext.Current.CancellationToken)).Should().NotContain(l => l.MessageId == message.Id,
            "it should complete the dead letter");
    }
}
