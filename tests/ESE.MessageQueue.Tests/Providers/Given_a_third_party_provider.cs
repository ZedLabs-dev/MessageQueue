using System.Diagnostics;
using AwesomeAssertions;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Extensions;
using ESE.MessageQueue.Processing;
using ESE.MessageQueue.Samples.LoopbackProvider;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace ESE.MessageQueue.Tests.Providers;

// ADR 0015: a provider compiled against the public SPI only (no InternalsVisibleTo) plugs into AddMessageQueue<T> by
// name and gets the shared pipeline: validation, retry, dead-lettering and trace propagation.
[Trait("Category", "Unit")]
public sealed class Given_a_third_party_provider : IAsyncDisposable
{
    readonly SpanCapture spans = new();
    readonly ServiceProvider provider;

    public Given_a_third_party_provider()
    {
        provider = Build(new Dictionary<string, string?>
        {
            ["MessageQueue:Provider"] = "loopback",
            ["MessageQueue:QueueName"] = "orders",
            ["MessageQueue:Retry:MaxRetries"] = "2",
            ["MessageQueue:Retry:InitialDelay"] = "00:00:00.005",
            ["MessageQueue:Retry:MaxDelay"] = "00:00:00.010",
            ["MessageQueue:Retry:UseJitter"] = "false"
        });
    }

    ServiceProvider Build(Dictionary<string, string?> values)
    {
        var services = new ServiceCollection();
        services.AddLoopbackProvider();
        services.AddMessageQueueTelemetry(spans.SourceName);
        services.AddMessageQueue<TestMessage>(new ConfigurationBuilder().AddInMemoryCollection(values).Build());
        return services.BuildServiceProvider();
    }

    public async ValueTask DisposeAsync()
    {
        await provider.DisposeAsync();
        spans.Dispose();
    }

    [Fact(Timeout = 10_000)]
    public async Task When_It_Is_Selected_By_Name()
    {
        var token = TestContext.Current.CancellationToken;
        var queue = provider.GetRequiredService<IMessageQueue<TestMessage>>();
        var received = new TaskCompletionSource<(TestMessage Message, MessageContext Context)>();
        await queue.SubscribeAsync((message, context, _) =>
        {
            received.TrySetResult((message, context));
            return Task.FromResult(MessageResult.Acknowledge);
        }, token);

        await queue.PublishAsync(new TestMessage("1", "hello"), new PublishOptions { Headers = new Dictionary<string, string> { ["tenant"] = "a" } }, token);
        var (message, context) = await received.Task.WaitAsync(token);

        queue.Should().BeOfType<LoopbackMessageQueue<TestMessage>>("it should register the queue of the provider the configuration names");
        message.Should().Be(new TestMessage("1", "hello"), "it should round-trip the message through the shared serializer");
        context.Headers.Should().Contain("tenant", "a", "it should deliver the publisher's headers");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_A_Handler_Keeps_Failing()
    {
        var token = TestContext.Current.CancellationToken;
        var queue = (LoopbackMessageQueue<TestMessage>)provider.GetRequiredService<IMessageQueue<TestMessage>>();
        var attempts = new List<int>();
        await queue.SubscribeAsync((_, context, _) =>
        {
            lock (attempts)
                attempts.Add(context.DeliveryAttempt);
            return Task.FromResult(MessageResult.Retry);
        }, token);

        await queue.PublishAsync(new TestMessage("1", "poison"), token);
        await Wait.UntilAsync(() => queue.DeadLetters.Count == 1, Wait.DeliveryTimeout);

        attempts.Should().Equal([1, 2, 3], "it should retry through the shared retry policy before giving up");
        queue.DeadLetters.Single().Reason.Should().Be(DeadLetterReasons.MaxRetriesExceeded,
            "it should dead-letter with the shared reason code once retries are exhausted");
    }

    // ADR 0023: dead-letter operations through the public SPI alone (IDeadLetterStore + DeadLetterQueue<T>).
    [Fact(Timeout = 10_000)]
    public async Task When_It_Opts_Into_Dead_Letter_Operations()
    {
        var token = TestContext.Current.CancellationToken;
        var queue = provider.GetRequiredService<IMessageQueue<TestMessage>>();
        var deadLetters = provider.GetRequiredService<IDeadLetterQueue<TestMessage>>();
        var attempts = new List<(string Id, int Attempt)>();
        var accept = false;
        await queue.SubscribeAsync((_, context, _) =>
        {
            lock (attempts)
                attempts.Add((context.MessageId, context.DeliveryAttempt));
            return Task.FromResult(Volatile.Read(ref accept) ? MessageResult.Acknowledge : MessageResult.DeadLetter);
        }, token);
        await queue.PublishAsync(new TestMessage("1", "rejected"), new PublishOptions { MessageId = "m-1" }, token);
        await Wait.UntilAsync(() => ((LoopbackMessageQueue<TestMessage>)queue).DeadLetters.Count == 1, Wait.DeliveryTimeout);

        var peeked = await deadLetters.PeekAsync(10, token);
        Volatile.Write(ref accept, true);
        var result = await deadLetters.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, token);
        await Wait.UntilAsync(() => attempts.Count == 2, Wait.DeliveryTimeout);

        queue.Capabilities.DeadLetterOperations.Should().Be(DeadLetterOperations.All, "it should declare the operations it supports");
        deadLetters.Should().BeSameAs(queue, "it should resolve the dead-letter operations as the queue");
        peeked.Should().ContainSingle("it should peek the dead letter").Which.ReasonCode.Should().Be(DeadLetterReasons.Rejected);
        result.Replayed.Should().Be(1, "it should replay the dead letter");
        attempts.Should().Equal([("m-1", 1), ("m-1", 1)], "it should redeliver the same message as a first attempt");
        ((LoopbackMessageQueue<TestMessage>)queue).DeadLetters.Should().BeEmpty("it should remove the replayed dead letter");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_A_Message_Is_Traced()
    {
        var token = TestContext.Current.CancellationToken;
        var queue = provider.GetRequiredService<IMessageQueue<TestMessage>>();
        await queue.SubscribeAsync((_, _, _) => Task.FromResult(MessageResult.Acknowledge), token);

        await queue.PublishAsync(new TestMessage("1", "traced"), token);
        var consume = (await spans.WaitForAsync(ActivityKind.Consumer, 1)).Single();

        consume.ParentSpanId.Should().Be(spans.Single(ActivityKind.Producer).SpanId,
            "it should parent the consume span on the publish span through the trace headers");
    }

    [Fact]
    public async Task When_Its_Configuration_Breaks_A_Shared_Rule()
    {
        await using var invalid = Build(new Dictionary<string, string?>
        {
            ["MessageQueue:Provider"] = "Loopback",
            ["MessageQueue:QueueName"] = "orders",
            ["MessageQueue:Retry:MaxRetries"] = "-1"
        });

        var act = () => invalid.GetRequiredService<IStartupValidator>().Validate();

        act.Should().Throw<OptionsValidationException>("it should apply the shared retry rules to a third-party config")
            .WithMessage("*Retry:MaxRetries*");
    }

    [Fact]
    public async Task When_Its_Configuration_Has_An_Unknown_Key()
    {
        await using var invalid = Build(new Dictionary<string, string?>
        {
            ["MessageQueue:Provider"] = "Loopback",
            ["MessageQueue:QueueName"] = "orders",
            ["MessageQueue:RetryAttempts"] = "3"
        });

        var act = () => invalid.GetRequiredService<IStartupValidator>().Validate();

        act.Should().Throw<InvalidOperationException>("it should bind a third-party config as strictly as the built-in ones")
            .WithMessage("*RetryAttempts*");
    }

    // §3a.5 (ADR 0020): a provider whose Capabilities say it cannot delay a retry is refused when the app requires it,
    // even though the provider knows nothing about the setting.
    [Fact]
    public async Task When_Delayed_Redelivery_Is_Required_But_Not_Supported()
    {
        await using var strict = Build(new Dictionary<string, string?>
        {
            ["MessageQueue:Provider"] = "loopback",
            ["MessageQueue:QueueName"] = "orders",
            ["MessageQueue:RequireDelayedRedelivery"] = "true"
        });

        var act = () => strict.GetRequiredService<IMessageQueue<TestMessage>>();

        act.Should().Throw<ESE.MessageQueue.Exceptions.ConfigurationException>("it should refuse a queue that cannot honor the required backoff")
            .WithMessage("*RequireDelayedRedelivery*");
    }

    [Fact]
    public async Task When_Delayed_Redelivery_Is_Required_Without_A_Backoff()
    {
        await using var strict = Build(new Dictionary<string, string?>
        {
            ["MessageQueue:Provider"] = "loopback",
            ["MessageQueue:QueueName"] = "orders",
            ["MessageQueue:RequireDelayedRedelivery"] = "true",
            ["MessageQueue:Retry:InitialDelay"] = "00:00:00"
        });

        strict.GetRequiredService<IMessageQueue<TestMessage>>().Should().NotBeNull("it should accept the queue when there is no delay to honor");
    }
}
