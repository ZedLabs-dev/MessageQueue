using System.Collections.Concurrent;
using AwesomeAssertions;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Extensions;
using ESE.MessageQueue.Hosting;
using ESE.MessageQueue.InMemory;
using ESE.MessageQueue.Processing;
using ESE.MessageQueue.Retry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ESE.MessageQueue.Tests.Hosting;

[Trait("Category", "Unit")]
public sealed class Given_a_message_queue_hosted_service : IAsyncDisposable
{
    public sealed class Script
    {
        public Func<TestMessage, MessageContext, MessageResult> Behavior { get; set; } = (_, _) => MessageResult.Acknowledge;
        public ConcurrentQueue<(ScriptedProcessor Processor, ScopedProbe Probe, MessageContext Context)> Calls { get; } = new();
        int disposedProbes;

        public int DisposedProbes => Volatile.Read(ref disposedProbes);

        public void ProbeDisposed() => Interlocked.Increment(ref disposedProbes);
    }

    public sealed class ScopedProbe : IDisposable
    {
        readonly Script script;

        public ScopedProbe(Script script)
        {
            this.script = script;
        }

        public void Dispose() => script.ProbeDisposed();
    }

    // A scoped dependency whose disposal fails, e.g. a unit of work that throws while flushing on dispose.
    public sealed class FailingDisposal : IAsyncDisposable
    {
        public ValueTask DisposeAsync() => throw new InvalidOperationException("flush failed");
    }

    public sealed class ScriptedProcessor : IMessageProcessor<TestMessage>
    {
        readonly Script script;
        readonly ScopedProbe probe;

        public ScriptedProcessor(Script script, ScopedProbe probe)
        {
            this.script = script;
            this.probe = probe;
        }

        public Task<MessageResult> ProcessAsync(TestMessage message, MessageContext context, CancellationToken cancellationToken)
        {
            script.Calls.Enqueue((this, probe, context));
            return Task.FromResult(script.Behavior(message, context));
        }
    }

    static readonly RetryPolicy FastRetry = new()
    {
        MaxRetries = 2,
        InitialDelay = TimeSpan.FromMilliseconds(5),
        MaxDelay = TimeSpan.FromMilliseconds(20),
        UseJitter = false
    };

    readonly Script script = new();
    readonly RecordingLogger<MessageQueueHostedService<TestMessage>> logger = new();
    readonly ServiceProvider provider;
    readonly InMemoryMessageQueue<TestMessage> queue;

    public Given_a_message_queue_hosted_service() : this(registerProcessor: true)
    {
    }

    Given_a_message_queue_hosted_service(bool registerProcessor, bool failingDisposal = false)
    {
        var services = new ServiceCollection();
        services.AddSingleton(script);
        services.AddScoped<ScopedProbe>();
        if (failingDisposal)
        {
            services.AddScoped<FailingDisposal>();
            services.AddScoped<IMessageProcessor<TestMessage>>(sp =>
            {
                sp.GetRequiredService<FailingDisposal>();
                return new ScriptedProcessor(sp.GetRequiredService<Script>(), sp.GetRequiredService<ScopedProbe>());
            });
            registerProcessor = false;
        }
        services.AddSingleton<ILogger<MessageQueueHostedService<TestMessage>>>(logger);
        services.AddInMemoryMessageQueue<TestMessage>(config => config.Retry = FastRetry);
        if (registerProcessor)
        {
            services.AddMessageProcessor<TestMessage, ScriptedProcessor>();
        }
        services.AddHostedService<MessageQueueHostedService<TestMessage>>();
        provider = services.BuildServiceProvider();
        queue = provider.GetRequiredService<InMemoryMessageQueue<TestMessage>>();
    }

    public ValueTask DisposeAsync() => provider.DisposeAsync();

    MessageQueueHostedService<TestMessage> Service => provider.GetServices<IHostedService>().OfType<MessageQueueHostedService<TestMessage>>().Single();

    async Task RunAsync(Func<Task> publish)
    {
        await Service.StartAsync(TestContext.Current.CancellationToken);
        await publish();
        await queue.WaitForIdleAsync(TestContext.Current.CancellationToken);
        await Service.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Processing_Several_Messages()
    {
        await RunAsync(() => queue.PublishAsync(
            [new TestMessage("1", "a"), new TestMessage("2", "b"), new TestMessage("3", "c")], TestContext.Current.CancellationToken));

        var calls = script.Calls.ToArray();
        calls.Should().HaveCount(3, "it should process every message");
        calls.Select(c => c.Processor).Distinct().Should().HaveCount(3, "it should resolve a new scoped processor for each message");
        calls.Select(c => c.Probe).Distinct().Should().HaveCount(3, "it should create a new DI scope for each message");
        script.DisposedProbes.Should().Be(3, "it should dispose each message's scope when the message is done");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Processor_Acknowledges()
    {
        await RunAsync(() => queue.PublishAsync(new TestMessage("1", "a"), TestContext.Current.CancellationToken));

        queue.ProcessedMessages.Should().ContainSingle("it should acknowledge the message");
        script.Calls.Should().ContainSingle("it should not redeliver an acknowledged message");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Processor_Asks_For_A_Retry()
    {
        script.Behavior = (_, context) => context.DeliveryAttempt == 1 ? MessageResult.Retry : MessageResult.Acknowledge;

        await RunAsync(() => queue.PublishAsync(new TestMessage("1", "a"), TestContext.Current.CancellationToken));

        script.Calls.Select(c => c.Context.DeliveryAttempt).Should().Equal([1, 2],
            "it should redeliver the message as the next attempt");
        queue.ProcessedMessages.Should().ContainSingle("it should acknowledge the retried message");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Processor_Asks_For_A_DeadLetter()
    {
        script.Behavior = (_, _) => MessageResult.DeadLetter;

        await RunAsync(() => queue.PublishAsync(new TestMessage("1", "a"), TestContext.Current.CancellationToken));

        script.Calls.Should().ContainSingle("it should not retry a dead-lettered message");
        queue.DeadLetters.Should().ContainSingle("it should dead-letter the message")
            .Which.Reason.Should().Be(DeadLetterReasons.Rejected, "it should record that the processor rejected it");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Processor_Throws()
    {
        script.Behavior = (_, context) => context.DeliveryAttempt == 1 ? throw new TimeoutException() : MessageResult.Acknowledge;

        await RunAsync(() => queue.PublishAsync(new TestMessage("1", "a"), TestContext.Current.CancellationToken));

        script.Calls.Should().HaveCount(2, "it should treat an exception as a retry");
        queue.ProcessedMessages.Should().ContainSingle("it should acknowledge the message once processing succeeds");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Processor_Reads_The_Context()
    {
        await RunAsync(() => queue.PublishAsync(new TestMessage("1", "a"),
            new PublishOptions { MessageId = "order-1", CorrelationId = "checkout-1" }, TestContext.Current.CancellationToken));

        var context = script.Calls.Single().Context;
        context.MessageId.Should().Be("order-1", "it should pass the real message id, not a fabricated one");
        context.CorrelationId.Should().Be("checkout-1", "it should pass the real correlation id");
        context.DeliveryAttempt.Should().Be(1, "it should pass the real delivery attempt");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Service_Stops()
    {
        await Service.StartAsync(TestContext.Current.CancellationToken);

        await Service.StopAsync(TestContext.Current.CancellationToken);

        queue.IsSubscribed.Should().BeFalse("it should unsubscribe when the host stops");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_No_Processor_Is_Registered()
    {
        await using var context = new Given_a_message_queue_hosted_service(registerProcessor: false);

        await context.Service.StartAsync(TestContext.Current.CancellationToken);
        await context.Service.StopAsync(TestContext.Current.CancellationToken);

        context.queue.IsSubscribed.Should().BeFalse("it should not consume messages nobody can process");
        context.logger.Inner.Entries.Should().Contain(e => e.Level == LogLevel.Warning && e.Message.Contains(nameof(TestMessage)),
            "it should warn that no processor is registered for the message type");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_The_Scope_Fails_To_Dispose_After_Success()
    {
        await using var context = new Given_a_message_queue_hosted_service(registerProcessor: true, failingDisposal: true);

        await context.RunAsync(() => context.queue.PublishAsync(new TestMessage("1", "a"), TestContext.Current.CancellationToken));

        context.script.Calls.Should().ContainSingle("it should not retry a message the processor already handled");
        context.queue.ProcessedMessages.Should().ContainSingle("it should keep the processor's Acknowledge");
        context.logger.Inner.Entries.Should().Contain(e => e.Level == LogLevel.Warning && e.Exception is InvalidOperationException,
            "it should log the disposal failure instead");
    }
}
