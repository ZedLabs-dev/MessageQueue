using System.Collections.Concurrent;
using AwesomeAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using ZedLabs.MessageQueue.Abstractions;
using ZedLabs.MessageQueue.Extensions;
using ZedLabs.MessageQueue.Hosting;
using ZedLabs.MessageQueue.InMemory;
using ZedLabs.MessageQueue.Processing;
using ZedLabs.MessageQueue.Retry;

namespace ZedLabs.MessageQueue.Tests.Hosting;

[Trait("Category", "Unit")]
public sealed class Given_a_message_queue_hosted_service : IAsyncDisposable
{
    public sealed class Script
    {
        public Func<TestMessage, MessageContext, MessageResult> Behavior { get; set; } = (_, _) => MessageResult.Acknowledge;
        public Func<CancellationToken, Task>? Work { get; set; }
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
            return script.Work is { } work ? RunAsync(work, message, context, cancellationToken) : Task.FromResult(script.Behavior(message, context));
        }

        async Task<MessageResult> RunAsync(Func<CancellationToken, Task> work, TestMessage message, MessageContext context, CancellationToken cancellationToken)
        {
            await work(cancellationToken);
            return script.Behavior(message, context);
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
    public async Task When_The_Service_Stops_While_A_Message_Is_Processed()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        script.Work = async token =>
        {
            started.TrySetResult();
            try
            {
                await Task.Delay(Timeout.Infinite, token);
            }
            catch (OperationCanceledException)
            {
                cancelled.TrySetResult();
                throw;
            }
        };
        await Service.StartAsync(TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("1", "slow"), TestContext.Current.CancellationToken);
        await started.Task.WaitAsync(TestContext.Current.CancellationToken);

        await Service.StopAsync(TestContext.Current.CancellationToken);

        cancelled.Task.IsCompleted.Should().BeTrue("it should cancel the in-flight processor's token when the host stops");
        queue.IsSubscribed.Should().BeFalse("it should unsubscribe");
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
    // The host stops waiting for StopAsync after HostOptions.ShutdownTimeout; a provider drain that may take as long
    // gets cut short, so the hosted service warns at start (event 5005).
    static async Task<List<string>> StartAndStopWithAsync(TimeSpan? providerTimeout, TimeSpan? hostTimeout, CancellationToken cancellationToken)
    {
        var services = new ServiceCollection();
        var recording = new RecordingLogger<MessageQueueHostedService<TestMessage>>();
        services.AddSingleton<ILogger<MessageQueueHostedService<TestMessage>>>(recording);
        services.AddSingleton(new Script());
        services.AddScoped<ScopedProbe>();
        if (hostTimeout is { } host)
            services.Configure<HostOptions>(options => options.ShutdownTimeout = host);
        services.AddInMemoryMessageQueue<TestMessage>(config =>
        {
            if (providerTimeout is { } timeout)
                config.ShutdownTimeout = timeout;
        });
        services.AddMessageProcessor<TestMessage, ScriptedProcessor>();
        services.AddHostedService<MessageQueueHostedService<TestMessage>>();
        await using var built = services.BuildServiceProvider();
        var service = built.GetServices<IHostedService>().OfType<MessageQueueHostedService<TestMessage>>().Single();

        await service.StartAsync(cancellationToken);
        await service.StopAsync(cancellationToken);

        return recording.Inner.Entries.Where(e => e.Level == LogLevel.Warning).Select(e => e.Message).ToList();
    }

    [Theory(Timeout = 10_000)]
    [InlineData(30, 30)]
    [InlineData(45, 30)]
    public async Task When_The_Providers_ShutdownTimeout_Is_Not_Below_The_Hosts(int providerSeconds, int hostSeconds)
    {
        var warnings = await StartAndStopWithAsync(TimeSpan.FromSeconds(providerSeconds), TimeSpan.FromSeconds(hostSeconds), TestContext.Current.CancellationToken);

        warnings.Should().ContainSingle("it should warn that the host can cut the drain short")
            .Which.Should().Contain("HostOptions.ShutdownTimeout", "it should name the host setting")
            .And.Contain($"{TimeSpan.FromSeconds(providerSeconds)}", "it should name the provider's timeout")
            .And.Contain($"{TimeSpan.FromSeconds(hostSeconds)}", "it should name the host's timeout")
            .And.Contain("Lower the queue's ShutdownTimeout", "it should not name a configuration key a code-configured queue does not have");
    }

    // A host ShutdownTimeout of -1 s stands for Timeout.InfiniteTimeSpan: the host then waits as long as it takes.
    [Theory(Timeout = 10_000)]
    [InlineData(29, 30)]
    [InlineData(600, -1)]
    public async Task When_The_Providers_ShutdownTimeout_Is_Below_The_Hosts(int providerSeconds, int hostSeconds)
    {
        var host = hostSeconds < 0 ? Timeout.InfiniteTimeSpan : TimeSpan.FromSeconds(hostSeconds);

        var warnings = await StartAndStopWithAsync(TimeSpan.FromSeconds(providerSeconds), host, TestContext.Current.CancellationToken);

        warnings.Should().BeEmpty("it should not warn when the drain ends before the host stops waiting");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Both_ShutdownTimeouts_Keep_Their_Defaults()
    {
        var warnings = await StartAndStopWithAsync(providerTimeout: null, hostTimeout: null, TestContext.Current.CancellationToken);

        warnings.Should().BeEmpty("it should leave the host time beyond the provider's default drain");
    }
    [Fact(Timeout = 10_000)]
    public async Task When_The_Host_Options_Cannot_Be_Read()
    {
        var services = new ServiceCollection();
        var recording = new RecordingLogger<MessageQueueHostedService<TestMessage>>();
        services.AddSingleton<ILogger<MessageQueueHostedService<TestMessage>>>(recording);
        services.AddSingleton(new Script());
        services.AddScoped<ScopedProbe>();
        services.AddSingleton<Microsoft.Extensions.Options.IOptions<HostOptions>>(_ => throw new InvalidOperationException("broken options"));
        services.AddInMemoryMessageQueue<TestMessage>(config => config.ShutdownTimeout = TimeSpan.FromMinutes(5));
        services.AddMessageProcessor<TestMessage, ScriptedProcessor>();
        services.AddHostedService<MessageQueueHostedService<TestMessage>>();
        await using var built = services.BuildServiceProvider();
        var service = built.GetServices<IHostedService>().OfType<MessageQueueHostedService<TestMessage>>().Single();

        var start = () => service.StartAsync(TestContext.Current.CancellationToken);

        await start.Should().NotThrowAsync("it should never fail a start over shutdown advice");
        built.GetRequiredService<InMemoryMessageQueue<TestMessage>>().IsSubscribed.Should().BeTrue("it should subscribe regardless");
        recording.Inner.Entries.Should().Contain(e => e.Level == LogLevel.Debug && e.Message.Contains("HostOptions.ShutdownTimeout"),
            "it should note at Debug level that the check was skipped");
        await service.StopAsync(TestContext.Current.CancellationToken);
    }
    [Fact(Timeout = 10_000)]
    public async Task When_A_Configured_Queue_Is_Not_Below_The_Hosts_ShutdownTimeout()
    {
        var services = new ServiceCollection();
        var recording = new RecordingLogger<MessageQueueHostedService<TestMessage>>();
        services.AddSingleton<ILogger<MessageQueueHostedService<TestMessage>>>(recording);
        services.AddSingleton(new Script());
        services.AddScoped<ScopedProbe>();
        services.Configure<HostOptions>(options => options.ShutdownTimeout = TimeSpan.FromSeconds(30));
        services.AddMessageQueue<TestMessage>(new Microsoft.Extensions.Configuration.ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Queues:Orders:Provider"] = "InMemory",
            ["Queues:Orders:QueueName"] = "orders",
            ["Queues:Orders:ShutdownTimeout"] = "00:00:40"
        }).Build(), "Queues:Orders");
        services.AddMessageProcessor<TestMessage, ScriptedProcessor>();
        services.AddHostedService<MessageQueueHostedService<TestMessage>>();
        await using var built = services.BuildServiceProvider();
        var service = built.GetServices<IHostedService>().OfType<MessageQueueHostedService<TestMessage>>().Single();

        await service.StartAsync(TestContext.Current.CancellationToken);
        await service.StopAsync(TestContext.Current.CancellationToken);

        recording.Inner.Entries.Where(e => e.Level == LogLevel.Warning).Should().ContainSingle()
            .Which.Message.Should().Contain("Lower Queues:Orders:ShutdownTimeout", "it should name the queue's own configuration key");
    }

    sealed class FailingBudgetedSubscriber : IMessageSubscriber<TestMessage>, IShutdownBudget
    {
        public TimeSpan ShutdownTimeout => TimeSpan.FromSeconds(30);

        public Task SubscribeAsync(Func<TestMessage, Task<bool>> handler, CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("broker unreachable");

        public Task SubscribeAsync(Func<TestMessage, MessageContext, CancellationToken, Task<MessageResult>> handler, CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("broker unreachable");

        public Task UnsubscribeAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Subscribing_Fails_After_The_Shutdown_Budget_Was_Checked()
    {
        var services = new ServiceCollection();
        services.Configure<HostOptions>(options => options.ShutdownTimeout = TimeSpan.FromSeconds(30));
        await using var built = services.BuildServiceProvider();
        var recording = new RecordingLogger<MessageQueueHostedService<TestMessage>>();
        var service = new MessageQueueHostedService<TestMessage>(new FailingBudgetedSubscriber(), built.GetRequiredService<IServiceScopeFactory>(), recording);

        var start = () => service.StartAsync(TestContext.Current.CancellationToken);

        await start.Should().ThrowAsync<InvalidOperationException>("it should still fail the start when subscribing fails");
        recording.Inner.Entries.Should().ContainSingle(e => e.Level == LogLevel.Warning && e.Message.Contains("HostOptions.ShutdownTimeout"),
            "it should give the shutdown advice before subscribing, so a failing start still shows it");
    }
}
