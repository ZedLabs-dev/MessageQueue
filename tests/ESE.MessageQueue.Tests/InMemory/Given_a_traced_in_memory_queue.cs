using System.Diagnostics;
using AwesomeAssertions;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Extensions;
using ESE.MessageQueue.InMemory;
using ESE.MessageQueue.Processing;
using ESE.MessageQueue.Retry;
using ESE.MessageQueue.Telemetry;
using ESE.MessageQueue.Telemetry.OpenTelemetry;
using ESE.MessageQueue.Tests.Telemetry;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace ESE.MessageQueue.Tests.InMemory;

// B10 end to end on the in-memory provider: one trace from publish to every delivery attempt, with metrics.
[Trait("Category", "Unit")]
public sealed class Given_a_traced_in_memory_queue : IAsyncDisposable
{
    static readonly RetryPolicy FastRetry = new()
    {
        MaxRetries = 2,
        InitialDelay = TimeSpan.FromMilliseconds(5),
        MaxDelay = TimeSpan.FromMilliseconds(20),
        UseJitter = false
    };

    readonly SpanCapture spans = new();
    readonly MetricCapture metrics;
    readonly InMemoryMessageQueue<TestMessage> queue;

    public Given_a_traced_in_memory_queue()
    {
        metrics = new MetricCapture(spans.SourceName);
        queue = new InMemoryMessageQueue<TestMessage>(
            new InMemoryConfig { QueueName = "orders", Retry = FastRetry }, new OpenTelemetryProvider(spans.SourceName));
    }

    public async ValueTask DisposeAsync()
    {
        await queue.DisposeAsync();
        metrics.Dispose();
        spans.Dispose();
    }

    async Task RunAsync(Func<TestMessage, MessageContext, CancellationToken, Task<MessageResult>> handler, CancellationToken cancellationToken, int expectedConsumes = 1)
    {
        await queue.SubscribeAsync(handler, cancellationToken);
        await queue.PublishAsync(new TestMessage("order-1", "content"), cancellationToken);
        await queue.WaitForIdleAsync(cancellationToken);
        await spans.WaitForAsync(ActivityKind.Consumer, expectedConsumes);
    }

    [Fact(Timeout = 10_000)]
    public async Task When_A_Message_Is_Published_And_Consumed()
    {
        await RunAsync((_, _, _) => Task.FromResult(MessageResult.Acknowledge), TestContext.Current.CancellationToken);

        var publish = spans.Single(ActivityKind.Producer);
        var consume = spans.Single(ActivityKind.Consumer);
        consume.TraceId.Should().Be(publish.TraceId, "it should continue the publisher's trace in the consumer");
        consume.ParentSpanId.Should().Be(publish.SpanId, "it should make the publish span the parent of the consume span");
        publish.GetTagItem("messaging.system").Should().Be("ese.inmemory", "it should name the in-memory provider");
        consume.GetTagItem("messaging.system").Should().Be("ese.inmemory", "it should name the in-memory provider");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_A_Message_Is_Published_Inside_An_Application_Span()
    {
        using var application = new ActivitySource(spans.SourceName).StartActivity("place order");
        await RunAsync((_, _, _) => Task.FromResult(MessageResult.Acknowledge), TestContext.Current.CancellationToken);

        var publish = spans.Single(ActivityKind.Producer);
        publish.ParentSpanId.Should().Be(application!.SpanId, "it should nest the publish span under the caller's span");
        spans.Single(ActivityKind.Consumer).TraceId.Should().Be(application.TraceId, "it should carry the caller's trace to the consumer");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_A_Message_Is_Retried_Then_Dead_Lettered()
    {
        await RunAsync((_, _, _) => Task.FromResult(MessageResult.Retry), TestContext.Current.CancellationToken, expectedConsumes: 3);

        var publish = spans.Single(ActivityKind.Producer);
        var consumes = spans.OfKind(ActivityKind.Consumer);
        consumes.Should().HaveCount(3, "it should trace the first attempt and both retries");
        consumes.Should().AllSatisfy(c => c.ParentSpanId.Should().Be(publish.SpanId, "it should keep the original trace across retries"));
        consumes.Select(c => c.GetTagItem("ese.messagequeue.delivery_attempt")).Should().BeEquivalentTo([1, 2, 3],
            "it should record each attempt");
        consumes.Single(c => (int)c.GetTagItem("ese.messagequeue.delivery_attempt")! == 3)
            .GetTagItem("ese.messagequeue.dead_letter.reason").Should().Be(DeadLetterReasons.MaxRetriesExceeded,
                "it should record why the last attempt was dead-lettered");
        metrics.Sum("ese.messagequeue.messages.retried").Should().Be(2, "it should count both retries");
        metrics.For("ese.messagequeue.messages.dead_lettered").Should().ContainSingle("it should count the dead letter")
            .Which.Tags.Should().Contain("ese.messagequeue.dead_letter.reason", DeadLetterReasons.MaxRetriesExceeded,
                "it should tag the dead letter with its reason");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_A_Batch_Is_Published()
    {
        await queue.SubscribeAsync((_, _, _) => Task.FromResult(MessageResult.Acknowledge), TestContext.Current.CancellationToken);

        await queue.PublishAsync([new TestMessage("1", "a"), new TestMessage("2", "b"), new TestMessage("3", "c")], TestContext.Current.CancellationToken);
        await queue.WaitForIdleAsync(TestContext.Current.CancellationToken);
        var consumes = await spans.WaitForAsync(ActivityKind.Consumer, 3);

        var publish = spans.Single(ActivityKind.Producer);
        publish.GetTagItem("messaging.batch.message_count").Should().Be(3, "it should trace a batch as one publish span");
        consumes.Should().HaveCount(3).And.AllSatisfy(c => c.ParentSpanId.Should().Be(publish.SpanId, "it should parent every message of the batch on it"));
        metrics.For("messaging.client.operation.duration").Should().ContainSingle("it should record one duration for the batch");
        metrics.Sum("messaging.client.sent.messages").Should().Be(3, "it should count every message of the batch");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_A_Message_Is_Rejected()
    {
        await RunAsync((_, _, _) => Task.FromResult(MessageResult.DeadLetter), TestContext.Current.CancellationToken);

        var consume = spans.Single(ActivityKind.Consumer);
        consume.Status.Should().Be(ActivityStatusCode.Error, "it should mark a dead-lettered delivery as failed");
        consume.GetTagItem("error.type").Should().Be(DeadLetterReasons.Rejected, "it should record the same error type as the metrics");
        metrics.For("messaging.process.duration").Single().Tags.Should().Contain("error.type", DeadLetterReasons.Rejected,
            "it should report the outcome to the metrics");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Metrics_Are_Collected()
    {
        await RunAsync((_, _, _) => Task.FromResult(MessageResult.Acknowledge), TestContext.Current.CancellationToken);

        metrics.Sum("messaging.client.sent.messages").Should().Be(1, "it should count the published message");
        metrics.For("messaging.client.operation.duration").Should().ContainSingle("it should record the publish duration");
        metrics.Sum("messaging.client.consumed.messages").Should().Be(1, "it should count the delivery");
        metrics.For("messaging.process.duration").Should().ContainSingle("it should record the processing duration")
            .Which.Tags.Should().Contain("messaging.destination.name", "orders", "it should tag the queue");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Telemetry_Fails_On_Publish()
    {
        var telemetry = new ThrowingTelemetryProvider();
        await using var broken = new InMemoryMessageQueue<TestMessage>(new InMemoryConfig { QueueName = "orders", Retry = FastRetry }, telemetry);
        await broken.SubscribeAsync((_, _, _) => Task.FromResult(MessageResult.Acknowledge), TestContext.Current.CancellationToken);

        await broken.PublishAsync(new TestMessage("order-1", "content"), TestContext.Current.CancellationToken);
        await broken.WaitForIdleAsync(TestContext.Current.CancellationToken);

        broken.ProcessedMessages.Should().ContainSingle("it should publish and deliver the message although every telemetry call throws");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Only_Injection_Fails()
    {
        var telemetry = new InjectionFailingTelemetryProvider();
        await using var broken = new InMemoryMessageQueue<TestMessage>(new InMemoryConfig { QueueName = "orders", Retry = FastRetry }, telemetry);
        await broken.SubscribeAsync((_, _, _) => Task.FromResult(MessageResult.Acknowledge), TestContext.Current.CancellationToken);

        await broken.PublishAsync(new TestMessage("order-1", "content"), TestContext.Current.CancellationToken);
        await broken.WaitForIdleAsync(TestContext.Current.CancellationToken);

        broken.ProcessedMessages.Should().ContainSingle("it should still deliver the message");
        telemetry.PublishedCount.Should().Be(1, "it should still count the publish");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Telemetry_Is_Registered_In_The_Container()
    {
        var telemetry = new TestTelemetryProvider();
        var services = new ServiceCollection();
        services.AddSingleton<ITelemetryProvider>(telemetry);
        services.AddMessageQueue<TestMessage>(new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MessageQueue:Provider"] = "InMemory",
            ["MessageQueue:QueueName"] = "orders"
        }).Build());
        await using var provider = services.BuildServiceProvider();
        var registered = provider.GetRequiredService<IMessageQueue<TestMessage>>();
        await registered.SubscribeAsync((_, _, _) => Task.FromResult(MessageResult.Acknowledge), TestContext.Current.CancellationToken);

        await registered.PublishAsync(new TestMessage("order-1", "content"), TestContext.Current.CancellationToken);
        await ((InMemoryMessageQueue<TestMessage>)registered).WaitForIdleAsync(TestContext.Current.CancellationToken);

        telemetry.Publishes.Should().ContainSingle("it should use the container's telemetry provider on publish");
        telemetry.Consumes.Should().ContainSingle("it should use the container's telemetry provider on consume")
            .Which.Parent!.ToTraceParent().Should().Be(telemetry.InjectedTraceParent, "it should carry the injected trace context to the consumer");
    }

    sealed class InjectionFailingTelemetryProvider : TestTelemetryProvider, ITelemetryProvider
    {
        void ITelemetryProvider.InjectTraceContext(IDictionary<string, string> headers) => throw new InvalidOperationException("broken propagator");
    }
}
