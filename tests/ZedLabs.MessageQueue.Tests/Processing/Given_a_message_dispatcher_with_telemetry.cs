using System.Diagnostics;
using AwesomeAssertions;
using ZedLabs.MessageQueue.Abstractions;
using ZedLabs.MessageQueue.InMemory;
using ZedLabs.MessageQueue.Processing;
using ZedLabs.MessageQueue.Providers;
using ZedLabs.MessageQueue.Retry;
using ZedLabs.MessageQueue.Serialization;
using ZedLabs.MessageQueue.Telemetry;
using ZedLabs.MessageQueue.Telemetry.OpenTelemetry;
using ZedLabs.MessageQueue.Tests.Telemetry;

namespace ZedLabs.MessageQueue.Tests.Processing;

// B10: the shared pipeline continues the publisher's trace on every provider, and reports what happened to each delivery.
[Trait("Category", "Unit")]
public class Given_a_message_dispatcher_with_telemetry
{
    const string TraceParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    static readonly RetryPolicy TwoRetries = new()
    {
        MaxRetries = 2,
        InitialDelay = TimeSpan.FromMilliseconds(10),
        MaxDelay = TimeSpan.FromMilliseconds(10),
        UseJitter = false
    };

    readonly InMemoryConfig config = new() { QueueName = "orders", Retry = TwoRetries };
    readonly FakeDeliveryActions actions = new();
    readonly TestTelemetryProvider telemetry = new();
    readonly TestMessage message = new("order-1", "content");

    Task<DispatchOutcome> Dispatch(
        Func<TestMessage, MessageContext, CancellationToken, Task<MessageResult>> handler,
        int attempt = 1,
        string? traceParent = TraceParent,
        string? traceState = null,
        ITelemetryProvider? provider = null,
        ReadOnlyMemory<byte>? body = null) =>
        new MessageDispatcher<TestMessage>(config, new TelemetryTarget("rabbitmq", "orders", nameof(TestMessage)), handler, provider ?? telemetry).DispatchAsync(
            body ?? MessageSerializer.Serialize(message, SerializationFormat.Json),
            new MessageContext { MessageId = "m-1", DeliveryAttempt = attempt, TraceParent = traceParent, TraceState = traceState },
            actions,
            TestContext.Current.CancellationToken);

    static Func<TestMessage, MessageContext, CancellationToken, Task<MessageResult>> Result(MessageResult result) =>
        (_, _, _) => Task.FromResult(result);

    [Fact]
    public async Task When_The_Message_Carries_A_Trace_Context()
    {
        await Dispatch(Result(MessageResult.Acknowledge), traceState: "rojo=00f067aa0ba902b7");

        var span = telemetry.Consumes.Should().ContainSingle("it should start one consume span").Subject;
        span.Parent.Should().Be(TraceContext.Parse(TraceParent, "rojo=00f067aa0ba902b7"),
            "it should parent the consume span on the publisher's trace context");
        span.Target.Should().Be(new TelemetryTarget("rabbitmq", "orders", nameof(TestMessage)), "it should name the provider, queue and type");
        span.Context!.DeliveryAttempt.Should().Be(1, "it should hand the delivery's metadata to the span");
        span.Disposed.Should().BeTrue("it should end the span");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("garbage")]
    [InlineData("00-0AF7651916CD43DD8448EB211C80319C-B7AD6B7169203331-01")]
    public async Task When_The_Trace_Context_Is_Missing_Or_Malformed(string? traceParent)
    {
        var outcome = await Dispatch(Result(MessageResult.Acknowledge), traceParent: traceParent);

        outcome.Should().Be(DispatchOutcome.Completed, "it should process the message regardless of its trace headers");
        telemetry.Consumes.Should().ContainSingle("it should still trace the delivery")
            .Which.Parent.Should().BeNull("it should start a new trace instead of trusting a bad header");
    }

    [Fact]
    public async Task When_A_Delivery_Is_Retried()
    {
        await Dispatch(Result(MessageResult.Retry), attempt: 1);

        telemetry.RetriedAttempts.Should().Equal([2], "it should count the retry with the next attempt");
        telemetry.Consumes.Single().Tags.Should().Contain("zedlabs.messagequeue.retry.next_attempt", 2, "it should record the retry on the span");
        telemetry.Consumed.Single().ErrorType.Should().Be("Retry", "it should report the delivery as not acknowledged");
    }

    [Fact]
    public async Task When_A_Retried_Delivery_Arrives()
    {
        await Dispatch(Result(MessageResult.Acknowledge), attempt: 2);

        telemetry.Consumes.Single().Parent!.SpanId.Should().Be("b7ad6b7169203331",
            "it should keep the original publish span as the parent of every attempt");
    }

    [Fact]
    public async Task When_A_Delivery_Is_Dead_Lettered()
    {
        await Dispatch(Result(MessageResult.Retry), attempt: 3);

        telemetry.DeadLetterReasons.Should().Equal([DeadLetterReasons.MaxRetriesExceeded], "it should count the dead letter with its reason");
        telemetry.Consumes.Single().Tags.Should().Contain("zedlabs.messagequeue.dead_letter.reason", DeadLetterReasons.MaxRetriesExceeded,
            "it should record the dead-letter reason on the span");
    }

    [Fact]
    public async Task When_A_Message_Is_Too_Large()
    {
        await Dispatch(Result(MessageResult.Acknowledge), body: new byte[config.MaxMessageSizeBytes + 1]);

        telemetry.Consumes.Single().Tags.Should().Contain("zedlabs.messagequeue.dead_letter.reason", DeadLetterReasons.MessageTooLarge,
            "it should record why a message never reached the handler");
        telemetry.Consumed.Single().ErrorType.Should().Be(DeadLetterReasons.MessageTooLarge, "it should report the outcome");
    }

    [Fact]
    public async Task When_The_Handler_Throws()
    {
        await Dispatch((_, _, _) => throw new TimeoutException("secret"));

        telemetry.Consumed.Single().ErrorType.Should().Be(typeof(TimeoutException).FullName, "it should report the exception type, never its text");
    }

    [Theory]
    [InlineData(MessageResult.Acknowledge, null)]
    [InlineData(MessageResult.Retry, "Retry")]
    [InlineData(MessageResult.DeadLetter, DeadLetterReasons.Rejected)]
    public async Task When_A_Delivery_Ends(MessageResult result, string? errorType)
    {
        await Dispatch(Result(result));

        telemetry.Consumes.Single().ErrorType.Should().Be(errorType, "it should mark the span with the same outcome the metrics report");
        telemetry.Consumed.Single().ErrorType.Should().Be(errorType, "it should report the outcome to the metrics");
    }

    [Fact]
    public async Task When_A_Delivery_Cannot_Be_Read()
    {
        await Dispatch(Result(MessageResult.Acknowledge), body: "null"u8.ToArray());

        telemetry.Consumes.Single().ErrorType.Should().Be(DeadLetterReasons.DeserializationFailed, "it should mark the span of an unreadable message as failed");
    }

    [Fact]
    public async Task When_An_Oversized_Delivery_Arrives()
    {
        await Dispatch(Result(MessageResult.Acknowledge), body: new byte[config.MaxMessageSizeBytes + 1]);

        telemetry.Consumes.Single().ErrorType.Should().Be(DeadLetterReasons.MessageTooLarge, "it should mark the span of an oversized message as failed");
    }

    [Fact]
    public async Task When_The_Handler_Throws_On_The_Span()
    {
        await Dispatch((_, _, _) => throw new TimeoutException("secret"));

        telemetry.Consumes.Single().ErrorType.Should().Be(typeof(TimeoutException).FullName, "it should mark the span with the exception type");
    }

    [Fact]
    public async Task When_Processing_Takes_Time()
    {
        await Dispatch(async (_, _, ct) =>
        {
            await Task.Delay(50, ct);
            return MessageResult.Acknowledge;
        });

        var consumed = telemetry.Consumed.Should().ContainSingle("it should record one delivery").Subject;
        consumed.Duration.Should().BeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(45), "it should measure the processing duration");
        consumed.ErrorType.Should().BeNull("it should report an acknowledged delivery as successful");
    }

    [Fact]
    public async Task When_An_Unrelated_Span_Is_Current()
    {
        using var spans = new SpanCapture();
        var provider = new OpenTelemetryProvider(spans.SourceName);
        using var ambient = new ActivitySource(spans.SourceName).StartActivity("host startup");

        await Dispatch(Result(MessageResult.Acknowledge), traceParent: null, provider: provider);

        var consume = spans.Single(ActivityKind.Consumer);
        consume.TraceId.Should().NotBe(ambient!.TraceId, "it should not attach a delivery to whatever span happened to be current on the consumer thread");
        consume.ParentSpanId.Should().Be(default(ActivitySpanId), "it should start a new trace for a message without trace headers");
        Activity.Current.Should().BeSameAs(ambient, "it should not change the caller's current span");
    }

    [Fact]
    public async Task When_The_Handler_Runs()
    {
        using var spans = new SpanCapture();
        var provider = new OpenTelemetryProvider(spans.SourceName);
        Activity? inHandler = null;

        await Dispatch((_, _, _) =>
        {
            inHandler = Activity.Current;
            return Task.FromResult(MessageResult.Acknowledge);
        }, provider: provider);

        inHandler.Should().BeSameAs(spans.Single(ActivityKind.Consumer), "it should make the consume span current for the handler, so its own spans nest under it");
    }

    [Fact]
    public async Task When_Extracting_The_Trace_Context_Throws()
    {
        var outcome = await Dispatch(Result(MessageResult.Acknowledge), provider: new ThrowingTelemetryProvider());

        outcome.Should().Be(DispatchOutcome.Completed, "it should process the message even when telemetry cannot read its trace headers");
        actions.Calls.Should().Equal(["Complete"], "it should settle the delivery once");
    }
}
