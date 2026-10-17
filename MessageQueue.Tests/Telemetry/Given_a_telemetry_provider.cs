using AwesomeAssertions;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Telemetry;

namespace ESE.MessageQueue.Tests.Telemetry;

[Trait("Category", "Unit")]
public class Given_a_telemetry_provider
{
    static readonly TelemetryTarget Target = new("test", "test-queue", "TestMessage");

    [Fact]
    public void When_Creating_Publish_Activity()
    {
        var provider = new TestTelemetryProvider();

        using var activity = provider.StartPublishActivity(Target, 1, null);

        activity.Should().NotBeNull("it should create an activity for publish operations");
        activity!.OperationName.Should().Be("publish", "it should set the operation name");
        activity.QueueName.Should().Be("test-queue", "it should set the queue name");
        activity.MessageType.Should().Be("TestMessage", "it should set the message type");
    }

    [Fact]
    public void When_Creating_Consume_Activity()
    {
        var provider = new TestTelemetryProvider();

        using var activity = provider.StartConsumeActivity(Target, new MessageContext { MessageId = "msg-123" }, null);

        activity.Should().NotBeNull("it should create an activity for consume operations");
        activity!.OperationName.Should().Be("consume", "it should set the operation name");
        activity.QueueName.Should().Be("test-queue", "it should set the queue name");
        activity.MessageId.Should().Be("msg-123", "it should set the message id");
    }

    [Fact]
    public void When_Recording_Message_Published()
    {
        var provider = new TestTelemetryProvider();

        provider.RecordMessagePublished(Target, 1, TimeSpan.Zero, null);

        provider.PublishedCount.Should().Be(1, "it should increment the published counter");
    }

    [Fact]
    public void When_Recording_Message_Consumed()
    {
        var provider = new TestTelemetryProvider();

        provider.RecordMessageConsumed(Target, TimeSpan.Zero, errorType: null);

        provider.ConsumedSuccessCount.Should().Be(1, "it should increment the success counter");
    }

    [Fact]
    public void When_Recording_Message_Failed()
    {
        var provider = new TestTelemetryProvider();

        provider.RecordMessageConsumed(Target, TimeSpan.Zero, errorType: "System.TimeoutException");

        provider.ConsumedFailureCount.Should().Be(1, "it should increment the failure counter");
    }

    [Fact]
    public void When_Recording_Message_Retried()
    {
        var provider = new TestTelemetryProvider();

        provider.RecordMessageRetried(Target, nextAttempt: 2);

        provider.RetriedCount.Should().Be(1, "it should increment the retry counter");
    }

    [Fact]
    public void When_Recording_Message_DeadLettered()
    {
        var provider = new TestTelemetryProvider();

        provider.RecordMessageDeadLettered(Target, "Rejected");

        provider.DeadLetteredCount.Should().Be(1, "it should increment the dead-letter counter");
    }

    [Fact]
    public void When_Using_NullTelemetryProvider()
    {
        var provider = NullTelemetryProvider.Instance;

        var activity = provider.StartPublishActivity(Target, 1, null);

        activity.Should().BeNull("it should return null for no-op provider");
    }

    [Fact]
    public void When_Extracting_Trace_Context()
    {
        var provider = new TestTelemetryProvider();
        var headers = new Dictionary<string, string>
        {
            ["traceparent"] = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        };

        var context = provider.ExtractTraceContext(headers);

        context.Should().NotBeNull("it should extract trace context from headers");
        context!.TraceId.Should().Be("0af7651916cd43dd8448eb211c80319c", "it should extract the trace id");
        context.SpanId.Should().Be("b7ad6b7169203331", "it should extract the span id");
    }

    [Fact]
    public void When_Injecting_Trace_Context()
    {
        var provider = new TestTelemetryProvider();
        var headers = new Dictionary<string, string>();

        provider.InjectTraceContext(headers);

        headers.Should().ContainKey("traceparent", "it should inject traceparent header");
    }
}
