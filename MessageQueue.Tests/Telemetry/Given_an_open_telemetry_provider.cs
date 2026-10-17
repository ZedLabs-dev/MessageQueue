using System.Diagnostics;
using AwesomeAssertions;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Extensions;
using ESE.MessageQueue.Telemetry;
using ESE.MessageQueue.Telemetry.OpenTelemetry;
using Microsoft.Extensions.DependencyInjection;

namespace ESE.MessageQueue.Tests.Telemetry;

[Trait("Category", "Unit")]
public class Given_an_open_telemetry_provider
{
    static readonly TelemetryTarget Target = new("test", "test-queue", "TestMessage");
    static readonly MessageContext Message = new() { MessageId = "msg-1" };

    [Fact]
    public void When_Creating_Provider_With_Source_Name()
    {
        var provider = new OpenTelemetryProvider("ESE.MessageQueue");

        provider.Should().NotBeNull("it should create the provider");
    }

    [Fact]
    public void When_Starting_Publish_Activity_Without_Listener()
    {
        var provider = new OpenTelemetryProvider("ESE.MessageQueue.Tests.NoListener");

        using var activity = provider.StartPublishActivity(Target, 1, null);

        activity.Should().BeNull("it should return null when no listener is registered");
    }

    [Fact]
    public void When_Starting_Publish_Activity_With_Listener()
    {
        const string sourceName = "ESE.MessageQueue.Tests.WithListener";
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == sourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
        };
        ActivitySource.AddActivityListener(listener);
        var provider = new OpenTelemetryProvider(sourceName);

        using var activity = provider.StartPublishActivity(Target, 1, null);

        activity.Should().NotBeNull("it should create an activity when listener is registered");
        activity!.OperationName.Should().Be("publish", "it should set operation name");
    }

    [Fact]
    public void When_Recording_Metrics()
    {
        var provider = new OpenTelemetryProvider("ESE.MessageQueue.Tests");

        var act = () =>
        {
            provider.RecordMessagePublished(Target, 1, TimeSpan.FromMilliseconds(3), null);
            provider.RecordMessageConsumed(Target, TimeSpan.FromMilliseconds(5), null);
            provider.RecordMessageRetried(Target, nextAttempt: 2);
            provider.RecordMessageDeadLettered(Target, "Rejected");
        };

        act.Should().NotThrow("it should record metrics without error");
    }

    [Fact]
    public void When_Registering_Telemetry_Via_DI()
    {
        var services = new ServiceCollection();

        services.AddMessageQueueTelemetry("Test.Source");
        var provider = services.BuildServiceProvider();

        var telemetry = provider.GetService<ITelemetryProvider>();
        telemetry.Should().NotBeNull("it should register the telemetry provider");
        telemetry.Should().BeOfType<OpenTelemetryProvider>("it should use OpenTelemetry by default");
    }

    [Fact]
    public void When_Registering_Custom_Telemetry_Provider()
    {
        var services = new ServiceCollection();

        services.AddSingleton<ITelemetryProvider>(NullTelemetryProvider.Instance);
        var provider = services.BuildServiceProvider();

        var telemetry = provider.GetService<ITelemetryProvider>();
        telemetry.Should().Be(NullTelemetryProvider.Instance, "it should use the custom provider");
    }

    [Fact]
    public void When_Setting_An_Error_Through_A_Base_Reference()
    {
        using var capture = new ActivityCapture("ESE.MessageQueue.Tests.BaseReference");
        var provider = new OpenTelemetryProvider(capture.SourceName);

        using (TelemetryActivity activity = provider.StartConsumeActivity(Target, Message, null)!)
        {
            activity.SetError(new InvalidOperationException("boom"));
        }

        capture.Single.Status.Should().Be(ActivityStatusCode.Error,
            "it should mark the span as failed even when called through the TelemetryActivity base type");
    }

    [Fact]
    public void When_Recording_An_Error_By_Default()
    {
        using var capture = new ActivityCapture("ESE.MessageQueue.Tests.DefaultErrorDetails");
        var provider = new OpenTelemetryProvider(capture.SourceName);

        using (var activity = provider.StartConsumeActivity(Target, Message, null)!)
        {
            activity.SetError(ThrownException("secret customer data"));
        }

        var span = capture.Single;
        var exceptionEvent = span.Events.Should().ContainSingle(e => e.Name == "exception",
            "it should record one exception event").Subject;
        exceptionEvent.Tags.Should().Contain(new KeyValuePair<string, object?>("exception.type", typeof(InvalidOperationException).FullName),
            "it should always record the exception type");
        exceptionEvent.Tags.Select(t => t.Key).Should().NotContain(["exception.message", "exception.stacktrace"],
            "it should not record the exception message or stack trace unless opted in");
        span.StatusDescription.Should().NotContain("secret", "it should not leak the exception message into the span status");
    }

    [Fact]
    public void When_Exception_Details_Are_Opted_In()
    {
        using var capture = new ActivityCapture("ESE.MessageQueue.Tests.OptInErrorDetails");
        var provider = new OpenTelemetryProvider(capture.SourceName, recordExceptionDetails: true);

        using (var activity = provider.StartConsumeActivity(Target, Message, null)!)
        {
            activity.SetError(ThrownException("diagnostic detail"));
        }

        var exceptionEvent = capture.Single.Events.Single(e => e.Name == "exception");
        exceptionEvent.Tags.Should().Contain(new KeyValuePair<string, object?>("exception.message", "diagnostic detail"),
            "it should record the message when the host opts in");
        exceptionEvent.Tags.Select(t => t.Key).Should().Contain("exception.stacktrace",
            "it should record the stack trace when the host opts in");
    }

    [Fact]
    public void When_Opted_In_Exception_Details_Are_Long()
    {
        using var capture = new ActivityCapture("ESE.MessageQueue.Tests.LongErrorDetails");
        var provider = new OpenTelemetryProvider(capture.SourceName, recordExceptionDetails: true);

        using (var activity = provider.StartConsumeActivity(Target, Message, null)!)
        {
            activity.SetError(ThrownException(new string('x', 20_000)));
        }

        var span = capture.Single;
        var tags = span.Events.Single(e => e.Name == "exception").Tags.ToDictionary(t => t.Key, t => t.Value as string);
        tags["exception.message"]!.Length.Should().BeLessThanOrEqualTo(OpenTelemetryActivity.MaxRecordedLength,
            "it should truncate a long exception message");
        tags["exception.stacktrace"]!.Length.Should().BeLessThanOrEqualTo(OpenTelemetryActivity.MaxRecordedLength,
            "it should truncate a long stack trace");
        span.StatusDescription!.Length.Should().BeLessThanOrEqualTo(OpenTelemetryActivity.MaxRecordedLength,
            "it should truncate the status description");
    }

    [Fact]
    public void When_Starting_A_Publish_Span()
    {
        using var spans = new SpanCapture();
        var provider = new OpenTelemetryProvider(spans.SourceName);

        provider.StartPublishActivity(Target, 3, null)!.Dispose();

        var span = spans.Single(ActivityKind.Producer);
        span.Kind.Should().Be(ActivityKind.Producer, "it should mark a publish as a producer span");
        span.DisplayName.Should().Be("publish test-queue", "it should name the span after the operation and destination");
        span.GetTagItem("messaging.system").Should().Be("test", "it should name the messaging system of the provider");
        span.GetTagItem("messaging.destination.name").Should().Be("test-queue", "it should name the destination");
        span.GetTagItem("messaging.operation.type").Should().Be("send", "it should use the semantic conventions' operation type");
        span.GetTagItem("messaging.batch.message_count").Should().Be(3, "it should record how many messages a batch publish sent");
    }

    [Fact]
    public void When_Starting_A_Consume_Span_With_A_Parent()
    {
        using var spans = new SpanCapture();
        var provider = new OpenTelemetryProvider(spans.SourceName);
        var parent = TraceContext.Parse("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", "rojo=00f067aa0ba902b7");
        var context = new MessageContext { MessageId = "msg-7", CorrelationId = "order-42", DeliveryAttempt = 3 };

        provider.StartConsumeActivity(Target, context, parent)!.Dispose();

        var span = spans.Single(ActivityKind.Consumer);
        span.Kind.Should().Be(ActivityKind.Consumer, "it should mark a delivery as a consumer span");
        span.TraceId.ToHexString().Should().Be("0af7651916cd43dd8448eb211c80319c", "it should continue the publisher's trace");
        span.ParentSpanId.ToHexString().Should().Be("b7ad6b7169203331", "it should be a child of the publish span");
        span.HasRemoteParent.Should().BeTrue("it should mark the parent as coming from another process");
        span.TraceStateString.Should().Be("rojo=00f067aa0ba902b7", "it should carry the vendor trace state");
        span.GetTagItem("messaging.operation.type").Should().Be("process", "it should use the semantic conventions' operation type");
        span.GetTagItem("messaging.message.id").Should().Be("msg-7", "it should record the message id");
        span.GetTagItem("messaging.message.conversation_id").Should().Be("order-42", "it should record the correlation id");
        span.GetTagItem("ese.messagequeue.delivery_attempt").Should().Be(3, "it should record which attempt this is");
    }

    [Fact]
    public void When_Injecting_Inside_A_Publish_Span()
    {
        using var spans = new SpanCapture();
        var provider = new OpenTelemetryProvider(spans.SourceName);
        var headers = new Dictionary<string, string>();

        using (provider.StartPublishActivity(Target, 1, null))
        {
            Activity.Current!.TraceStateString = "congo=t61rcWkgMzE";
            provider.InjectTraceContext(headers);
        }

        var span = spans.Single(ActivityKind.Producer);
        headers.Should().Contain("traceparent", $"00-{span.TraceId}-{span.SpanId}-01", "it should propagate the publish span as the parent");
        headers.Should().Contain("tracestate", "congo=t61rcWkgMzE", "it should propagate the vendor trace state");
    }

    [Fact]
    public void When_Injecting_Without_A_Span()
    {
        var provider = new OpenTelemetryProvider("ESE.MessageQueue.Tests.NoSpan");
        var headers = new Dictionary<string, string>();
        using var suppressed = new SuppressedCurrent();

        provider.InjectTraceContext(headers);

        headers.Should().BeEmpty("it should not invent a trace context when there is no current span");
    }

    [Fact]
    public void When_Tagging_A_Span()
    {
        using var spans = new SpanCapture();
        var provider = new OpenTelemetryProvider(spans.SourceName);

        using (var activity = provider.StartConsumeActivity(Target, Message, null)!)
        {
            activity.SetTag("ese.messagequeue.dead_letter.reason", "Rejected");
        }

        spans.Single(ActivityKind.Consumer).GetTagItem("ese.messagequeue.dead_letter.reason").Should().Be("Rejected",
            "it should pass tags through to the span");
    }

    [Fact]
    public void When_Recording_A_Publish()
    {
        var source = $"ESE.MessageQueue.Tests.{Guid.NewGuid():N}";
        using var metrics = new MetricCapture(source);
        var provider = new OpenTelemetryProvider(source);

        provider.RecordMessagePublished(Target, 3, TimeSpan.FromMilliseconds(250), null);
        provider.RecordMessagePublished(Target, 1, TimeSpan.FromMilliseconds(10), "System.TimeoutException");

        var sent = metrics.For("messaging.client.sent.messages");
        sent.Sum(m => m.Value).Should().Be(4, "it should count every published message");
        sent[0].Tags.Should().Contain("messaging.system", "test", "it should tag the messaging system");
        sent[0].Tags.Should().Contain("messaging.destination.name", "test-queue", "it should tag the destination");
        sent[0].Tags.Should().Contain("messaging.operation.name", "publish", "it should tag the operation");
        sent[0].Tags.Should().NotContainKey("error.type", "it should not tag a successful publish with an error");
        sent[1].Tags.Should().Contain("error.type", "System.TimeoutException", "it should tag a failed publish with the error type");
        var duration = metrics.For("messaging.client.operation.duration");
        duration.Select(m => m.Value).Should().Equal([0.25, 0.01], "it should record the publish duration in seconds");
        metrics.Instruments["messaging.client.operation.duration"].Unit.Should().Be("s", "it should use seconds, as the conventions require");
    }

    [Fact]
    public void When_Recording_A_Consume()
    {
        var source = $"ESE.MessageQueue.Tests.{Guid.NewGuid():N}";
        using var metrics = new MetricCapture(source);
        var provider = new OpenTelemetryProvider(source);

        provider.RecordMessageConsumed(Target, TimeSpan.FromMilliseconds(500), null);
        provider.RecordMessageConsumed(Target, TimeSpan.FromMilliseconds(20), "Rejected");

        metrics.Sum("messaging.client.consumed.messages").Should().Be(2, "it should count every delivery");
        var processing = metrics.For("messaging.process.duration");
        processing.Select(m => m.Value).Should().Equal([0.5, 0.02], "it should record the processing duration in seconds");
        processing[1].Tags.Should().Contain("error.type", "Rejected", "it should tag an unsuccessful delivery with its outcome");
        processing[0].Tags.Should().Contain("messaging.operation.name", "process", "it should tag the operation");
    }

    [Fact]
    public void When_Recording_Retries_And_Dead_Letters()
    {
        var source = $"ESE.MessageQueue.Tests.{Guid.NewGuid():N}";
        using var metrics = new MetricCapture(source);
        var provider = new OpenTelemetryProvider(source);

        provider.RecordMessageRetried(Target, nextAttempt: 2);
        provider.RecordMessageDeadLettered(Target, "MaxRetriesExceeded");

        metrics.Sum("ese.messagequeue.messages.retried").Should().Be(1, "it should count the retry");
        metrics.For("ese.messagequeue.messages.dead_lettered").Should().ContainSingle("it should count the dead letter")
            .Which.Tags.Should().Contain("ese.messagequeue.dead_letter.reason", "MaxRetriesExceeded", "it should tag the reason code");
    }

    [Fact]
    public void When_The_Parent_Is_Not_Sampled()
    {
        using var spans = new SpanCapture();
        var provider = new OpenTelemetryProvider(spans.SourceName);
        var parent = TraceContext.Parse("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00", null);

        provider.StartConsumeActivity(Target, Message, parent)!.Dispose();

        spans.Single(ActivityKind.Consumer).ParentId.Should().Be("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00",
            "it should keep an unsampled parent unsampled, so the sampler can honour the publisher's decision");
    }

    [Fact]
    public void When_Injecting_Inside_An_Unrecorded_Span()
    {
        var source = $"ESE.MessageQueue.Tests.{Guid.NewGuid():N}";
        using var listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == source,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
        };
        ActivitySource.AddActivityListener(listener);
        var provider = new OpenTelemetryProvider(source);
        var headers = new Dictionary<string, string>();

        using (provider.StartPublishActivity(Target, 1, null))
        {
            provider.InjectTraceContext(headers);
        }

        headers["traceparent"].Should().EndWith("-00", "it should propagate that the publish span was not sampled");
    }

    [Fact]
    public void When_Instruments_Are_Created()
    {
        var source = $"ESE.MessageQueue.Tests.{Guid.NewGuid():N}";
        using var metrics = new MetricCapture(source);
        using var provider = new OpenTelemetryProvider(source);

        metrics.Instruments.ToDictionary(i => i.Key, i => i.Value.Unit).Should().BeEquivalentTo(new Dictionary<string, string?>
        {
            ["messaging.client.sent.messages"] = "{message}",
            ["messaging.client.operation.duration"] = "s",
            ["messaging.client.consumed.messages"] = "{message}",
            ["messaging.process.duration"] = "s",
            ["ese.messagequeue.messages.retried"] = "{message}",
            ["ese.messagequeue.messages.dead_lettered"] = "{message}"
        }, "it should use the semantic conventions' units");
    }

    [Fact]
    public void When_Metrics_Are_Tagged()
    {
        var source = $"ESE.MessageQueue.Tests.{Guid.NewGuid():N}";
        using var metrics = new MetricCapture(source);
        using var provider = new OpenTelemetryProvider(source);
        string[] common = ["messaging.system", "messaging.destination.name", "messaging.operation.name", "ese.messagequeue.message_type"];

        provider.RecordMessagePublished(Target, 1, TimeSpan.FromMilliseconds(1), null);
        provider.RecordMessageConsumed(Target, TimeSpan.FromMilliseconds(1), "Retry");
        provider.RecordMessageRetried(Target, 2);
        provider.RecordMessageDeadLettered(Target, "Rejected");

        metrics.For("messaging.client.sent.messages").Single().Tags.Keys.Should().BeEquivalentTo(common, "it should tag a publish with the low-cardinality attributes only");
        metrics.For("messaging.client.operation.duration").Single().Tags.Keys.Should().BeEquivalentTo(common, "it should tag the publish duration the same way");
        metrics.For("messaging.client.consumed.messages").Single().Tags.Keys.Should().BeEquivalentTo([.. common, "error.type"], "it should add the error type of a failed delivery");
        metrics.For("messaging.process.duration").Single().Tags.Keys.Should().BeEquivalentTo([.. common, "error.type"], "it should tag the processing duration like the count");
        metrics.For("ese.messagequeue.messages.retried").Single().Tags.Keys.Should().BeEquivalentTo(common, "it should not tag retries with the attempt");
        metrics.For("ese.messagequeue.messages.dead_lettered").Single().Tags.Keys.Should().BeEquivalentTo([.. common, "ese.messagequeue.dead_letter.reason"],
            "it should tag dead letters with the reason and nothing per message");
    }

    [Fact]
    public void When_Consuming_From_A_Subscription()
    {
        using var spans = new SpanCapture();
        using var metrics = new MetricCapture(spans.SourceName);
        using var provider = new OpenTelemetryProvider(spans.SourceName);
        var target = new TelemetryTarget("servicebus", "events", "TestMessage") { Subscription = "billing" };

        provider.StartConsumeActivity(target, Message, null)!.Dispose();
        provider.RecordMessageConsumed(target, TimeSpan.FromMilliseconds(1), null);

        var span = spans.Single(ActivityKind.Consumer);
        span.DisplayName.Should().Be("process events", "it should name the span after the topic");
        span.GetTagItem("messaging.destination.subscription.name").Should().Be("billing", "it should name the subscription");
        metrics.For("messaging.process.duration").Single().Tags.Should().Contain("messaging.destination.subscription.name", "billing",
            "it should tag consumer metrics with the subscription");
    }

    [Fact]
    public void When_The_System_Calls_Publishing_Send()
    {
        using var spans = new SpanCapture();
        using var metrics = new MetricCapture(spans.SourceName);
        using var provider = new OpenTelemetryProvider(spans.SourceName);
        var target = new TelemetryTarget("servicebus", "orders", "TestMessage") { PublishOperation = "send" };

        provider.StartPublishActivity(target, 1, "m-9")!.Dispose();
        provider.RecordMessagePublished(target, 1, TimeSpan.FromMilliseconds(1), null);

        var span = spans.Single(ActivityKind.Producer);
        span.DisplayName.Should().Be("send orders", "it should name the span after the system's operation");
        span.GetTagItem("messaging.operation.name").Should().Be("send", "it should use the system's operation name");
        span.GetTagItem("messaging.message.id").Should().Be("m-9", "it should record the id of a single published message");
        metrics.For("messaging.client.sent.messages").Single().Tags.Should().Contain("messaging.operation.name", "send",
            "it should tag the metric with the same operation name");
    }

    [Fact]
    public void When_Publishing_A_Batch_Span()
    {
        using var spans = new SpanCapture();
        using var provider = new OpenTelemetryProvider(spans.SourceName);

        provider.StartPublishActivity(Target, 3, null)!.Dispose();

        spans.Single(ActivityKind.Producer).GetTagItem("messaging.message.id").Should().BeNull("it should not name one message on a batch span");
    }

    [Fact]
    public void When_The_Target_Has_A_Routing_Key()
    {
        using var spans = new SpanCapture();
        using var provider = new OpenTelemetryProvider(spans.SourceName);
        var target = new TelemetryTarget("rabbitmq", "orders-exchange:orders", "TestMessage") { RoutingKey = "orders" };

        provider.StartPublishActivity(target, 1, null)!.Dispose();
        provider.StartConsumeActivity(target, Message, null)!.Dispose();

        spans.Spans.Should().HaveCount(2).And.AllSatisfy(s =>
            s.GetTagItem("messaging.rabbitmq.destination.routing_key").Should().Be("orders", "it should record the RabbitMQ routing key"));
    }

    [Fact]
    public void When_An_Error_Is_Set()
    {
        using var spans = new SpanCapture();
        using var provider = new OpenTelemetryProvider(spans.SourceName);

        using (var activity = provider.StartConsumeActivity(Target, Message, null)!)
        {
            activity.SetError(new TimeoutException("secret"));
        }

        spans.Single(ActivityKind.Consumer).GetTagItem("error.type").Should().Be(typeof(TimeoutException).FullName,
            "it should set error.type on the span, as on the metrics");
    }

    [Fact]
    public void When_A_Span_Fails_Without_An_Exception()
    {
        using var spans = new SpanCapture();
        using var provider = new OpenTelemetryProvider(spans.SourceName);

        using (var activity = provider.StartConsumeActivity(Target, Message, null)!)
        {
            activity.SetErrorType("Rejected");
        }

        var span = spans.Single(ActivityKind.Consumer);
        span.Status.Should().Be(ActivityStatusCode.Error, "it should mark an unsuccessful delivery as failed");
        span.GetTagItem("error.type").Should().Be("Rejected", "it should record the outcome as the error type");
    }

    [Fact]
    public void When_Nothing_Listens()
    {
        using var provider = new OpenTelemetryProvider($"ESE.MessageQueue.Tests.{Guid.NewGuid():N}");

        provider.ExtractTraceContext(new Dictionary<string, string> { ["traceparent"] = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01" })
            .Should().BeNull("it should not parse trace headers for spans nobody records");
    }

    [Fact]
    public async Task When_The_Container_Is_Disposed()
    {
        using var spans = new SpanCapture();
        var services = new ServiceCollection();
        services.AddMessageQueueTelemetry(spans.SourceName);
        var container = services.BuildServiceProvider();
        var provider = container.GetRequiredService<ITelemetryProvider>();

        await container.DisposeAsync();

        provider.StartPublishActivity(Target, 1, null).Should().BeNull("it should dispose the ActivitySource and Meter with the container");
    }

    // Clears Activity.Current for the test and restores it afterwards.
    sealed class SuppressedCurrent : IDisposable
    {
        readonly Activity? previous = Activity.Current;

        public SuppressedCurrent() => Activity.Current = null;

        public void Dispose() => Activity.Current = previous;
    }

    static InvalidOperationException ThrownException(string message)
    {
        try
        {
            throw new InvalidOperationException(message);
        }
        catch (InvalidOperationException ex)
        {
            return ex;
        }
    }

    sealed class ActivityCapture : IDisposable
    {
        readonly ActivityListener listener;
        readonly List<Activity> stopped = [];

        public ActivityCapture(string sourceName)
        {
            SourceName = sourceName;
            listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == sourceName,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = stopped.Add
            };
            ActivitySource.AddActivityListener(listener);
        }

        public string SourceName { get; }

        public Activity Single => stopped.Should().ContainSingle("it should have produced one span").Subject;

        public void Dispose() => listener.Dispose();
    }
}
