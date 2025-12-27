using System.Diagnostics;
using ESE.MessageQueue.Extensions;
using ESE.MessageQueue.Telemetry;
using ESE.MessageQueue.Telemetry.OpenTelemetry;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;

namespace ESE.MessageQueue.Tests;

public class TelemetryTests
{
    [Fact]
    public void When_Creating_Publish_Activity()
    {
        var provider = new TestTelemetryProvider();

        using var activity = provider.StartPublishActivity("test-queue", "TestMessage");

        activity.Should().NotBeNull("it should create an activity for publish operations");
        activity!.OperationName.Should().Be("publish", "it should set the operation name");
        activity.QueueName.Should().Be("test-queue", "it should set the queue name");
        activity.MessageType.Should().Be("TestMessage", "it should set the message type");
    }

    [Fact]
    public void When_Creating_Consume_Activity()
    {
        var provider = new TestTelemetryProvider();

        using var activity = provider.StartConsumeActivity("test-queue", "TestMessage", "msg-123");

        activity.Should().NotBeNull("it should create an activity for consume operations");
        activity!.OperationName.Should().Be("consume", "it should set the operation name");
        activity.QueueName.Should().Be("test-queue", "it should set the queue name");
        activity.MessageId.Should().Be("msg-123", "it should set the message id");
    }

    [Fact]
    public void When_Recording_Message_Published()
    {
        var provider = new TestTelemetryProvider();

        provider.RecordMessagePublished("test-queue", "TestMessage");

        provider.PublishedCount.Should().Be(1, "it should increment the published counter");
    }

    [Fact]
    public void When_Recording_Message_Consumed()
    {
        var provider = new TestTelemetryProvider();

        provider.RecordMessageConsumed("test-queue", "TestMessage", success: true);

        provider.ConsumedSuccessCount.Should().Be(1, "it should increment the success counter");
    }

    [Fact]
    public void When_Recording_Message_Failed()
    {
        var provider = new TestTelemetryProvider();

        provider.RecordMessageConsumed("test-queue", "TestMessage", success: false);

        provider.ConsumedFailureCount.Should().Be(1, "it should increment the failure counter");
    }

    [Fact]
    public void When_Recording_Message_Retried()
    {
        var provider = new TestTelemetryProvider();

        provider.RecordMessageRetried("test-queue", "TestMessage", attemptNumber: 2);

        provider.RetriedCount.Should().Be(1, "it should increment the retry counter");
    }

    [Fact]
    public void When_Recording_Message_DeadLettered()
    {
        var provider = new TestTelemetryProvider();

        provider.RecordMessageDeadLettered("test-queue", "TestMessage", "Processing failed");

        provider.DeadLetteredCount.Should().Be(1, "it should increment the dead-letter counter");
    }

    [Fact]
    public void When_Using_NullTelemetryProvider()
    {
        var provider = NullTelemetryProvider.Instance;

        var activity = provider.StartPublishActivity("test-queue", "TestMessage");

        activity.Should().BeNull("it should return null for no-op provider");
    }

    [Fact]
    public void When_Extracting_Trace_Context()
    {
        var provider = new TestTelemetryProvider();
        var headers = new Dictionary<string, object>
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
        var headers = new Dictionary<string, object>();

        provider.InjectTraceContext(headers);

        headers.Should().ContainKey("traceparent", "it should inject traceparent header");
    }
}

public class TestTelemetryProvider : ITelemetryProvider
{
    public int PublishedCount { get; private set; }
    public int ConsumedSuccessCount { get; private set; }
    public int ConsumedFailureCount { get; private set; }
    public int RetriedCount { get; private set; }
    public int DeadLetteredCount { get; private set; }

    public TelemetryActivity? StartPublishActivity(string queueName, string messageType)
    {
        return new TelemetryActivity("publish", queueName, messageType);
    }

    public TelemetryActivity? StartConsumeActivity(string queueName, string messageType, string? messageId)
    {
        return new TelemetryActivity("consume", queueName, messageType) { MessageId = messageId };
    }

    public void RecordMessagePublished(string queueName, string messageType)
    {
        PublishedCount++;
    }

    public void RecordMessageConsumed(string queueName, string messageType, bool success)
    {
        if (success)
            ConsumedSuccessCount++;
        else
            ConsumedFailureCount++;
    }

    public void RecordMessageRetried(string queueName, string messageType, int attemptNumber)
    {
        RetriedCount++;
    }

    public void RecordMessageDeadLettered(string queueName, string messageType, string reason)
    {
        DeadLetteredCount++;
    }

    public TraceContext? ExtractTraceContext(IDictionary<string, object> headers)
    {
        if (headers.TryGetValue("traceparent", out var value) && value is string traceparent)
        {
            var parts = traceparent.Split('-');
            if (parts.Length >= 3)
                return new TraceContext(parts[1], parts[2]);
        }
        return null;
    }

    public void InjectTraceContext(IDictionary<string, object> headers)
    {
        headers["traceparent"] = "00-generated-trace-id-generated-span-id-01";
    }
}

public class OpenTelemetryProviderTests
{
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

        using var activity = provider.StartPublishActivity("test-queue", "TestMessage");

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

        using var activity = provider.StartPublishActivity("test-queue", "TestMessage");

        activity.Should().NotBeNull("it should create an activity when listener is registered");
        activity!.OperationName.Should().Be("publish", "it should set operation name");
    }

    [Fact]
    public void When_Recording_Metrics()
    {
        var provider = new OpenTelemetryProvider("ESE.MessageQueue.Tests");

        var act = () =>
        {
            provider.RecordMessagePublished("test-queue", "TestMessage");
            provider.RecordMessageConsumed("test-queue", "TestMessage", success: true);
            provider.RecordMessageRetried("test-queue", "TestMessage", attemptNumber: 1);
            provider.RecordMessageDeadLettered("test-queue", "TestMessage", "test reason");
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
}

