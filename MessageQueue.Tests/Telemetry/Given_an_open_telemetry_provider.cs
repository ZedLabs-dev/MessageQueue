using System.Diagnostics;
using AwesomeAssertions;
using ESE.MessageQueue.Extensions;
using ESE.MessageQueue.Telemetry;
using ESE.MessageQueue.Telemetry.OpenTelemetry;
using Microsoft.Extensions.DependencyInjection;

namespace ESE.MessageQueue.Tests.Telemetry;

[Trait("Category", "Unit")]
public class Given_an_open_telemetry_provider
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

    [Fact]
    public void When_Setting_An_Error_Through_A_Base_Reference()
    {
        using var capture = new ActivityCapture("ESE.MessageQueue.Tests.BaseReference");
        var provider = new OpenTelemetryProvider(capture.SourceName);

        using (TelemetryActivity activity = provider.StartConsumeActivity("test-queue", "TestMessage", "msg-1")!)
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

        using (var activity = provider.StartConsumeActivity("test-queue", "TestMessage", "msg-1")!)
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

        using (var activity = provider.StartConsumeActivity("test-queue", "TestMessage", "msg-1")!)
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

        using (var activity = provider.StartConsumeActivity("test-queue", "TestMessage", "msg-1")!)
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
