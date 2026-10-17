using AwesomeAssertions;
using ESE.MessageQueue.Processing;
using ESE.MessageQueue.Providers;
using ESE.MessageQueue.Telemetry;
using Microsoft.Extensions.Logging.Abstractions;

namespace ESE.MessageQueue.Tests.Processing;

[Trait("Category", "Unit")]
public class Given_publish_telemetry
{
    static readonly TelemetryTarget Target = new("test", "orders", "Order");

    [Fact]
    public void When_Telemetry_Is_Disabled()
    {
        var first = PublishTelemetry.Start(NullTelemetryProvider.Instance, Target, 1, NullLogger.Instance, "m-1");
        var second = PublishTelemetry.Start(NullTelemetryProvider.Instance, Target, 3, NullLogger.Instance);
        first.Failed(new TimeoutException());

        first.Should().BeSameAs(second, "it should not allocate per publish when no telemetry provider is registered");
        first.TraceHeaders.Should().BeEmpty("it should add no trace headers without telemetry");
    }
}
