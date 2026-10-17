using AwesomeAssertions;
using ESE.MessageQueue.AzureServiceBus;
using ESE.MessageQueue.Configuration;
using ESE.MessageQueue.RabbitMQ;

namespace ESE.MessageQueue.Tests.Packaging;

// ADR 0005: an app that uses one broker (or only the in-memory provider) does not pull in the others' SDKs.
[Trait("Category", "Unit")]
public class Given_the_package_split
{
    static IEnumerable<string> References(Type type) =>
        type.Assembly.GetReferencedAssemblies().Select(a => a.Name ?? "");

    // S2: the library uses System.Diagnostics only; choosing OpenTelemetry packages and exporters is the host's job.
    static readonly string[] Forbidden = ["RabbitMQ.", "Azure.", "OpenTelemetry"];

    [Fact]
    public void When_Inspecting_The_Core_Package() =>
        References(typeof(MessageQueueConfig)).Should().NotContain(
            name => Forbidden.Any(prefix => name.StartsWith(prefix, StringComparison.Ordinal)),
            "it should not reference a broker SDK or OpenTelemetry from the core package");

    [Fact]
    public void When_Inspecting_The_RabbitMq_Package() =>
        References(typeof(RabbitMqConfig)).Should().NotContain(
            name => name.StartsWith("Azure.", StringComparison.Ordinal) || name.StartsWith("OpenTelemetry", StringComparison.Ordinal),
            "it should not reference the Azure SDK or OpenTelemetry from the RabbitMQ package");

    [Fact]
    public void When_Inspecting_The_Azure_Service_Bus_Package() =>
        References(typeof(AzureServiceBusConfig)).Should().NotContain(
            name => name.StartsWith("RabbitMQ.", StringComparison.Ordinal) || name.StartsWith("OpenTelemetry", StringComparison.Ordinal),
            "it should not reference RabbitMQ.Client or OpenTelemetry from the Azure Service Bus package");
}
