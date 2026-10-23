using System.Reflection;
using AwesomeAssertions;
using ESE.MessageQueue.AzureServiceBus;
using ESE.MessageQueue.Configuration;
using ESE.MessageQueue.RabbitMQ;
using ESE.MessageQueue.Telemetry;
using ESE.MessageQueue.Telemetry.OpenTelemetry;
using Microsoft.Extensions.Logging;

namespace ESE.MessageQueue.Tests.Packaging;

// docs/operations/monitoring.md is what operators build dashboards and alerts from: it must list every log event the
// three packages can write (id and level), every metric with its unit, and every span and metric attribute name.
[Trait("Category", "Unit")]
public class Given_the_monitoring_reference
{
    const string Page = "docs/operations/monitoring.md";

    [Fact]
    public void When_Reading_The_Log_Events()
    {
        var table = MarkdownDocument.Load(Page).Tables.Single(t => t.Heading == "Log events");
        var id = table.Column("EventId");
        var level = table.Column("Level");
        var documented = table.Rows.Select(row => $"{row[id]} {row[level]}").ToList();

        var assemblies = new[] { typeof(MessageQueueConfig).Assembly, typeof(RabbitMqConfig).Assembly, typeof(AzureServiceBusConfig).Assembly };
        var emitted = assemblies.SelectMany(a => a.GetTypes())
            .SelectMany(t => t.GetMethods(BindingFlags.Static | BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly))
            .Select(m => m.GetCustomAttribute<LoggerMessageAttribute>())
            .OfType<LoggerMessageAttribute>()
            .Select(a => $"{a.EventId} {a.Level}")
            .ToList();

        emitted.Should().HaveCountGreaterThan(50, "it should find the library's log messages");
        documented.Should().BeEquivalentTo(emitted, "it should document every log event with its id and level, and no other");
    }

    [Fact]
    public void When_Reading_The_Metrics()
    {
        var table = MarkdownDocument.Load(Page).Tables.Single(t => t.Heading == "Metrics" && t.Header.Contains("Unit"));
        var name = table.Column("Metric");
        var unit = table.Column("Unit");
        var documented = table.Rows.ToDictionary(row => MarkdownDocument.CodeSpans(row[name]).Single(), row => MarkdownDocument.CodeSpans(row[unit]).Single());

        var meter = $"docs-{Guid.NewGuid():N}";
        using var capture = new MetricCapture(meter);
        using var provider = new OpenTelemetryProvider(meter);
        var instruments = capture.Instruments.Values.ToDictionary(i => i.Name, i => i.Unit ?? string.Empty);

        instruments.Should().HaveCountGreaterThan(5, "it should find the provider's instruments");
        documented.Should().BeEquivalentTo(instruments, "it should document every metric with its unit, and no other");
    }

    [Fact]
    public void When_Reading_The_Attribute_Names()
    {
        var text = File.ReadAllText(RepositoryFiles.Path(Page));
        var names = typeof(TelemetryNames).GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral)
            .Select(f => (string)f.GetRawConstantValue()!)
            .ToList();

        names.Should().HaveCountGreaterThan(20, "it should find the telemetry names");
        names.Where(n => !text.Contains($"`{n}`", StringComparison.Ordinal))
            .Should().BeEmpty("it should name every span and metric attribute the library writes");
    }
}
