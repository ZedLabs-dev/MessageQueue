using AwesomeAssertions;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using ZedLabs.MessageQueue.HealthChecks;

namespace ZedLabs.MessageQueue.Tests.HealthChecks;

// The opt-in Manage-rights check (S4) reports counts only; failures never put exception text, the namespace or the
// exception itself into the result (S5).
[Trait("Category", "Unit")]
public class Given_an_azure_service_bus_metrics_health_check
{
    const string Secret = "sb://secret-host.servicebus.windows.net";

    static Task<HealthCheckResult> CheckAsync(AzureServiceBusMetricsHealthCheck check) =>
        check.CheckHealthAsync(new HealthCheckContext { Registration = new HealthCheckRegistration("metrics", check, null, null) },
            TestContext.Current.CancellationToken);

    [Fact]
    public async Task When_The_Metrics_Are_Available()
    {
        var properties = ServiceBusModelFactory.QueueRuntimeProperties("orders", activeMessageCount: 3, deadLetterMessageCount: 1, scheduledMessageCount: 2);
        var check = new AzureServiceBusMetricsHealthCheck("orders", (_, _) => Task.FromResult(properties));

        var result = await CheckAsync(check);

        result.Status.Should().Be(HealthStatus.Healthy, "it should report available metrics as healthy");
        result.Data.Should().BeEquivalentTo(new Dictionary<string, object>
        {
            ["activeMessageCount"] = 3L,
            ["deadLetterMessageCount"] = 1L,
            ["scheduledMessageCount"] = 2L
        }, "it should report the counts and nothing else");
        result.Exception.Should().BeNull("it should attach no exception");
    }

    [Fact]
    public async Task When_The_Queue_Does_Not_Exist()
    {
        var logger = new RecordingLogger();
        var check = new AzureServiceBusMetricsHealthCheck("orders", (_, _) =>
            throw new ServiceBusException($"{Secret}: entity not found", ServiceBusFailureReason.MessagingEntityNotFound))
        { Logger = logger };

        var result = await CheckAsync(check);

        result.Status.Should().Be(HealthStatus.Unhealthy, "it should report a missing queue");
        result.Description.Should().NotContain("secret-host", "it should not echo the exception text");
        result.Exception.Should().BeNull("it should not hand the raw exception to a health endpoint");
        result.Data.Should().BeEmpty("it should put nothing in the result data");
        logger.Entries.Should().Contain(e => e.Exception is ServiceBusException, "it should log the exception instead");
    }

    [Fact]
    public async Task When_The_Administration_Call_Fails()
    {
        var logger = new RecordingLogger();
        var check = new AzureServiceBusMetricsHealthCheck("orders", (_, _) =>
            throw new UnauthorizedAccessException($"{Secret} denied Manage"))
        { Logger = logger };

        var result = await CheckAsync(check);

        result.Status.Should().Be(HealthStatus.Unhealthy, "it should report the failure");
        result.Description.Should().NotContain("secret-host", "it should not echo the exception text");
        result.Exception.Should().BeNull("it should not hand the raw exception to a health endpoint");
        result.Data.Should().BeEmpty("it should put nothing in the result data");
        logger.Entries.Should().Contain(e => e.Exception is UnauthorizedAccessException, "it should log the exception instead");
    }
}
