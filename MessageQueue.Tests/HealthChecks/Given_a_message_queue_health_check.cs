using AwesomeAssertions;
using ESE.MessageQueue.HealthChecks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ESE.MessageQueue.Tests.HealthChecks;

[Trait("Category", "Unit")]
public class Given_a_message_queue_health_check
{
    [Fact]
    public async Task When_Health_Check_Succeeds()
    {
        var healthCheck = new MessageQueueHealthCheck("test-queue", _ => Task.FromResult(true));
        var context = new HealthCheckContext
        {
            Registration = new HealthCheckRegistration("test", healthCheck, null, null)
        };

        var result = await healthCheck.CheckHealthAsync(context, TestContext.Current.CancellationToken);

        result.Status.Should().Be(HealthStatus.Healthy, "it should report healthy status");
        result.Description.Should().Contain("test-queue", "it should include the queue name in the description");
    }

    [Fact]
    public async Task When_Health_Check_Fails()
    {
        var healthCheck = new MessageQueueHealthCheck("test-queue", _ => Task.FromResult(false));
        var context = new HealthCheckContext
        {
            Registration = new HealthCheckRegistration("test", healthCheck, null, null)
        };

        var result = await healthCheck.CheckHealthAsync(context, TestContext.Current.CancellationToken);

        result.Status.Should().Be(HealthStatus.Unhealthy, "it should report unhealthy status");
    }

    [Fact]
    public async Task When_Health_Check_Throws_Exception()
    {
        var healthCheck = new MessageQueueHealthCheck("test-queue",
            _ => throw new InvalidOperationException("Connection failed"));
        var context = new HealthCheckContext
        {
            Registration = new HealthCheckRegistration("test", healthCheck, null, null)
        };

        var result = await healthCheck.CheckHealthAsync(context, TestContext.Current.CancellationToken);

        result.Status.Should().Be(HealthStatus.Unhealthy, "it should report unhealthy status on exception");
        result.Exception.Should().BeOfType<InvalidOperationException>("it should include the exception");
    }

    [Fact]
    public async Task When_Health_Check_Throws_An_Exception_With_Sensitive_Text()
    {
        var healthCheck = new MessageQueueHealthCheck("test-queue",
            _ => throw new InvalidOperationException("amqps://user:hunter2@broker.internal refused"));
        var context = new HealthCheckContext
        {
            Registration = new HealthCheckRegistration("test", healthCheck, null, null)
        };

        var result = await healthCheck.CheckHealthAsync(context, TestContext.Current.CancellationToken);

        result.Data.Values.Select(v => v.ToString()).Should().NotContain(v => v!.Contains("hunter2"), "it should not put exception text in the data");
        result.Description.Should().NotContain("hunter2", "it should not put exception text in the description");
    }

    [Fact]
    public void When_Registering_Health_Check_Via_DI()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddHealthChecks()
            .AddMessageQueueHealthCheck("my-queue", _ => Task.FromResult(true));

        var provider = services.BuildServiceProvider();
        var healthCheckService = provider.GetService<HealthCheckService>();

        healthCheckService.Should().NotBeNull("it should register the health check service");
    }
}
