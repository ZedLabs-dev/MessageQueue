using AwesomeAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using ZedLabs.MessageQueue.Extensions;
using ZedLabs.MessageQueue.HealthChecks;
using ZedLabs.MessageQueue.RabbitMQ;

namespace ZedLabs.MessageQueue.Tests.HealthChecks;

// Health endpoints are often unauthenticated: results carry no exception text, host or port (S5).
[Trait("Category", "Unit")]
public class Given_a_rabbitmq_health_check
{
    const string Secret = "S3CRET-PASSWORD";
    const string UnreachableBroker = $"amqp://app:{Secret}@127.0.0.1:1/";

    sealed class FakeProbe(Func<CancellationToken, Task<bool>> isConnected) : IRabbitMqConnectionProbe
    {
        public Task<bool> IsConnectedAsync(CancellationToken cancellationToken) => isConnected(cancellationToken);
    }

    static HealthCheckContext Context(IHealthCheck check) =>
        new() { Registration = new HealthCheckRegistration("rabbitmq", check, null, null) };

    static void ShouldNotLeak(HealthCheckResult result)
    {
        result.Data.Should().BeEmpty("it should put no broker topology or exception text in the result data");
        result.Exception.Should().BeNull("it should not hand the raw exception, which can name the broker or credentials, to a health endpoint");
        result.Description.Should().NotContain("127.0.0.1", "it should not reveal the broker address")
            .And.NotContain(Secret, "it should not reveal credentials");
    }

    [Fact]
    public async Task When_The_Queue_Connection_Is_Open()
    {
        var check = new RabbitMqHealthCheck(new FakeProbe(_ => Task.FromResult(true)));

        var result = await check.CheckHealthAsync(Context(check), TestContext.Current.CancellationToken);

        result.Status.Should().Be(HealthStatus.Healthy, "it should report the queue's open connection as healthy");
        ShouldNotLeak(result);
    }

    [Fact]
    public async Task When_The_Queue_Connection_Is_Closed()
    {
        var check = new RabbitMqHealthCheck(new FakeProbe(_ => Task.FromResult(false)));

        var result = await check.CheckHealthAsync(Context(check), TestContext.Current.CancellationToken);

        result.Status.Should().Be(HealthStatus.Unhealthy, "it should report a closed connection as unhealthy");
    }

    [Fact]
    public async Task When_The_Probe_Throws()
    {
        var check = new RabbitMqHealthCheck(new FakeProbe(_ =>
            Task.FromException<bool>(new InvalidOperationException($"connect to 127.0.0.1:5672 as app:{Secret} failed"))));

        var result = await check.CheckHealthAsync(Context(check), TestContext.Current.CancellationToken);

        result.Status.Should().Be(HealthStatus.Unhealthy, "it should report the failure");
        ShouldNotLeak(result);
    }

    [Fact]
    public async Task When_Probing_An_Unreachable_Broker_By_Connection_String()
    {
        var check = new RabbitMqHealthCheck(UnreachableBroker);

        var result = await check.CheckHealthAsync(Context(check), TestContext.Current.CancellationToken);

        result.Status.Should().Be(HealthStatus.Unhealthy, "it should report an unreachable broker");
        ShouldNotLeak(result);
    }

    [Fact]
    public async Task When_Registered_For_A_Queue_In_Dependency_Injection()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRabbitMqProvider();
        services.AddMessageQueue<TestMessage>(new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MessageQueue:Provider"] = "RabbitMq",
            ["MessageQueue:ConnectionString"] = UnreachableBroker,
            ["MessageQueue:QueueName"] = "orders"
        }).Build());
        services.AddHealthChecks().AddRabbitMqHealthCheck<TestMessage>();
        await using var provider = services.BuildServiceProvider();

        var report = await provider.GetRequiredService<HealthCheckService>().CheckHealthAsync(TestContext.Current.CancellationToken);

        var entry = report.Entries["rabbitmq"];
        entry.Status.Should().Be(HealthStatus.Unhealthy, "it should probe the registered queue's connection");
        entry.Data.Should().BeEmpty("it should put nothing sensitive in the result data");
        entry.Exception.Should().BeNull("it should not attach the exception, which can carry the connection string");
    }

    [Fact]
    public async Task When_Registered_For_A_Message_Type_Without_A_RabbitMQ_Queue()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInMemoryMessageQueue<TestMessage>();
        services.AddHealthChecks().AddRabbitMqHealthCheck<TestMessage>();
        await using var provider = services.BuildServiceProvider();

        var act = () => provider.GetRequiredService<HealthCheckService>().CheckHealthAsync(TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<InvalidOperationException>("it should fail loudly when no RabbitMQ queue is registered for the type")
            .WithMessage($"*{nameof(TestMessage)}*");
    }

    [Fact]
    public async Task When_Probing_A_Remote_Broker_Over_Plain_Amqp()
    {
        var logger = new RecordingLogger();
        var check = new RabbitMqHealthCheck("amqp://app:pw@rabbit.internal/") { Logger = logger };

        var result = await check.CheckHealthAsync(Context(check), TestContext.Current.CancellationToken);

        result.Status.Should().Be(HealthStatus.Unhealthy, "it should apply the same TLS rules as the queue");
        result.Exception.Should().BeNull("it should keep the exception out of the result");
        logger.Entries.Should().Contain(e => e.Exception is ZedLabs.MessageQueue.Exceptions.ConfigurationException,
            "it should log that it refused to dial without TLS");
    }
}
