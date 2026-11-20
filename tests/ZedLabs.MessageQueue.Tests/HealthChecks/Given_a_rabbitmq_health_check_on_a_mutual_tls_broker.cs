using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using ZedLabs.MessageQueue.HealthChecks;
using ZedLabs.MessageQueue.RabbitMQ;

namespace ZedLabs.MessageQueue.Tests.HealthChecks;

// The standalone connection-string check against a broker that requires client certificates (ADR 0022).
[Trait("Category", "Integration")]
[Collection(RabbitMqMutualTlsCollection.Name)]
public sealed class Given_a_rabbitmq_health_check_on_a_mutual_tls_broker : IAsyncLifetime
{
    const int Timeout = 60_000;

    readonly RabbitMqMutualTlsBrokerFixture broker;

    public Given_a_rabbitmq_health_check_on_a_mutual_tls_broker(RabbitMqMutualTlsBrokerFixture broker)
    {
        this.broker = broker;
    }

    public ValueTask InitializeAsync()
    {
        broker.SkipUnlessAvailable();
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    async Task<HealthReportEntry> ProbeAsync(RabbitMqTlsConfig tls, CancellationToken cancellationToken)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddHealthChecks().AddRabbitMqHealthCheck(broker.ConnectionString, tls, timeout: TimeSpan.FromSeconds(30));
        await using var provider = services.BuildServiceProvider();
        var report = await provider.GetRequiredService<HealthCheckService>().CheckHealthAsync(cancellationToken);
        return report.Entries["rabbitmq"];
    }

    RabbitMqTlsConfig Tls() => new()
    {
        ServerName = TestCertificateAuthority.ServerName,
        CaCertificatePath = broker.Authority.CaPemPath
    };

    [Fact(Timeout = Timeout)]
    public async Task When_The_Check_Presents_A_Client_Certificate()
    {
        var tls = Tls();
        tls.ClientCertificatePath = broker.Authority.ClientCertificatePemPath;
        tls.ClientCertificateKeyPath = broker.Authority.ClientKeyPemPath;

        var entry = await ProbeAsync(tls, TestContext.Current.CancellationToken);

        entry.Status.Should().Be(HealthStatus.Healthy, "it should connect with the client certificate and the trusted CA");
    }

    [Fact(Timeout = Timeout)]
    public async Task When_The_Check_Presents_No_Client_Certificate()
    {
        var entry = await ProbeAsync(Tls(), TestContext.Current.CancellationToken);

        entry.Status.Should().Be(HealthStatus.Unhealthy, "it should report the broker's refusal of a client without a certificate");
        entry.Exception.Should().BeNull("it should keep the exception out of the result");
        (entry.Description ?? string.Empty).Should().NotContain(RabbitMqMutualTlsBrokerFixture.Password, "it should not reveal the password");
    }
}
