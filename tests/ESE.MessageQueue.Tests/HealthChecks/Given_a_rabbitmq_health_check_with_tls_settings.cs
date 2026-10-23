using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using AwesomeAssertions;
using ESE.MessageQueue.Exceptions;
using ESE.MessageQueue.HealthChecks;
using ESE.MessageQueue.RabbitMQ;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ESE.MessageQueue.Tests.HealthChecks;

// The standalone connection-string check takes the queue's Tls settings (ADR 0022): a client certificate for brokers
// that require one, a trust anchor and the TLS rules. One check instance per registration loads the certificate once
// and disposes it with the container. Port 1 on the loopback interface refuses the connection: no broker needed.
[Trait("Category", "Unit")]
public sealed class Given_a_rabbitmq_health_check_with_tls_settings : IDisposable
{
    const string Secret = "S3CRET-PASSWORD";
    const string UnreachableTlsBroker = $"amqps://app:{Secret}@127.0.0.1:1/";

    readonly TestCertificateAuthority authority = new();

    public void Dispose() => authority.Dispose();

    RabbitMqTlsConfig ClientTls() => new()
    {
        ClientCertificatePath = authority.ClientPfxPath,
        ClientCertificatePassword = TestCertificateAuthority.Password,
        CaCertificatePath = authority.CaPemPath,
        ServerName = TestCertificateAuthority.ServerName
    };

    static HealthCheckContext Context(IHealthCheck check) =>
        new() { Registration = new HealthCheckRegistration("rabbitmq", check, null, null) };

    static ServiceProvider Register(Action<IHealthChecksBuilder> register)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        register(services.AddHealthChecks());
        return services.BuildServiceProvider();
    }

    static void ShouldNotLeak(HealthReportEntry entry)
    {
        entry.Exception.Should().BeNull("it should keep the exception, which can name the broker or a path, out of the result");
        entry.Data.Should().BeEmpty("it should put nothing sensitive in the result data");
        (entry.Description ?? string.Empty).Should().NotContain(Secret, "it should not reveal the connection password")
            .And.NotContain(TestCertificateAuthority.Password, "it should not reveal the client certificate password");
    }

    [Fact]
    public void When_Built_With_A_Client_Certificate_And_A_Ca()
    {
        using var check = new RabbitMqHealthCheck(UnreachableTlsBroker, tls: ClientTls());

        var factory = check.CreateConnectionFactory();

        factory.Ssl.Certs!.Cast<X509Certificate2>().Should().ContainSingle("it should present the configured client certificate")
            .Which.Thumbprint.Should().Be(authority.ClientThumbprint, "it should present the configured client certificate");
        factory.Ssl.ServerName.Should().Be(TestCertificateAuthority.ServerName, "it should check the configured server name");
        factory.Ssl.CertificateValidationCallback!(factory, authority.Server, null, SslPolicyErrors.RemoteCertificateChainErrors)
            .Should().BeTrue("it should trust the configured CA");
        factory.AutomaticRecoveryEnabled.Should().BeFalse("it should probe once instead of recovering in the background");
        factory.RequestedConnectionTimeout.Should().Be(TimeSpan.FromSeconds(5), "it should give up on a silent broker within the probe timeout");
    }

    [Fact]
    public async Task When_Probed_More_Than_Once_Through_Dependency_Injection()
    {
        var provider = Register(builder => builder.AddRabbitMqHealthCheck(UnreachableTlsBroker, ClientTls()));
        var service = provider.GetRequiredService<HealthCheckService>();

        var first = await service.CheckHealthAsync(TestContext.Current.CancellationToken);
        var check = provider.GetRequiredKeyedService<RabbitMqHealthCheck>("rabbitmq");
        var certificate = check.ClientCertificate;
        var second = await service.CheckHealthAsync(TestContext.Current.CancellationToken);

        first.Entries["rabbitmq"].Status.Should().Be(HealthStatus.Unhealthy, "it should report the unreachable broker");
        ShouldNotLeak(first.Entries["rabbitmq"]);
        ShouldNotLeak(second.Entries["rabbitmq"]);
        certificate.Should().NotBeNull("it should load the client certificate for the first probe");
        provider.GetRequiredKeyedService<RabbitMqHealthCheck>("rabbitmq").ClientCertificate
            .Should().BeSameAs(certificate, "it should load the certificate once, not once per probe");
        await provider.DisposeAsync();
        certificate!.Handle.Should().Be(IntPtr.Zero, "it should dispose the certificate with the container");
    }

    [Fact]
    public async Task When_Two_Checks_Are_Registered_With_Different_Settings()
    {
        using var other = new TestCertificates();
        var otherTls = new RabbitMqTlsConfig { ClientCertificatePath = other.CombinedPemPath, ServerName = "other.example" };
        await using var provider = Register(builder => builder
            .AddRabbitMqHealthCheck(UnreachableTlsBroker, ClientTls(), name: "a")
            .AddRabbitMqHealthCheck(UnreachableTlsBroker, otherTls, name: "b"));

        var report = await provider.GetRequiredService<HealthCheckService>().CheckHealthAsync(TestContext.Current.CancellationToken);

        report.Entries.Keys.Should().BeEquivalentTo(["a", "b"], "it should run both checks");
        var a = provider.GetRequiredKeyedService<RabbitMqHealthCheck>("a");
        var b = provider.GetRequiredKeyedService<RabbitMqHealthCheck>("b");
        a.Should().NotBeSameAs(b, "it should keep one instance per registration");
        a.ClientCertificate!.Thumbprint.Should().Be(authority.ClientThumbprint, "it should probe check a with a's certificate");
        b.ClientCertificate!.Thumbprint.Should().Be(other.Thumbprint, "it should probe check b with b's certificate");
        a.CreateConnectionFactory().Ssl.ServerName.Should().Be(TestCertificateAuthority.ServerName, "it should keep a's server name");
        b.CreateConnectionFactory().Ssl.ServerName.Should().Be("other.example", "it should keep b's server name");
    }

    [Fact]
    public void When_The_Tls_Settings_Change_After_Registration()
    {
        var tls = ClientTls();
        using var provider = Register(builder => builder.AddRabbitMqHealthCheck(UnreachableTlsBroker, tls));
        tls.ClientCertificatePath = Path.Combine(authority.Folder, "missing.pfx");

        var factory = provider.GetRequiredKeyedService<RabbitMqHealthCheck>("rabbitmq").CreateConnectionFactory();

        factory.Ssl.Certs!.Cast<X509Certificate2>().Should().ContainSingle("it should keep the settings it was registered with")
            .Which.Thumbprint.Should().Be(authority.ClientThumbprint, "it should keep the settings it was registered with");
    }

    [Fact]
    public void When_The_Tls_Settings_Change_After_Construction()
    {
        var tls = ClientTls();
        using var check = new RabbitMqHealthCheck(UnreachableTlsBroker, tls: tls);
        tls.ClientCertificatePath = Path.Combine(authority.Folder, "missing.pfx");

        var factory = check.CreateConnectionFactory();

        factory.Ssl.Certs!.Cast<X509Certificate2>().Should().ContainSingle("it should keep the settings it was constructed with")
            .Which.Thumbprint.Should().Be(authority.ClientThumbprint, "it should keep the settings it was constructed with");
    }

    [Fact]
    public async Task When_The_Client_Certificate_Cannot_Be_Loaded()
    {
        var logger = new RecordingLogger();
        var tls = ClientTls();
        tls.ClientCertificatePath = Path.Combine(authority.Folder, "missing.pfx");
        using var check = new RabbitMqHealthCheck(UnreachableTlsBroker, tls: tls) { Logger = logger };

        var result = await check.CheckHealthAsync(Context(check), TestContext.Current.CancellationToken);

        result.Status.Should().Be(HealthStatus.Unhealthy, "it should report a check that cannot present its certificate");
        result.Exception.Should().BeNull("it should keep the exception out of the result");
        logger.Entries.Should().Contain(e => e.Exception is ConfigurationException && e.Exception.Message.Contains("Tls:ClientCertificatePath"),
            "it should log which setting is wrong");
        logger.Entries.Should().NotContain(e => e.Message.Contains(TestCertificateAuthority.Password) || (e.Exception != null && e.Exception.ToString().Contains(TestCertificateAuthority.Password)),
            "it should not log the client certificate password");
    }

    [Fact]
    public async Task When_A_Client_Certificate_Is_Set_Over_Plain_Amqp()
    {
        var logger = new RecordingLogger();
        using var check = new RabbitMqHealthCheck($"amqp://app:{Secret}@127.0.0.1:1/", tls: ClientTls()) { Logger = logger };

        var result = await check.CheckHealthAsync(Context(check), TestContext.Current.CancellationToken);

        result.Status.Should().Be(HealthStatus.Unhealthy, "it should apply the queue's rule that a client certificate needs TLS");
        logger.Entries.Should().Contain(e => e.Exception is ConfigurationException && e.Exception.Message.Contains("needs TLS"),
            "it should log that it refused to send a certificate without TLS");
        check.ClientCertificate.Should().BeNull("it should not load a certificate it refuses to use");
    }

    [Fact]
    public void When_RequireTls_Is_Off_For_A_Remote_Plain_Broker()
    {
        using var provider = Register(builder => builder.AddRabbitMqHealthCheck("amqp://app:pw@rabbit.invalid:1/", new RabbitMqTlsConfig(), requireTls: false));
        var check = provider.GetRequiredKeyedService<RabbitMqHealthCheck>("rabbitmq");

        var act = () => check.CreateConnectionFactory();

        act.Should().NotThrow("it should honor RequireTls = false for a broker that is deliberately plain");
    }

    [Fact]
    public void When_Registered_Without_Tls_Settings_And_RequireTls_Off()
    {
        using var provider = Register(builder => builder.AddRabbitMqHealthCheck("amqp://app:pw@rabbit.invalid:1/", requireTls: false));

        var act = () => provider.GetRequiredKeyedService<RabbitMqHealthCheck>("rabbitmq").CreateConnectionFactory();

        act.Should().NotThrow("it should pass RequireTls = false on from the overload without Tls settings");
    }

    [Fact]
    public void When_RequireTls_Is_Left_At_Its_Default_For_A_Remote_Plain_Broker()
    {
        using var provider = Register(builder => builder.AddRabbitMqHealthCheck("amqp://app:pw@rabbit.invalid:1/", new RabbitMqTlsConfig()));

        var act = () => provider.GetRequiredKeyedService<RabbitMqHealthCheck>("rabbitmq").CreateConnectionFactory();

        act.Should().Throw<ConfigurationException>("it should refuse plain AMQP to a remote host, like the queue");
    }

    [Fact]
    public async Task When_Probed_After_Disposal()
    {
        var logger = new RecordingLogger();
        var check = new RabbitMqHealthCheck(UnreachableTlsBroker, tls: ClientTls()) { Logger = logger };
        check.CreateConnectionFactory();
        check.Dispose();

        var result = await check.CheckHealthAsync(Context(check), TestContext.Current.CancellationToken);

        result.Status.Should().Be(HealthStatus.Unhealthy, "it should not probe with a disposed certificate");
        logger.Entries.Should().Contain(e => e.Exception is ObjectDisposedException, "it should log that the check was disposed");
    }

    [Fact]
    public void When_Registered_Without_Tls_Settings()
    {
        var act = () => Register(builder => builder.AddRabbitMqHealthCheck(UnreachableTlsBroker, (RabbitMqTlsConfig)null!));

        act.Should().Throw<ArgumentNullException>("it should refuse a missing Tls settings object at registration");
    }
}
