using System.Text;
using DotNet.Testcontainers.Builders;
using Testcontainers.RabbitMq;
using ZedLabs.MessageQueue.RabbitMQ;

namespace ZedLabs.MessageQueue.Tests;

// A second RabbitMQ container, from the same pinned image as RabbitMqBrokerFixture, that accepts AMQP over TLS on 5671
// only from clients presenting a certificate issued by a CA generated for this run (verify_peer +
// fail_if_no_peer_cert). Its own collection keeps it off the shared broker; it starts only when a test in the
// collection runs. Same Docker rules as the shared broker: skipped without Docker unless MQ_REQUIRE_DOCKER is set.
public sealed class RabbitMqMutualTlsBrokerFixture : IAsyncLifetime
{
    public const string Password = "mtls-broker-S3CRET";
    const string UserName = "mtls-app";
    const string RequireDockerVariable = "MQ_REQUIRE_DOCKER";
    const ushort TlsPort = 5671;
    const string CertificateFolder = "/etc/rabbitmq/tls";

    // rabbitmq.conf settings for the TLS listener, in the image's conf.d folder next to its defaults.
    static readonly string TlsConfig = string.Join('\n',
        $"listeners.ssl.default = {TlsPort}",
        $"ssl_options.cacertfile = {CertificateFolder}/ca.crt",
        $"ssl_options.certfile = {CertificateFolder}/server.crt",
        $"ssl_options.keyfile = {CertificateFolder}/server.key",
        "ssl_options.verify = verify_peer",
        "ssl_options.fail_if_no_peer_cert = true",
        string.Empty);

    RabbitMqContainer? container;
    string? unavailableReason;

    public TestCertificateAuthority Authority { get; } = new();

    // amqps:// with the mapped TLS port; the host is whatever Testcontainers reports, so configs set Tls:ServerName.
    public string ConnectionString => container is null
        ? throw new InvalidOperationException("The mutual-TLS RabbitMQ container is not running.")
        : $"amqps://{UserName}:{Password}@{container.Hostname}:{container.GetMappedPublicPort(TlsPort)}/";

    static bool DockerRequired =>
        Environment.GetEnvironmentVariable(RequireDockerVariable)?.Trim() is { } value
        && (value == "1" || value.Equals("true", StringComparison.OrdinalIgnoreCase));

    public async ValueTask InitializeAsync()
    {
        try
        {
            // Copied in, not bind-mounted: copies are world-readable for the image's rabbitmq user on every host.
            container = new RabbitMqBuilder(RabbitMqBrokerFixture.Image)
                .WithUsername(UserName)
                .WithPassword(Password)
                .WithPortBinding(TlsPort, true)
                .WithResourceMapping(File.ReadAllBytes(Authority.CaPemPath), $"{CertificateFolder}/ca.crt")
                .WithResourceMapping(File.ReadAllBytes(Authority.ServerCertificatePemPath), $"{CertificateFolder}/server.crt")
                .WithResourceMapping(File.ReadAllBytes(Authority.ServerKeyPemPath), $"{CertificateFolder}/server.key")
                .WithResourceMapping(Encoding.UTF8.GetBytes(TlsConfig), "/etc/rabbitmq/conf.d/90-mutual-tls.conf")
                .Build();
            await container.StartAsync();
        }
        catch (DockerUnavailableException ex) when (!DockerRequired)
        {
            unavailableReason = $"Docker is not available, so the mutual-TLS RabbitMQ tests cannot run ({ex.Message}). " +
                $"Start Docker, or set {RequireDockerVariable}=true to make this a failure.";
            return;
        }

        TestContext.Current.SendDiagnosticMessage(
            $"Mutual-TLS RabbitMQ container {container.Id} ({RabbitMqBrokerFixture.Image}) listening at {container.Hostname}:{container.GetMappedPublicPort(TlsPort)}");
    }

    public async ValueTask DisposeAsync()
    {
        if (container is not null)
        {
            await container.DisposeAsync();
        }
        Authority.Dispose();
    }

    public void SkipUnlessAvailable() => Assert.SkipWhen(unavailableReason is not null, unavailableReason ?? string.Empty);

    // A queue config for this broker: TLS against the generated CA and the server name on its certificate; the caller
    // adds (or leaves out) the client certificate.
    public RabbitMqConfig NewConfig(string prefix, Action<RabbitMqTlsConfig>? tls = null)
    {
        var config = new RabbitMqConfig
        {
            ConnectionString = ConnectionString,
            QueueName = $"{prefix}-{Guid.NewGuid():N}",
            Retry = RabbitMqTestQueues.FastRetry,
            UseQuorumQueues = true,
            QuorumInitialGroupSize = 1
        };
        config.Tls.ServerName = TestCertificateAuthority.ServerName;
        config.Tls.CaCertificatePath = Authority.CaPemPath;
        tls?.Invoke(config.Tls);
        return config;
    }
}

[CollectionDefinition(Name)]
public sealed class RabbitMqMutualTlsCollection : ICollectionFixture<RabbitMqMutualTlsBrokerFixture>
{
    public const string Name = "RabbitMQ mutual-TLS broker";
}
