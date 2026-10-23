using System.Security.Cryptography.X509Certificates;
using ESE.MessageQueue.RabbitMQ;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;

namespace ESE.MessageQueue.HealthChecks;

/// <summary>
/// Health check for RabbitMQ connectivity. Registered with <c>AddRabbitMqHealthCheck&lt;T&gt;()</c> it reuses the
/// queue's connection; with a connection string it opens (and closes) a connection per probe, with the queue's TLS
/// settings: its client certificate and CA certificates are loaded once and disposed with the check. The result carries
/// no broker address, no exception text and no exception (S5); the exception is logged instead.
/// </summary>
sealed class RabbitMqHealthCheck : IHealthCheck, IDisposable
{
    // Where a failed probe's exception goes (S5: never into the result). Set by the registration extensions.
    internal ILogger Logger { get; init; } = NullLogger.Instance;

    const string Healthy = "RabbitMQ connection is healthy.";
    const string NotOpen = "RabbitMQ connection is not open.";
    const string Failed = "RabbitMQ health check failed.";

    readonly string? connectionString;
    readonly bool? requireTls;
    readonly RabbitMqTlsConfig tls = new();
    readonly RabbitMqTlsCertificates certificates = new();
    readonly IRabbitMqConnectionProbe? probe;

    /// <summary>
    /// Probes by opening a connection per check, with the same TLS rules as the queue: unless
    /// <paramref name="requireTls"/> is false, plain <c>amqp://</c> to a host other than a loopback host (localhost, 127.0.0.0/8, ::1) is refused.
    /// <paramref name="tls"/> is copied: later changes to it do not reach the check.
    /// </summary>
    public RabbitMqHealthCheck(string connectionString, bool? requireTls = null, RabbitMqTlsConfig? tls = null)
    {
        this.connectionString = connectionString;
        this.requireTls = requireTls;
        if (tls is not null)
            this.tls = tls with { };
    }

    internal RabbitMqHealthCheck(IRabbitMqConnectionProbe probe)
    {
        this.probe = probe;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var open = probe is not null
                ? await probe.IsConnectedAsync(cancellationToken)
                : await ProbeConnectionStringAsync(cancellationToken);

            return open ? HealthCheckResult.Healthy(Healthy) : HealthCheckResult.Unhealthy(NotOpen);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            HealthCheckLog.ProbeFailed(Logger, "rabbitmq", ex);
            return HealthCheckResult.Unhealthy(Failed);
        }
    }

    internal X509Certificate2? ClientCertificate => certificates.LoadedClientCertificate;

    public void Dispose() => certificates.Dispose();

    internal ConnectionFactory CreateConnectionFactory()
    {
        var factory = RabbitMqConnections.CreateFactory(new RabbitMqConfig
        {
            ConnectionString = connectionString!,
            RequireTls = requireTls,
            Tls = tls,
            QueueName = "health-check"
        }, certificates);
        factory.RequestedConnectionTimeout = TimeSpan.FromSeconds(5);
        factory.AutomaticRecoveryEnabled = false;
        return factory;
    }

    async Task<bool> ProbeConnectionStringAsync(CancellationToken cancellationToken)
    {
        var factory = CreateConnectionFactory();

        await using var connection = await factory.CreateConnectionAsync(cancellationToken);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);
        return connection.IsOpen && channel.IsOpen;
    }
}
