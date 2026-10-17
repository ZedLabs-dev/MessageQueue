using ESE.MessageQueue.RabbitMQ;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using RabbitMQ.Client;

namespace ESE.MessageQueue.HealthChecks;

/// <summary>
/// Health check for RabbitMQ connectivity. Registered with <c>AddRabbitMqHealthCheck&lt;T&gt;()</c> it reuses the
/// queue's connection; with a connection string it opens (and closes) a connection per probe. The result carries no
/// broker address and no exception text (S5); the exception itself is attached for the host's logging.
/// </summary>
public class RabbitMqHealthCheck : IHealthCheck
{
    const string Healthy = "RabbitMQ connection is healthy.";
    const string NotOpen = "RabbitMQ connection is not open.";
    const string Failed = "RabbitMQ health check failed.";

    readonly string? connectionString;
    readonly bool? requireTls;
    readonly IRabbitMqConnectionProbe? probe;

    /// <summary>
    /// Probes by opening a connection per check, with the same TLS rules as the queue: unless
    /// <paramref name="requireTls"/> is false, plain <c>amqp://</c> to a host other than localhost is refused.
    /// </summary>
    public RabbitMqHealthCheck(string connectionString, bool? requireTls = null)
    {
        this.connectionString = connectionString;
        this.requireTls = requireTls;
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
            return HealthCheckResult.Unhealthy(Failed, ex);
        }
    }

    async Task<bool> ProbeConnectionStringAsync(CancellationToken cancellationToken)
    {
        var factory = RabbitMqConnections.CreateFactory(new RabbitMqConfig
        {
            ConnectionString = connectionString!,
            RequireTls = requireTls,
            QueueName = "health-check"
        });
        factory.RequestedConnectionTimeout = TimeSpan.FromSeconds(5);
        factory.AutomaticRecoveryEnabled = false;

        await using var connection = await factory.CreateConnectionAsync(cancellationToken);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);
        return connection.IsOpen && channel.IsOpen;
    }
}
