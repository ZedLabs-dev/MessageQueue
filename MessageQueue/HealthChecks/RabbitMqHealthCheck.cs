using Microsoft.Extensions.Diagnostics.HealthChecks;
using RabbitMQ.Client;

namespace ESE.MessageQueue.HealthChecks;

/// <summary>
/// Health check for RabbitMQ connectivity.
/// </summary>
public class RabbitMqHealthCheck : IHealthCheck
{
    readonly string connectionString;

    public RabbitMqHealthCheck(string connectionString)
    {
        this.connectionString = connectionString;
    }

    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var factory = new ConnectionFactory
            {
                Uri = new Uri(connectionString),
                RequestedConnectionTimeout = TimeSpan.FromSeconds(5)
            };

            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();

            if (connection.IsOpen && channel.IsOpen)
            {
                return Task.FromResult(HealthCheckResult.Healthy(
                    "RabbitMQ connection is healthy.",
                    new Dictionary<string, object>
                    {
                        ["server"] = connection.Endpoint.HostName,
                        ["port"] = connection.Endpoint.Port
                    }));
            }

            return Task.FromResult(HealthCheckResult.Unhealthy("RabbitMQ connection is not open."));
        }
        catch (Exception ex)
        {
            return Task.FromResult(HealthCheckResult.Unhealthy(
                "RabbitMQ health check failed.",
                ex,
                new Dictionary<string, object>
                {
                    ["error"] = ex.Message
                }));
        }
    }
}

