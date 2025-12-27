using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ESE.MessageQueue.HealthChecks;

/// <summary>
/// Health check for message queue connectivity.
/// </summary>
public class MessageQueueHealthCheck : IHealthCheck
{
    readonly Func<CancellationToken, Task<bool>> connectivityCheck;
    readonly string queueName;

    public MessageQueueHealthCheck(string queueName, Func<CancellationToken, Task<bool>> connectivityCheck)
    {
        this.queueName = queueName;
        this.connectivityCheck = connectivityCheck;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var isHealthy = await connectivityCheck(cancellationToken);

            if (isHealthy)
            {
                return HealthCheckResult.Healthy($"Message queue '{queueName}' is accessible.");
            }

            return HealthCheckResult.Unhealthy($"Message queue '{queueName}' is not accessible.");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy(
                $"Message queue '{queueName}' health check failed.",
                ex,
                new Dictionary<string, object>
                {
                    ["queue"] = queueName,
                    ["error"] = ex.Message
                });
        }
    }
}

/// <summary>
/// Health check result with additional queue metrics.
/// </summary>
public record QueueHealthStatus
{
    public bool IsHealthy { get; init; }
    public string QueueName { get; init; } = string.Empty;
    public int? MessageCount { get; init; }
    public int? ConsumerCount { get; init; }
    public TimeSpan? ResponseTime { get; init; }
    public string? ErrorMessage { get; init; }
}

