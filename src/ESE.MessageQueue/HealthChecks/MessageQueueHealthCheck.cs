using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace ESE.MessageQueue.HealthChecks;

/// <summary>
/// A health check that runs an application-supplied connectivity probe: Healthy when it returns true, Unhealthy when
/// it returns false or throws. Results name only the queue: no exception, exception text, host or credentials (S5); a
/// thrown exception is logged instead. Thread-safe if the probe is.
/// </summary>
public sealed class MessageQueueHealthCheck : IHealthCheck
{
    readonly Func<CancellationToken, Task<bool>> connectivityCheck;
    readonly string queueName;
    readonly ILogger logger;

    /// <summary>
    /// Creates the check for <paramref name="queueName"/> (used in the result description) with the probe to run. A
    /// probe that throws is logged to <paramref name="logger"/>; the result carries no exception.
    /// </summary>
    public MessageQueueHealthCheck(string queueName, Func<CancellationToken, Task<bool>> connectivityCheck, ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(queueName);
        ArgumentNullException.ThrowIfNull(connectivityCheck);
        this.queueName = queueName;
        this.connectivityCheck = connectivityCheck;
        this.logger = logger ?? NullLogger.Instance;
    }

    /// <inheritdoc />
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
        // S5: the exception is logged, not returned: its text can carry hosts or credentials.
        catch (Exception ex)
        {
            HealthCheckLog.ProbeFailed(logger, queueName, ex);
            return HealthCheckResult.Unhealthy($"Message queue '{queueName}' health check failed.");
        }
    }
}
