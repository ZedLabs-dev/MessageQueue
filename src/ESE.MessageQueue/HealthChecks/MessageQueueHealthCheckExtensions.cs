using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ESE.MessageQueue.HealthChecks;

/// <summary>
/// Registers the generic message queue health check. Broker-specific checks live in the provider packages
/// (<c>AddRabbitMqHealthCheck</c>, <c>AddAzureServiceBusHealthCheck</c>).
/// </summary>
public static class MessageQueueHealthCheckExtensions
{
    /// <summary>
    /// Adds a health check for a message queue.
    /// </summary>
    public static IHealthChecksBuilder AddMessageQueueHealthCheck(
        this IHealthChecksBuilder builder,
        string name,
        Func<CancellationToken, Task<bool>> connectivityCheck,
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null,
        TimeSpan? timeout = null)
    {
        return builder.Add(new HealthCheckRegistration(
            name,
            sp => new MessageQueueHealthCheck(name, connectivityCheck, HealthCheckLoggers.For(sp)),
            failureStatus,
            tags,
            timeout));
    }
}
