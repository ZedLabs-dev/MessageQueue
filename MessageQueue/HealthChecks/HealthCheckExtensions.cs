using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ESE.MessageQueue.HealthChecks;

/// <summary>
/// Extension methods for registering message queue health checks.
/// </summary>
public static class HealthCheckExtensions
{
    static readonly string[] DefaultRabbitMqTags = ["messagequeue", "rabbitmq"];
    static readonly string[] DefaultAzureServiceBusTags = ["messagequeue", "azureservicebus"];

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
            _ => new MessageQueueHealthCheck(name, connectivityCheck),
            failureStatus,
            tags,
            timeout));
    }

    /// <summary>
    /// Adds a health check for RabbitMQ connectivity.
    /// </summary>
    public static IHealthChecksBuilder AddRabbitMqHealthCheck(
        this IHealthChecksBuilder builder,
        string connectionString,
        string name = "rabbitmq",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null,
        TimeSpan? timeout = null)
    {
        return builder.Add(new HealthCheckRegistration(
            name,
            _ => new RabbitMqHealthCheck(connectionString),
            failureStatus,
            tags ?? DefaultRabbitMqTags,
            timeout ?? TimeSpan.FromSeconds(5)));
    }

    /// <summary>
    /// Adds a health check for Azure Service Bus connectivity.
    /// </summary>
    public static IHealthChecksBuilder AddAzureServiceBusHealthCheck(
        this IHealthChecksBuilder builder,
        string connectionString,
        string queueName,
        string name = "azureservicebus",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null,
        TimeSpan? timeout = null)
    {
        return builder.Add(new HealthCheckRegistration(
            name,
            _ => new AzureServiceBusHealthCheck(connectionString, queueName),
            failureStatus,
            tags ?? DefaultAzureServiceBusTags,
            timeout ?? TimeSpan.FromSeconds(10)));
    }
}

