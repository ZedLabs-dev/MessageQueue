using ESE.MessageQueue.RabbitMQ;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ESE.MessageQueue.HealthChecks;

/// <summary>
/// Registers RabbitMQ health checks. Results carry no broker address and no exception text.
/// </summary>
public static class RabbitMqHealthCheckExtensions
{
    static readonly string[] DefaultRabbitMqTags = ["messagequeue", "rabbitmq"];

    /// <summary>
    /// Adds a health check for RabbitMQ connectivity.
    /// </summary>
    public static IHealthChecksBuilder AddRabbitMqHealthCheck(
        this IHealthChecksBuilder builder,
        string connectionString,
        string name = "rabbitmq",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null,
        TimeSpan? timeout = null,
        bool? requireTls = null)
    {
        return builder.Add(new HealthCheckRegistration(
            name,
            sp => new RabbitMqHealthCheck(connectionString, requireTls) { Logger = HealthCheckLoggers.For(sp) },
            failureStatus,
            tags ?? DefaultRabbitMqTags,
            timeout ?? TimeSpan.FromSeconds(5)));
    }

    /// <summary>
    /// Adds a health check for the RabbitMQ queue registered for <typeparamref name="T"/> with
    /// <c>AddMessageQueue&lt;T&gt;</c>. It reuses that queue's connection (connecting it if needed) instead of opening
    /// a connection per probe.
    /// </summary>
    public static IHealthChecksBuilder AddRabbitMqHealthCheck<T>(
        this IHealthChecksBuilder builder,
        string name = "rabbitmq",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null,
        TimeSpan? timeout = null) where T : class
    {
        return builder.Add(new HealthCheckRegistration(
            name,
            sp => new RabbitMqHealthCheck(sp.GetService<RabbitMqMessageQueue<T>>()
                ?? throw new InvalidOperationException(
                    $"No RabbitMQ queue is registered for {typeof(T).FullName}; register it with AddMessageQueue<T> and Provider = RabbitMq"))
            { Logger = HealthCheckLoggers.For(sp) },
            failureStatus,
            tags ?? DefaultRabbitMqTags,
            timeout ?? TimeSpan.FromSeconds(5)));
    }
}
