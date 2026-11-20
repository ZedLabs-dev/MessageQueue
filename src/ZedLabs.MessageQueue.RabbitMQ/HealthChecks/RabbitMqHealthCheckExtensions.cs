using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using ZedLabs.MessageQueue.RabbitMQ;

namespace ZedLabs.MessageQueue.HealthChecks;

/// <summary>
/// Registers RabbitMQ health checks. Results carry no broker address and no exception text.
/// </summary>
public static class RabbitMqHealthCheckExtensions
{
    static readonly string[] DefaultRabbitMqTags = ["messagequeue", "rabbitmq"];

    /// <summary>
    /// Adds a health check for RabbitMQ connectivity that opens a connection per probe. Unless
    /// <paramref name="requireTls"/> is false, plain <c>amqp://</c> to a host other than a loopback host is refused. For
    /// a broker that requires a client certificate or a private CA, use the overload that takes
    /// <see cref="RabbitMqTlsConfig"/>, or <c>AddRabbitMqHealthCheck&lt;T&gt;()</c>.
    /// </summary>
    public static IHealthChecksBuilder AddRabbitMqHealthCheck(
        this IHealthChecksBuilder builder,
        string connectionString,
        string name = "rabbitmq",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null,
        TimeSpan? timeout = null,
        bool? requireTls = null) =>
        builder.AddRabbitMqHealthCheck(connectionString, new RabbitMqTlsConfig(), name, failureStatus, tags, timeout, requireTls);

    /// <summary>
    /// Adds a health check for RabbitMQ connectivity that opens a connection per probe with the queue's TLS settings:
    /// <paramref name="tls"/> supplies the client certificate (mutual TLS), the CA certificates the broker's certificate
    /// must chain to (<see cref="RabbitMqTlsConfig.CaCertificatePath"/>), the server name and the acceptable policy
    /// errors, under the same rules as the queue; <paramref name="requireTls"/> is the queue's <c>RequireTls</c>. One
    /// check instance serves every probe: it loads the certificates on the first probe and is disposed, with them, by
    /// the container. <paramref name="tls"/> is copied at registration. Invalid settings report Unhealthy and are logged.
    /// </summary>
    public static IHealthChecksBuilder AddRabbitMqHealthCheck(
        this IHealthChecksBuilder builder,
        string connectionString,
        RabbitMqTlsConfig tls,
        string name = "rabbitmq",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null,
        TimeSpan? timeout = null,
        bool? requireTls = null)
    {
        ArgumentNullException.ThrowIfNull(tls);
        var settings = tls with { };
        builder.Services.AddKeyedSingleton(name, (sp, _) =>
            new RabbitMqHealthCheck(connectionString, requireTls, settings) { Logger = HealthCheckLoggers.For(sp) });
        return builder.Add(new HealthCheckRegistration(
            name,
            sp => sp.GetRequiredKeyedService<RabbitMqHealthCheck>(name),
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
