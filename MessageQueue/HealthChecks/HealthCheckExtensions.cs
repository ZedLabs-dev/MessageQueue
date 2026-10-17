using ESE.MessageQueue.AzureServiceBus;
using ESE.MessageQueue.RabbitMQ;
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
        TimeSpan? timeout = null,
        bool? requireTls = null)
    {
        return builder.Add(new HealthCheckRegistration(
            name,
            _ => new RabbitMqHealthCheck(connectionString, requireTls),
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
                    $"No RabbitMQ queue is registered for {typeof(T).FullName}; register it with AddMessageQueue<T> and Provider = RabbitMq")),
            failureStatus,
            tags ?? DefaultRabbitMqTags,
            timeout ?? TimeSpan.FromSeconds(5)));
    }

    /// <summary>
    /// Adds the least-privilege Azure Service Bus health check (a peek; Listen rights only). One client and receiver
    /// serve every probe and are disposed with the container. Prefer <c>AddAzureServiceBusHealthCheck&lt;T&gt;()</c>,
    /// which reuses the registered queue's client and identity.
    /// </summary>
    public static IHealthChecksBuilder AddAzureServiceBusHealthCheck(
        this IHealthChecksBuilder builder,
        string connectionString,
        string queueName,
        string name = "azureservicebus",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null,
        TimeSpan? timeout = null,
        string? subscriptionName = null)
    {
        builder.Services.AddKeyedSingleton(name, (_, _) => new AzureServiceBusHealthCheck(connectionString, queueName, subscriptionName));
        return builder.Add(new HealthCheckRegistration(
            name,
            sp => sp.GetRequiredKeyedService<AzureServiceBusHealthCheck>(name),
            failureStatus,
            tags ?? DefaultAzureServiceBusTags,
            timeout ?? TimeSpan.FromSeconds(10)));
    }

    /// <summary>
    /// Adds a health check for the Azure Service Bus queue registered for <typeparamref name="T"/> with
    /// <c>AddMessageQueue&lt;T&gt;</c>. It reuses that queue's client (and identity): Unhealthy while the queue's
    /// processor is not running, otherwise a peek on the consumed queue or subscription (Listen rights only).
    /// </summary>
    public static IHealthChecksBuilder AddAzureServiceBusHealthCheck<T>(
        this IHealthChecksBuilder builder,
        string name = "azureservicebus",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null,
        TimeSpan? timeout = null) where T : class
    {
        return builder.Add(new HealthCheckRegistration(
            name,
            sp => new AzureServiceBusHealthCheck(sp.GetService<AzureServiceBusMessageQueue<T>>()
                ?? throw new InvalidOperationException(
                    $"No Azure Service Bus queue is registered for {typeof(T).FullName}; register it with AddMessageQueue<T> and Provider = AzureServiceBus")),
            failureStatus,
            tags ?? DefaultAzureServiceBusTags,
            timeout ?? TimeSpan.FromSeconds(10)));
    }

    /// <summary>
    /// Adds the opt-in metrics check (message counts through the administration client). It needs <b>Manage</b>
    /// rights; use it only where that privilege is acceptable.
    /// </summary>
    public static IHealthChecksBuilder AddAzureServiceBusMetricsHealthCheck(
        this IHealthChecksBuilder builder,
        string connectionString,
        string queueName,
        string name = "azureservicebus-metrics",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null,
        TimeSpan? timeout = null)
    {
        builder.Services.AddKeyedSingleton(name, (_, _) => new AzureServiceBusMetricsHealthCheck(connectionString, queueName));
        return builder.Add(new HealthCheckRegistration(
            name,
            sp => sp.GetRequiredKeyedService<AzureServiceBusMetricsHealthCheck>(name),
            failureStatus,
            tags ?? DefaultAzureServiceBusTags,
            timeout ?? TimeSpan.FromSeconds(10)));
    }

    /// <summary>
    /// Adds the least-privilege Azure Service Bus health check with Entra ID identity (Data Receiver role).
    /// </summary>
    public static IHealthChecksBuilder AddAzureServiceBusHealthCheck(
        this IHealthChecksBuilder builder,
        string fullyQualifiedNamespace,
        Azure.Core.TokenCredential credential,
        string queueName,
        string name = "azureservicebus",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null,
        TimeSpan? timeout = null,
        string? subscriptionName = null)
    {
        builder.Services.AddKeyedSingleton(name, (_, _) => new AzureServiceBusHealthCheck(fullyQualifiedNamespace, credential, queueName, subscriptionName));
        return builder.Add(new HealthCheckRegistration(
            name,
            sp => sp.GetRequiredKeyedService<AzureServiceBusHealthCheck>(name),
            failureStatus,
            tags ?? DefaultAzureServiceBusTags,
            timeout ?? TimeSpan.FromSeconds(10)));
    }

    /// <summary>The opt-in metrics check with Entra ID identity (needs Manage rights).</summary>
    public static IHealthChecksBuilder AddAzureServiceBusMetricsHealthCheck(
        this IHealthChecksBuilder builder,
        string fullyQualifiedNamespace,
        Azure.Core.TokenCredential credential,
        string queueName,
        string name = "azureservicebus-metrics",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null,
        TimeSpan? timeout = null)
    {
        builder.Services.AddKeyedSingleton(name, (_, _) => new AzureServiceBusMetricsHealthCheck(fullyQualifiedNamespace, credential, queueName));
        return builder.Add(new HealthCheckRegistration(
            name,
            sp => sp.GetRequiredKeyedService<AzureServiceBusMetricsHealthCheck>(name),
            failureStatus,
            tags ?? DefaultAzureServiceBusTags,
            timeout ?? TimeSpan.FromSeconds(10)));
    }
}
