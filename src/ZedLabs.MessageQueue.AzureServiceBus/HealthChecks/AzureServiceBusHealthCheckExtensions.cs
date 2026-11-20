using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using ZedLabs.MessageQueue.AzureServiceBus;

namespace ZedLabs.MessageQueue.HealthChecks;

/// <summary>
/// Registers Azure Service Bus health checks. The default checks peek (Listen rights only); the metrics checks need
/// Manage rights and are opt-in. Results carry no namespace and no exception text.
/// </summary>
public static class AzureServiceBusHealthCheckExtensions
{
    static readonly string[] DefaultAzureServiceBusTags = ["messagequeue", "azureservicebus"];

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
        ThrowIfInvalidConnectionString(connectionString);
        builder.Services.AddKeyedSingleton(name, (sp, _) => new AzureServiceBusHealthCheck(connectionString, queueName, subscriptionName) { Logger = HealthCheckLoggers.For(sp) });
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
                    $"No Azure Service Bus queue is registered for {typeof(T).FullName}; register it with AddMessageQueue<T> and Provider = AzureServiceBus"))
            { Logger = HealthCheckLoggers.For(sp) },
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
        ThrowIfInvalidConnectionString(connectionString);
        builder.Services.AddKeyedSingleton(name, (sp, _) => new AzureServiceBusMetricsHealthCheck(connectionString, queueName) { Logger = HealthCheckLoggers.For(sp) });
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
        ThrowIfInvalidNamespace(fullyQualifiedNamespace);
        builder.Services.AddKeyedSingleton(name, (sp, _) => new AzureServiceBusHealthCheck(fullyQualifiedNamespace, credential, queueName, subscriptionName) { Logger = HealthCheckLoggers.For(sp) });
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
        ThrowIfInvalidNamespace(fullyQualifiedNamespace);
        builder.Services.AddKeyedSingleton(name, (sp, _) => new AzureServiceBusMetricsHealthCheck(fullyQualifiedNamespace, credential, queueName) { Logger = HealthCheckLoggers.For(sp) });
        return builder.Add(new HealthCheckRegistration(
            name,
            sp => sp.GetRequiredKeyedService<AzureServiceBusMetricsHealthCheck>(name),
            failureStatus,
            tags ?? DefaultAzureServiceBusTags,
            timeout ?? TimeSpan.FromSeconds(10)));
    }

    // Validated at registration with fixed messages (S10): a bad value fails at startup, and the value is never echoed.
    static void ThrowIfInvalidConnectionString(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString) || !AzureServiceBusConnections.IsValidConnectionString(connectionString))
            throw new ArgumentException(AzureServiceBusConnections.InvalidConnectionStringMessage, nameof(connectionString));
    }

    static void ThrowIfInvalidNamespace(string fullyQualifiedNamespace)
    {
        if (!AzureServiceBusConnections.IsValidNamespace(fullyQualifiedNamespace))
            throw new ArgumentException(AzureServiceBusConnections.InvalidNamespaceMessage, nameof(fullyQualifiedNamespace));
    }
}
