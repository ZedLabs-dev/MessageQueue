using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace ESE.MessageQueue.HealthChecks;

/// <summary>
/// Opt-in health check that reports queue metrics (active, dead-letter and scheduled message counts) through
/// <see cref="ServiceBusAdministrationClient"/>. That client needs <b>Manage</b> rights, so prefer
/// <see cref="AzureServiceBusHealthCheck"/> unless the metrics are worth the extra privilege (S4). One administration
/// client is kept for every probe; the result carries counts only, never exception text or the namespace (S5).
/// </summary>
sealed class AzureServiceBusMetricsHealthCheck : IHealthCheck
{
    // Where a failed probe's exception goes (S5: never into the result). Set by the registration extensions.
    internal ILogger Logger { get; init; } = NullLogger.Instance;

    const string Healthy = "Azure Service Bus queue metrics are available.";
    const string NotFound = "Azure Service Bus queue does not exist.";
    const string Failed = "Azure Service Bus metrics health check failed.";

    readonly Func<string, CancellationToken, Task<QueueRuntimeProperties>> getProperties;
    readonly string queueName;

    public AzureServiceBusMetricsHealthCheck(string connectionString, string queueName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);
        this.queueName = queueName;
        var admin = new Lazy<ServiceBusAdministrationClient>(() => new ServiceBusAdministrationClient(connectionString));
        getProperties = async (queue, token) => (await admin.Value.GetQueueRuntimePropertiesAsync(queue, token)).Value;
    }

    /// <summary>With Entra ID identity; the identity needs a role that grants Manage on the queue.</summary>
    public AzureServiceBusMetricsHealthCheck(string fullyQualifiedNamespace, Azure.Core.TokenCredential credential, string queueName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fullyQualifiedNamespace);
        ArgumentNullException.ThrowIfNull(credential);
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);
        this.queueName = queueName;
        var admin = new Lazy<ServiceBusAdministrationClient>(() => new ServiceBusAdministrationClient(fullyQualifiedNamespace, credential));
        getProperties = async (queue, token) => (await admin.Value.GetQueueRuntimePropertiesAsync(queue, token)).Value;
    }

    // Test seam: the administration call.
    internal AzureServiceBusMetricsHealthCheck(string queueName, Func<string, CancellationToken, Task<QueueRuntimeProperties>> getProperties)
    {
        this.queueName = queueName;
        this.getProperties = getProperties;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            var properties = await getProperties(queueName, cancellationToken);
            return HealthCheckResult.Healthy(Healthy, new Dictionary<string, object>
            {
                ["activeMessageCount"] = properties.ActiveMessageCount,
                ["deadLetterMessageCount"] = properties.DeadLetterMessageCount,
                ["scheduledMessageCount"] = properties.ScheduledMessageCount
            });
        }
        catch (Azure.Messaging.ServiceBus.ServiceBusException ex) when (ex.Reason == Azure.Messaging.ServiceBus.ServiceBusFailureReason.MessagingEntityNotFound)
        {
            HealthCheckLog.ProbeFailed(Logger, "azureservicebus-metrics", ex);
            return HealthCheckResult.Unhealthy(NotFound);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            HealthCheckLog.ProbeFailed(Logger, "azureservicebus-metrics", ex);
            return HealthCheckResult.Unhealthy(Failed);
        }
    }
}
