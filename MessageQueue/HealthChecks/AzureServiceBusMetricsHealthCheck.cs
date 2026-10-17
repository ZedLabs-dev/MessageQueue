using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ESE.MessageQueue.HealthChecks;

/// <summary>
/// Opt-in health check that reports queue metrics (active, dead-letter and scheduled message counts) through
/// <see cref="ServiceBusAdministrationClient"/>. That client needs <b>Manage</b> rights, so prefer
/// <see cref="AzureServiceBusHealthCheck"/> unless the metrics are worth the extra privilege (S4). One administration
/// client is kept for every probe; the result carries counts only, never exception text or the namespace (S5).
/// </summary>
public sealed class AzureServiceBusMetricsHealthCheck : IHealthCheck
{
    const string Healthy = "Azure Service Bus queue metrics are available.";
    const string NotFound = "Azure Service Bus queue does not exist.";
    const string Failed = "Azure Service Bus metrics health check failed.";

    readonly Lazy<ServiceBusAdministrationClient> admin;
    readonly string queueName;

    public AzureServiceBusMetricsHealthCheck(string connectionString, string queueName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);
        this.queueName = queueName;
        admin = new Lazy<ServiceBusAdministrationClient>(() => new ServiceBusAdministrationClient(connectionString));
    }

    /// <summary>With Entra ID identity; the identity needs a role that grants Manage on the queue.</summary>
    public AzureServiceBusMetricsHealthCheck(string fullyQualifiedNamespace, Azure.Core.TokenCredential credential, string queueName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fullyQualifiedNamespace);
        ArgumentNullException.ThrowIfNull(credential);
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);
        this.queueName = queueName;
        admin = new Lazy<ServiceBusAdministrationClient>(() => new ServiceBusAdministrationClient(fullyQualifiedNamespace, credential));
    }

    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            var properties = (await admin.Value.GetQueueRuntimePropertiesAsync(queueName, cancellationToken)).Value;
            return HealthCheckResult.Healthy(Healthy, new Dictionary<string, object>
            {
                ["activeMessageCount"] = properties.ActiveMessageCount,
                ["deadLetterMessageCount"] = properties.DeadLetterMessageCount,
                ["scheduledMessageCount"] = properties.ScheduledMessageCount
            });
        }
        catch (Azure.Messaging.ServiceBus.ServiceBusException ex) when (ex.Reason == Azure.Messaging.ServiceBus.ServiceBusFailureReason.MessagingEntityNotFound)
        {
            return HealthCheckResult.Unhealthy(NotFound, ex);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            return HealthCheckResult.Unhealthy(Failed, ex);
        }
    }
}
