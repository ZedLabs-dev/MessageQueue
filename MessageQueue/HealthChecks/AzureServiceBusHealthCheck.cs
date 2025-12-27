using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ESE.MessageQueue.HealthChecks;

/// <summary>
/// Health check for Azure Service Bus connectivity.
/// </summary>
public class AzureServiceBusHealthCheck : IHealthCheck
{
    readonly string connectionString;
    readonly string queueName;

    public AzureServiceBusHealthCheck(string connectionString, string queueName)
    {
        this.connectionString = connectionString;
        this.queueName = queueName;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var adminClient = new ServiceBusAdministrationClient(connectionString);
            var queueProperties = await adminClient.GetQueueRuntimePropertiesAsync(queueName, cancellationToken);

            return HealthCheckResult.Healthy(
                $"Azure Service Bus queue '{queueName}' is accessible.",
                new Dictionary<string, object>
                {
                    ["queue"] = queueName,
                    ["activeMessageCount"] = queueProperties.Value.ActiveMessageCount,
                    ["deadLetterMessageCount"] = queueProperties.Value.DeadLetterMessageCount,
                    ["scheduledMessageCount"] = queueProperties.Value.ScheduledMessageCount
                });
        }
        catch (ServiceBusException ex) when (ex.Reason == ServiceBusFailureReason.MessagingEntityNotFound)
        {
            return HealthCheckResult.Unhealthy(
                $"Azure Service Bus queue '{queueName}' does not exist.",
                ex,
                new Dictionary<string, object>
                {
                    ["queue"] = queueName,
                    ["error"] = "Queue not found"
                });
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy(
                $"Azure Service Bus health check failed for queue '{queueName}'.",
                ex,
                new Dictionary<string, object>
                {
                    ["queue"] = queueName,
                    ["error"] = ex.Message
                });
        }
    }
}

