using Azure.Messaging.ServiceBus;
using ESE.MessageQueue.AzureServiceBus;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ESE.MessageQueue.HealthChecks;

/// <summary>
/// Least-privilege health check for Azure Service Bus (S4): a peek, which needs only Listen rights, never Manage.
/// Registered with <c>AddAzureServiceBusHealthCheck&lt;T&gt;()</c> it reuses the queue's client and also reports
/// Unhealthy while that queue's processor is not running; with a connection string it keeps one client and receiver
/// for every probe (connection string, or namespace + TokenCredential with the Data Receiver role). The result carries no exception text, host or namespace (S5); the exception is attached for the
/// host's logging.
/// </summary>
public sealed class AzureServiceBusHealthCheck : IHealthCheck, IAsyncDisposable, IDisposable
{
    const string Healthy = "Azure Service Bus entity is reachable.";
    const string NotRunning = "Azure Service Bus processor is not running.";
    const string Failed = "Azure Service Bus health check failed.";

    readonly IAzureServiceBusProbe? probe;
    readonly Lazy<ServiceBusClient>? client;
    readonly Lazy<ServiceBusReceiver>? receiver;

    /// <summary>Peeks <paramref name="queueName"/>, or the subscription of a topic when <paramref name="subscriptionName"/> is set.</summary>
    public AzureServiceBusHealthCheck(string connectionString, string queueName, string? subscriptionName = null)
        : this(() => new ServiceBusClient(connectionString), queueName, subscriptionName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
    }

    AzureServiceBusHealthCheck(Func<ServiceBusClient> createClient, string queueName, string? subscriptionName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);
        client = new Lazy<ServiceBusClient>(createClient);
        receiver = new Lazy<ServiceBusReceiver>(() => subscriptionName is null
            ? client.Value.CreateReceiver(queueName)
            : client.Value.CreateReceiver(queueName, subscriptionName));
    }

    /// <summary>Peeks with Entra ID identity: <paramref name="fullyQualifiedNamespace"/> and <paramref name="credential"/>.</summary>
    public AzureServiceBusHealthCheck(string fullyQualifiedNamespace, Azure.Core.TokenCredential credential, string queueName, string? subscriptionName = null)
        : this(() => new ServiceBusClient(fullyQualifiedNamespace, credential), queueName, subscriptionName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fullyQualifiedNamespace);
        ArgumentNullException.ThrowIfNull(credential);
    }

    // Test seam: the receiver used for the peek.
    internal ServiceBusReceiver Receiver => receiver!.Value;

    internal AzureServiceBusHealthCheck(IAzureServiceBusProbe probe)
    {
        this.probe = probe;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            if (probe is not null)
                return await probe.IsHealthyAsync(cancellationToken) ? HealthCheckResult.Healthy(Healthy) : HealthCheckResult.Unhealthy(NotRunning);

            await receiver!.Value.PeekMessageAsync(cancellationToken: cancellationToken);
            return HealthCheckResult.Healthy(Healthy);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            return HealthCheckResult.Unhealthy(Failed, ex);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (receiver is { IsValueCreated: true })
            await receiver.Value.DisposeAsync();
        if (client is { IsValueCreated: true })
            await client.Value.DisposeAsync();
    }

    // For containers disposed synchronously; the SDK client only disposes asynchronously.
    public void Dispose() => Task.Run(() => DisposeAsync().AsTask()).GetAwaiter().GetResult();
}
