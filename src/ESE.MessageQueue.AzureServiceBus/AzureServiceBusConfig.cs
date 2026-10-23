using System.ComponentModel.DataAnnotations;
using System.Text;
using ESE.MessageQueue.Configuration;

namespace ESE.MessageQueue.AzureServiceBus;

public sealed record AzureServiceBusConfig : MessageQueueConfig
{
    public const string ProviderName = "AzureServiceBus";

    public override string Provider => ProviderName;

    public static readonly TimeSpan DefaultShutdownTimeout = TimeSpan.FromSeconds(25);

    public string ConnectionString { get; set; } = string.Empty;

    public string FullyQualifiedNamespace { get; set; } = string.Empty;

    public TimeSpan MessageTimeToLive { get; set; } = TimeSpan.FromDays(14);

    [Range(0, int.MaxValue)]
    public int PrefetchCount { get; set; } = 10;

    public TimeSpan MaxAutoLockRenewalDuration { get; set; } = TimeSpan.FromMinutes(5);

    public bool EnableBatchedOperations { get; set; } = true;

    [Range(1, int.MaxValue)]
    public int MaxBatchSize { get; set; } = 100;

    public string? TopicName { get; set; }

    public string? SubscriptionName { get; set; }

    public TimeSpan SendTimeout { get; set; } = TimeSpan.FromSeconds(30);

    public TimeSpan ShutdownTimeout { get; set; } = DefaultShutdownTimeout;

    public bool TransactionalRetries { get; set; } = true;

    protected override bool PrintMembers(StringBuilder builder) =>
        ConfigValidation.PrintMembers(this, builder, nameof(ConnectionString));

    internal bool UsesSubscription => !string.IsNullOrWhiteSpace(TopicName);

    // Queue mode delays a retry with a scheduled copy; a subscription can only abandon (ADR 0009).
    internal bool SupportsDelayedRedelivery => !UsesSubscription;
}
