using System.ComponentModel.DataAnnotations;
using System.Text;
using ESE.MessageQueue.Configuration;

namespace ESE.MessageQueue.AzureServiceBus;

public record AzureServiceBusConfig : MessageQueueConfig
{
    public override MessageQueueProvider Provider => MessageQueueProvider.AzureServiceBus;

    [Required]
    public string ConnectionString { get; set; } = string.Empty;

    public TimeSpan MessageTimeToLive { get; set; } = TimeSpan.FromDays(14);

    [Range(0, int.MaxValue)]
    public int PrefetchCount { get; set; } = 10;

    public TimeSpan MaxAutoLockRenewalDuration { get; set; } = TimeSpan.FromMinutes(5);

    public bool EnableBatchedOperations { get; set; } = true;

    [Range(1, int.MaxValue)]
    public int MaxBatchSize { get; set; } = 100;

    public string? TopicName { get; set; }

    public string? SubscriptionName { get; set; }

    [Range(1, 64)]
    public int SenderPoolSize { get; set; } = 2;

    [Range(1, int.MaxValue)]
    public int MaxConcurrentSends { get; set; } = 1000;

    public TimeSpan SendTimeout { get; set; } = TimeSpan.FromSeconds(30);

    protected override bool PrintMembers(StringBuilder builder) =>
        ConfigValidation.PrintMembers(this, builder, nameof(ConnectionString));
}
