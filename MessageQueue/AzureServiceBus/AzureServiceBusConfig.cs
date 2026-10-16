using ESE.MessageQueue.Configuration;

namespace ESE.MessageQueue.AzureServiceBus;

public record AzureServiceBusConfig : MessageQueueConfig
{
    public override MessageQueueProvider Provider => MessageQueueProvider.AzureServiceBus;
    public TimeSpan MessageTimeToLive { get; set; } = TimeSpan.FromDays(14);
    public int MaxConcurrentCalls { get; set; } = 10;
    public int PrefetchCount { get; set; } = 10;
    public TimeSpan MaxAutoLockRenewalDuration { get; set; } = TimeSpan.FromMinutes(5);
    public bool EnableBatchedOperations { get; set; } = true;
    public int MaxBatchSize { get; set; } = 100;
    public TimeSpan BatchFlushInterval { get; set; } = TimeSpan.FromSeconds(1);
    public bool EnablePartitioning { get; set; }
    public bool RequiresDuplicateDetection { get; set; }
    public TimeSpan DuplicateDetectionHistoryTimeWindow { get; set; } = TimeSpan.FromMinutes(10);
    public string? TopicName { get; set; }
    public string? SubscriptionName { get; set; }
    public int SenderPoolSize { get; set; } = 2;
    public int MaxConcurrentSends { get; set; } = 1000;
    public TimeSpan SendTimeout { get; set; } = TimeSpan.FromSeconds(30);
}
