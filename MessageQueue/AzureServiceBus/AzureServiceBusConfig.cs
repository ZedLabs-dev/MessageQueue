using AgentReview.MessageQueue.Configuration;

namespace AgentReview.MessageQueue.AzureServiceBus;

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
    public bool EnablePartitioning { get; set; } = false;
    public bool RequiresDuplicateDetection { get; set; } = false;
    public TimeSpan DuplicateDetectionHistoryTimeWindow { get; set; } = TimeSpan.FromMinutes(10);
}