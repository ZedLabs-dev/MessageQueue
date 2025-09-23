using AgentReview.MessageQueue.Configuration;

namespace AgentReview.MessageQueue.RabbitMQ;

public record RabbitMqConfig : MessageQueueConfig
{
    public override MessageQueueProvider Provider => MessageQueueProvider.RabbitMq;
    public bool UseQuorumQueues { get; set; } = true;
    public int QuorumInitialGroupSize { get; set; } = 3;
    public bool EnablePublisherConfirms { get; set; } = true;
    public int PrefetchCount { get; set; } = 10;
    public bool EnableClusterMode { get; set; } = false;
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(60);
    public TimeSpan NetworkRecoveryInterval { get; set; } = TimeSpan.FromSeconds(10);
}