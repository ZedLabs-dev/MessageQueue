using ESE.MessageQueue.Configuration;

namespace ESE.MessageQueue.RabbitMQ;

public record RabbitMqConfig : MessageQueueConfig
{
    public override MessageQueueProvider Provider => MessageQueueProvider.RabbitMq;

    // Queue settings
    public bool UseQuorumQueues { get; set; } = true;
    public int QuorumInitialGroupSize { get; set; } = 3;
    public int PrefetchCount { get; set; } = 10;

    // Exchange settings
    public string ExchangeName { get; set; } = string.Empty;
    public string ExchangeType { get; set; } = "direct";

    // Publisher confirms and channel pool settings
    public bool EnablePublisherConfirms { get; set; } = true;
    public int PublisherChannelPoolSize { get; set; } = 2;
    public int MaxOutstandingConfirms { get; set; } = 1000;
    public TimeSpan ConfirmTimeout { get; set; } = TimeSpan.FromSeconds(30);

    // Connection settings
    public bool EnableClusterMode { get; set; } = false;
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(60);
    public TimeSpan NetworkRecoveryInterval { get; set; } = TimeSpan.FromSeconds(10);
}