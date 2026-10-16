using System.ComponentModel.DataAnnotations;
using System.Text;
using ESE.MessageQueue.Configuration;

namespace ESE.MessageQueue.RabbitMQ;

public record RabbitMqConfig : MessageQueueConfig
{
    public override MessageQueueProvider Provider => MessageQueueProvider.RabbitMq;

    [Required]
    public string ConnectionString { get; set; } = string.Empty;

    public string DeadLetterQueueName { get; set; } = string.Empty;

    public bool UseQuorumQueues { get; set; } = true;

    [Range(1, int.MaxValue)]
    public int QuorumInitialGroupSize { get; set; } = 3;

    [Range(1, ushort.MaxValue)]
    public int PrefetchCount { get; set; } = 10;

    public string ExchangeName { get; set; } = string.Empty;

    [Required]
    public string ExchangeType { get; set; } = "direct";

    public bool EnablePublisherConfirms { get; set; } = true;

    [Range(1, 64)]
    public int PublisherChannelPoolSize { get; set; } = 2;

    [Range(1, int.MaxValue)]
    public int MaxOutstandingConfirms { get; set; } = 1000;

    public TimeSpan ConfirmTimeout { get; set; } = TimeSpan.FromSeconds(30);

    public bool EnableClusterMode { get; set; }

    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(60);

    public TimeSpan NetworkRecoveryInterval { get; set; } = TimeSpan.FromSeconds(10);

    protected override bool PrintMembers(StringBuilder builder) =>
        ConfigValidation.PrintMembers(this, builder, nameof(ConnectionString));

    internal string EffectiveDeadLetterQueueName =>
        string.IsNullOrWhiteSpace(DeadLetterQueueName) ? $"{QueueName}.deadletter" : DeadLetterQueueName;
}
