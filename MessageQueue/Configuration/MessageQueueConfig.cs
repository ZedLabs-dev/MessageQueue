using ESE.MessageQueue.Serialization;

namespace ESE.MessageQueue.Configuration;

public abstract record MessageQueueConfig
{
    public abstract MessageQueueProvider Provider { get; }
    public string ConnectionString { get; set; } = string.Empty;
    public string QueueName { get; set; } = string.Empty;
    public int RetryAttempts { get; set; } = 3;
    public TimeSpan RetryDelay { get; init; } = TimeSpan.FromSeconds(5);
    public bool EnableDeadLetterQueue { get; set; } = true;
    public string DeadLetterQueueName { get; init; } = string.Empty;

    /// <summary>
    /// Serialization format for messages. Default is JSON.
    /// Use MessagePack for high-throughput scenarios requiring smaller payloads and faster serialization.
    /// </summary>
    public SerializationFormat SerializationFormat { get; set; } = SerializationFormat.Json;
}
