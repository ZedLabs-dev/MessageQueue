using System.ComponentModel.DataAnnotations;
using ESE.MessageQueue.Retry;
using ESE.MessageQueue.Serialization;

namespace ESE.MessageQueue.Configuration;

public abstract record MessageQueueConfig
{
    public const string SectionName = "MessageQueue";
    public const int DefaultMaxMessageSizeBytes = 1024 * 1024;
    public const int DefaultMaxConcurrentCalls = 10;
    public const int MaxConcurrentCallsLimit = 1000;

    public abstract MessageQueueProvider Provider { get; }

    [Required]
    public string QueueName { get; set; } = string.Empty;

    [Required]
    public RetryPolicy Retry { get; set; } = new();

    public SerializationFormat SerializationFormat { get; set; } = SerializationFormat.Json;

    public bool EnableDeadLetterQueue { get; set; } = true;

    [Range(1, int.MaxValue)]
    public int MaxMessageSizeBytes { get; set; } = DefaultMaxMessageSizeBytes;

    [Range(1, MaxConcurrentCallsLimit)]
    public int MaxConcurrentCalls { get; set; } = DefaultMaxConcurrentCalls;
}
