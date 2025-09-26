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
}
