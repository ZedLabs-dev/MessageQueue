using ESE.MessageQueue.Configuration;

namespace ESE.MessageQueue.InMemory;

public record InMemoryConfig : MessageQueueConfig
{
    public static readonly TimeSpan DefaultShutdownTimeout = TimeSpan.FromSeconds(30);

    public override MessageQueueProvider Provider => MessageQueueProvider.InMemory;

    public TimeSpan ShutdownTimeout { get; set; } = DefaultShutdownTimeout;
}
