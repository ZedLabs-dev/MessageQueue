using ESE.MessageQueue.Configuration;

namespace ESE.MessageQueue.InMemory;

public sealed record InMemoryConfig : MessageQueueConfig
{
    public static readonly TimeSpan DefaultShutdownTimeout = TimeSpan.FromSeconds(30);

    public const string ProviderName = "InMemory";

    public override string Provider => ProviderName;

    public TimeSpan ShutdownTimeout { get; set; } = DefaultShutdownTimeout;
}
