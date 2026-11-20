using ZedLabs.MessageQueue.Configuration;

namespace ZedLabs.MessageQueue.InMemory;

public sealed record InMemoryConfig : MessageQueueConfig
{
    public static readonly TimeSpan DefaultShutdownTimeout = TimeSpan.FromSeconds(25);

    public const string ProviderName = "InMemory";

    public override string Provider => ProviderName;

    public TimeSpan ShutdownTimeout { get; set; } = DefaultShutdownTimeout;
}
