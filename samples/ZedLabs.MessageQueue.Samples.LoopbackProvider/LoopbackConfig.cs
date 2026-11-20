using ZedLabs.MessageQueue.Configuration;

namespace ZedLabs.MessageQueue.Samples.LoopbackProvider;

public sealed record LoopbackConfig : MessageQueueConfig
{
    public const string ProviderName = "Loopback";

    public override string Provider => ProviderName;

    public int MaxDeadLetters { get; set; } = 1000;
}
