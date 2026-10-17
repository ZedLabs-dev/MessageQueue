using ESE.MessageQueue.Configuration;

namespace ESE.MessageQueue.Samples.LoopbackProvider;

public sealed record LoopbackConfig : MessageQueueConfig
{
    public const string ProviderName = "Loopback";

    public override string Provider => ProviderName;

    public int MaxDeadLetters { get; set; } = 1000;
}
