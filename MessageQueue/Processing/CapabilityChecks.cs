using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Configuration;
using Microsoft.Extensions.Logging;

namespace ESE.MessageQueue.Processing;

// Capability honesty (§3a.5): say so when the configuration asks for something the provider cannot do yet.
static partial class CapabilityChecks
{
    public static void WarnIfBackoffUnsupported(MessageQueueCapabilities capabilities, MessageQueueConfig config, ILogger logger)
    {
        var retry = config.Retry;
        var hasBackoff = retry.MaxRetries > 0 && (retry.InitialDelay > TimeSpan.Zero || retry.CustomDelayCalculator is not null);
        if (!capabilities.DelayedRedelivery && hasBackoff)
            BackoffUnsupported(new SafeLogger(logger), config.QueueName, retry.InitialDelay, retry.MaxDelay);
    }

    [LoggerMessage(EventId = 1101, Level = LogLevel.Warning,
        Message = "Queue {QueueName}: this provider has no broker-side delayed redelivery yet (Capabilities.DelayedRedelivery = false), " +
            "so the retry backoff ({InitialDelay} to {MaxDelay}) is applied in process or not at all")]
    static partial void BackoffUnsupported(ILogger logger, string queueName, TimeSpan initialDelay, TimeSpan maxDelay);
}
