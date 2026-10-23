using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Configuration;
using ESE.MessageQueue.Retry;
using Microsoft.Extensions.Logging;

namespace ESE.MessageQueue.Processing;

// Capability honesty (§3a.5, ADR 0020): say so when the configuration asks for something the provider cannot do, and
// fail instead when the application requires it (RequireDelayedRedelivery).
static partial class CapabilityChecks
{
    // A retry policy that would wait between attempts.
    public static bool HasBackoff(RetryPolicy retry) =>
        retry.MaxRetries > 0 && (retry.InitialDelay > TimeSpan.Zero || retry.CustomDelayCalculator is not null);

    // The failure when the application requires delayed redelivery that the provider or entity cannot give; else null.
    public static string? UnsupportedRequiredBackoff(bool delayedRedelivery, MessageQueueConfig config) =>
        config.RequireDelayedRedelivery && !delayedRedelivery && config.Retry is { } retry && HasBackoff(retry)
            ? $"RequireDelayedRedelivery is true, but this provider or entity cannot delay a retry (Capabilities.DelayedRedelivery = false) " +
              $"and the retry policy has a backoff ({retry.InitialDelay} to {retry.MaxDelay}): use an entity that can (for Azure Service Bus a " +
              "queue, not a topic subscription), set Retry:InitialDelay to 0, or set RequireDelayedRedelivery = false to retry without the delay"
            : null;

    public static void WarnIfBackoffUnsupported(MessageQueueCapabilities capabilities, MessageQueueConfig config, ILogger logger)
    {
        var retry = config.Retry;
        if (!capabilities.DelayedRedelivery && HasBackoff(retry))
            BackoffUnsupported(new SafeLogger(logger), config.QueueName, retry.InitialDelay, retry.MaxDelay);
    }

    [LoggerMessage(EventId = 1101, Level = LogLevel.Warning,
        Message = "Queue {QueueName}: this provider or entity has no broker-side delayed redelivery (Capabilities.DelayedRedelivery = false), " +
            "so the retry backoff ({InitialDelay} to {MaxDelay}) is applied in process or not at all")]
    static partial void BackoffUnsupported(ILogger logger, string queueName, TimeSpan initialDelay, TimeSpan maxDelay);
}
