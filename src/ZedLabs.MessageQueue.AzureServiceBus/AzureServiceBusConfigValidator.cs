using ZedLabs.MessageQueue.Processing;
using ZedLabs.MessageQueue.Providers;

namespace ZedLabs.MessageQueue.AzureServiceBus;

// Azure Service Bus rules on top of the shared ones (entity, identity or connection string, timeouts).
sealed class AzureServiceBusConfigValidator : MessageQueueConfigValidator<AzureServiceBusConfig>
{
    protected override void ValidateProvider(AzureServiceBusConfig options, ICollection<string> errors)
    {
        if (!string.IsNullOrWhiteSpace(options.TopicName) && string.IsNullOrWhiteSpace(options.SubscriptionName))
            errors.Add("SubscriptionName is required when TopicName is set");

        if (string.IsNullOrWhiteSpace(options.TopicName) && !string.IsNullOrWhiteSpace(options.SubscriptionName))
            errors.Add("SubscriptionName requires TopicName: a subscription belongs to a topic");

        AzureServiceBusConnections.Validate(options, errors);

        if (options.SendTimeout <= TimeSpan.Zero)
            errors.Add("SendTimeout must be > 0");

        if (options.ShutdownTimeout <= TimeSpan.Zero)
            errors.Add("ShutdownTimeout must be > 0");

        if (options.MessageTimeToLive <= TimeSpan.Zero)
            errors.Add("MessageTimeToLive must be > 0");

        if (options.MaxAutoLockRenewalDuration < TimeSpan.Zero)
            errors.Add("MaxAutoLockRenewalDuration must be >= 0");

        if (CapabilityChecks.UnsupportedRequiredBackoff(options.SupportsDelayedRedelivery, options) is { } unsupported)
            errors.Add(unsupported);
    }
}
