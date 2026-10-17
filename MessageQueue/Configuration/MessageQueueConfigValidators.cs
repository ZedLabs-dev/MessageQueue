using ESE.MessageQueue.AzureServiceBus;
using ESE.MessageQueue.InMemory;
using ESE.MessageQueue.RabbitMQ;
using ESE.MessageQueue.Retry;
using ESE.MessageQueue.Serialization;
using Microsoft.Extensions.Options;

namespace ESE.MessageQueue.Configuration;

// Rules DataAnnotations cannot express. [Required]/[Range] on the config records are checked by ValidateDataAnnotations.
static class MessageQueueConfigRules
{
    public static List<string> Validate(MessageQueueConfig config)
    {
        var errors = new List<string>();

        if (!Enum.IsDefined(config.SerializationFormat))
            errors.Add($"SerializationFormat '{config.SerializationFormat}' is not supported");

        if (config.Retry is { } retry)
            ValidateRetry(retry, errors);

        return errors;
    }

    static void ValidateRetry(RetryPolicy retry, List<string> errors)
    {
        if (retry.MaxRetries is < 0 or > RetryPolicy.MaxRetriesLimit)
            errors.Add($"Retry:MaxRetries must be between 0 and {RetryPolicy.MaxRetriesLimit}");

        if (retry.InitialDelay < TimeSpan.Zero)
            errors.Add("Retry:InitialDelay must be >= 0");

        if (retry.MaxDelay < retry.InitialDelay)
            errors.Add("Retry:MaxDelay must be >= Retry:InitialDelay");

        if (retry.MaxDelay > RetryPolicy.MaxAllowedDelay)
            errors.Add($"Retry:MaxDelay must be <= {RetryPolicy.MaxAllowedDelay}");

        if (retry.BackoffMultiplier is < 1.0 or > 100.0 || double.IsNaN(retry.BackoffMultiplier))
            errors.Add("Retry:BackoffMultiplier must be between 1 and 100");

        if (retry.JitterFactor is < 0.0 or > 1.0 || double.IsNaN(retry.JitterFactor))
            errors.Add("Retry:JitterFactor must be between 0 and 1");

        if (!Enum.IsDefined(retry.Strategy))
            errors.Add($"Retry:Strategy '{retry.Strategy}' is not supported");

        if (retry.Strategy == RetryStrategy.Custom && retry.CustomDelayCalculator is null)
            errors.Add("Retry:Strategy Custom requires a CustomDelayCalculator");
    }

    public static ValidateOptionsResult ToResult(List<string> errors) =>
        errors.Count > 0 ? ValidateOptionsResult.Fail(errors) : ValidateOptionsResult.Success;
}

public class RabbitMqConfigValidator : IValidateOptions<RabbitMqConfig>
{
    public ValidateOptionsResult Validate(string? name, RabbitMqConfig options)
    {
        var errors = MessageQueueConfigRules.Validate(options);
        errors.AddRange(RabbitMqConnections.Validate(options));
        RabbitMqNames.Validate(options, errors);

        if (options.ConfirmTimeout <= TimeSpan.Zero)
            errors.Add("ConfirmTimeout must be > 0");

        if (options.HeartbeatInterval < TimeSpan.Zero)
            errors.Add("HeartbeatInterval must be >= 0");

        if (options.NetworkRecoveryInterval <= TimeSpan.Zero)
            errors.Add("NetworkRecoveryInterval must be > 0");

        if (options.ShutdownTimeout <= TimeSpan.Zero)
            errors.Add("ShutdownTimeout must be > 0");

        return MessageQueueConfigRules.ToResult(errors);
    }
}

public class AzureServiceBusConfigValidator : IValidateOptions<AzureServiceBusConfig>
{
    public ValidateOptionsResult Validate(string? name, AzureServiceBusConfig options)
    {
        var errors = MessageQueueConfigRules.Validate(options);

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

        return MessageQueueConfigRules.ToResult(errors);
    }
}

public class InMemoryConfigValidator : IValidateOptions<InMemoryConfig>
{
    public ValidateOptionsResult Validate(string? name, InMemoryConfig options)
    {
        var errors = MessageQueueConfigRules.Validate(options);

        if (options.ShutdownTimeout <= TimeSpan.Zero)
            errors.Add("ShutdownTimeout must be > 0");

        return MessageQueueConfigRules.ToResult(errors);
    }
}
