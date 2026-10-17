using ESE.MessageQueue.InMemory;
using ESE.MessageQueue.Providers;
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

// The in-memory provider's rules on top of the shared ones.
sealed class InMemoryConfigValidator : MessageQueueConfigValidator<InMemoryConfig>
{
    protected override void ValidateProvider(InMemoryConfig options, ICollection<string> errors)
    {
        if (options.ShutdownTimeout <= TimeSpan.Zero)
            errors.Add("ShutdownTimeout must be > 0");
    }
}
