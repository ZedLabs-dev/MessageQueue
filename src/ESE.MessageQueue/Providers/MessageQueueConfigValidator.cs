using ESE.MessageQueue.Configuration;
using Microsoft.Extensions.Options;

namespace ESE.MessageQueue.Providers;

/// <summary>
/// Base for a provider's config validator: checks the portable rules every provider shares (retry policy ranges,
/// serialization format) and then the provider's own rules. DataAnnotations on the config are checked separately by
/// <c>ValidateDataAnnotations</c>. Stateless and thread-safe as long as <see cref="ValidateProvider"/> is.
/// </summary>
/// <typeparam name="TConfig">The provider's config type.</typeparam>
public abstract class MessageQueueConfigValidator<TConfig> : IValidateOptions<TConfig> where TConfig : MessageQueueConfig
{
    /// <summary>Validates <paramref name="options"/>; every failure is reported, not just the first.</summary>
    public ValidateOptionsResult Validate(string? name, TConfig options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var errors = MessageQueueConfigRules.Validate(options);
        ValidateProvider(options, errors);
        return MessageQueueConfigRules.ToResult(errors);
    }

    /// <summary>
    /// Runs the same checks as startup validation (DataAnnotations, the base rules and <see cref="ValidateProvider"/>)
    /// for a queue constructed directly instead of through <c>AddMessageQueue&lt;T&gt;</c>.
    /// </summary>
    /// <exception cref="OptionsValidationException">The config is invalid; the exception lists every failure.</exception>
    public void ThrowIfInvalid(TConfig config) => ConfigValidation.ThrowIfInvalid(config, this);

    /// <summary>Adds a message to <paramref name="errors"/> for each provider-specific rule <paramref name="options"/> breaks.</summary>
    protected abstract void ValidateProvider(TConfig options, ICollection<string> errors);
}
