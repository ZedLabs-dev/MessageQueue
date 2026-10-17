using System.ComponentModel.DataAnnotations;
using System.Reflection;
using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;

namespace ESE.MessageQueue.Configuration;

static class ConfigValidation
{
    // The same rules as startup validation (DataAnnotations + the provider's validator), for queues constructed
    // directly instead of through AddMessageQueue.
    public static void ThrowIfInvalid<TConfig>(TConfig config, IValidateOptions<TConfig> validator) where TConfig : class
    {
        ArgumentNullException.ThrowIfNull(config);

        var failures = new List<string>();
        var annotations = new List<ValidationResult>();
        if (!Validator.TryValidateObject(config, new ValidationContext(config), annotations, validateAllProperties: true))
            failures.AddRange(annotations.Select(r => r.ErrorMessage ?? "Invalid value"));

        var result = validator.Validate(Options.DefaultName, config);
        if (result.Failed)
            failures.AddRange(result.Failures);

        if (failures.Count > 0)
            throw new OptionsValidationException(Options.DefaultName, typeof(TConfig), failures);
    }

    // Enum settings must be one of the enum's names. The binder would also accept numbers ("1", "+1") and
    // flag combinations ("A,B"), which silently select some other member.
    public static bool IsEnumName<TEnum>(string? raw) where TEnum : struct, Enum =>
        raw is not null && Enum.GetNames<TEnum>().Any(n => string.Equals(n, raw.Trim(), StringComparison.OrdinalIgnoreCase));

    // Prints a config record like the compiler-generated ToString, with secrets redacted.
    public static bool PrintMembers(object config, StringBuilder builder, params string[] secrets)
    {
        var first = true;
        foreach (var property in config.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (property.GetIndexParameters().Length > 0 || property.GetMethod is null)
                continue;

            if (!first)
                builder.Append(", ");
            first = false;

            var value = property.GetValue(config);
            builder.Append(property.Name).Append(" = ");
            if (secrets.Contains(property.Name))
                builder.Append(value is string { Length: > 0 } ? "***" : "");
            else
                builder.Append(value);
        }
        return true;
    }
}

// Startup check for enum settings, which the binder parses too leniently (see ConfigValidation.IsEnumName).
sealed class EnumNameValidator<TConfig> : IValidateOptions<TConfig> where TConfig : MessageQueueConfig
{
    readonly string name;
    readonly IConfiguration section;

    public EnumNameValidator(string name, IConfiguration section)
    {
        this.name = name;
        this.section = section;
    }

    public ValidateOptionsResult Validate(string? optionsName, TConfig options)
    {
        if (optionsName != name)
            return ValidateOptionsResult.Skip;

        var errors = new List<string>();
        Check<Serialization.SerializationFormat>("SerializationFormat", errors);
        Check<Retry.RetryStrategy>("Retry:Strategy", errors);
        return errors.Count > 0 ? ValidateOptionsResult.Fail(errors) : ValidateOptionsResult.Success;
    }

    void Check<TEnum>(string key, List<string> errors) where TEnum : struct, Enum
    {
        var raw = section[key];
        if (raw is not null && !ConfigValidation.IsEnumName<TEnum>(raw))
            errors.Add($"{key} '{raw}' is not supported. Supported values: {string.Join(", ", Enum.GetNames<TEnum>())}");
    }
}
