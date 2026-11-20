using System.ComponentModel.DataAnnotations;
using System.Reflection;
using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;

namespace ZedLabs.MessageQueue.Configuration;

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

    // Enum settings must be one of the enum's names (for a [Flags] enum, a comma-separated list of names). The binder
    // would also accept numbers ("1", "+1") and flag combinations of a non-flags enum ("A,B"), which silently select
    // some other member.
    public static bool IsEnumName(Type enumType, string? raw)
    {
        if (raw is null)
            return false;
        var names = Enum.GetNames(enumType);
        var parts = enumType.IsDefined(typeof(FlagsAttribute), inherit: false) ? raw.Split(',') : [raw];
        return parts.All(part => names.Any(n => string.Equals(n, part.Trim(), StringComparison.OrdinalIgnoreCase)));
    }

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

// Startup check for enum settings, which the binder parses too leniently (see ConfigValidation.IsEnumName). Every enum
// property of the config type is checked, nested settings objects included (Retry:Strategy, a provider's
// Tls:ClientCertificateStoreName), so a provider's own enum settings get the same rule without registering anything.
sealed class EnumNameValidator<TConfig> : IValidateOptions<TConfig> where TConfig : MessageQueueConfig
{
    const int MaxDepth = 4;

    static readonly IReadOnlyList<(string Key, Type EnumType)> Settings = [.. EnumSettings(typeof(TConfig), "", 0)];

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
        foreach (var (key, enumType) in Settings)
        {
            var raw = section[key];
            if (raw is not null && !ConfigValidation.IsEnumName(enumType, raw))
                errors.Add($"{key} '{raw}' is not supported. Supported values: {string.Join(", ", Enum.GetNames(enumType))}");
        }
        return errors.Count > 0 ? ValidateOptionsResult.Fail(errors) : ValidateOptionsResult.Success;
    }

    // The configuration keys of every bindable enum property, recursing into nested settings classes.
    static IEnumerable<(string Key, Type EnumType)> EnumSettings(Type type, string prefix, int depth)
    {
        foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (property.GetIndexParameters().Length > 0 || property.SetMethod is not { IsPublic: true })
                continue;

            var propertyType = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;
            if (propertyType.IsEnum)
            {
                yield return (prefix + property.Name, propertyType);
            }
            else if (depth < MaxDepth && propertyType.IsClass && !propertyType.IsAbstract && propertyType != typeof(string)
                && !typeof(Delegate).IsAssignableFrom(propertyType))
            {
                foreach (var nested in EnumSettings(propertyType, $"{prefix}{property.Name}:", depth + 1))
                    yield return nested;
            }
        }
    }
}
