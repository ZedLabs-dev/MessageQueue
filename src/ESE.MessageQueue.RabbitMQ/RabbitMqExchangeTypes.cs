namespace ESE.MessageQueue.RabbitMQ;

// ExchangeType goes to exchange.declare unchanged. RabbitMQ matches type names exactly (lowercase): the four built-in
// types, or a plugin's type, which is named "x-..." by convention. Anything else fails only on first connect.
static class RabbitMqExchangeTypes
{
    const string PluginPrefix = "x-";
    static readonly string[] BuiltIn = ["direct", "fanout", "topic", "headers"];

    public static void Validate(string? exchangeType, ICollection<string> errors)
    {
        // An empty value is reported by [Required].
        if (string.IsNullOrEmpty(exchangeType) || IsKnown(exchangeType))
            return;

        errors.Add($"ExchangeType '{exchangeType}' is not supported: use direct, fanout, topic or headers, or a plugin type starting with '{PluginPrefix}' (exchange types are lowercase and case-sensitive)");
    }

    static bool IsKnown(string exchangeType) =>
        BuiltIn.Contains(exchangeType, StringComparer.Ordinal)
        || (exchangeType.Length > PluginPrefix.Length && exchangeType.StartsWith(PluginPrefix, StringComparison.Ordinal));
}
