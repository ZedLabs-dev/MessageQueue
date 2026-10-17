using Azure.Messaging.ServiceBus;

namespace ESE.MessageQueue.AzureServiceBus;

// How a queue reaches its namespace (S3): Entra ID identity (FullyQualifiedNamespace + TokenCredential, the
// production default) or a SAS connection string. Exactly one. Errors never echo the connection string (S10).
static class AzureServiceBusConnections
{
    public static void Validate(AzureServiceBusConfig config, List<string> errors)
    {
        var hasConnectionString = !string.IsNullOrWhiteSpace(config.ConnectionString);
        var hasNamespace = !string.IsNullOrWhiteSpace(config.FullyQualifiedNamespace);

        if (hasConnectionString == hasNamespace)
        {
            errors.Add("Set exactly one of ConnectionString (shared access key) or FullyQualifiedNamespace (Entra ID identity, recommended)");
            return;
        }

        if (hasConnectionString && !IsValidConnectionString(config.ConnectionString))
            errors.Add("ConnectionString is not a valid Service Bus connection string: it needs Endpoint=sb://<namespace>/ and " +
                "SharedAccessKeyName with SharedAccessKey, or SharedAccessSignature");

        if (hasNamespace && Uri.CheckHostName(config.FullyQualifiedNamespace.Trim()) != UriHostNameType.Dns)
            errors.Add("FullyQualifiedNamespace must be a host name such as contoso.servicebus.windows.net (no scheme or path)");
    }

    // The same rules ServiceBusClient applies, checked up front so a bad value fails at startup with a fixed message.
    static bool IsValidConnectionString(string connectionString)
    {
        try
        {
            var properties = ServiceBusConnectionStringProperties.Parse(connectionString);
            var hasKey = !string.IsNullOrEmpty(properties.SharedAccessKeyName) && !string.IsNullOrEmpty(properties.SharedAccessKey);
            return properties.Endpoint is { IsAbsoluteUri: true } && (hasKey || !string.IsNullOrEmpty(properties.SharedAccessSignature));
        }
        catch (Exception)
        {
            return false;
        }
    }
}
