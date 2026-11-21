namespace ZedLabs.MessageQueue.Tests;

// Which Service Bus the integration tests use: a namespace from MQ_ASB_CONNECTION_STRING, else the emulator when the
// owner accepted its licence through MQ_ASB_ACCEPT_EULA, else none (the tests skip).
sealed record ServiceBusTestTarget(string? ConnectionString, bool StartEmulator, string? SkipReason)
{
    public const string AcceptEulaVariable = "MQ_ASB_ACCEPT_EULA";

    public static ServiceBusTestTarget Resolve(Func<string, string?> environment)
    {
        var connectionString = environment(AzureServiceBusEnvironment.ConnectionStringVariable);
        if (!string.IsNullOrWhiteSpace(connectionString))
        {
            return new ServiceBusTestTarget(connectionString, StartEmulator: false, SkipReason: null);
        }

        if (environment(AcceptEulaVariable)?.Trim() is { } accept
            && (accept == "1" || accept.Equals("Y", StringComparison.OrdinalIgnoreCase)
                || accept.Equals("true", StringComparison.OrdinalIgnoreCase)))
        {
            return new ServiceBusTestTarget(ConnectionString: null, StartEmulator: true, SkipReason: null);
        }

        return new ServiceBusTestTarget(null, false,
            $"No Azure Service Bus to test against. Set {AzureServiceBusEnvironment.ConnectionStringVariable} to a namespace, " +
            $"or set {AcceptEulaVariable}=Y to accept the licences of the Service Bus emulator and SQL Server and start " +
            "them with Testcontainers (needs Docker).");
    }
}
