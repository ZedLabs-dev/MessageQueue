using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Networks;
using Testcontainers.MsSql;
using Testcontainers.ServiceBus;

namespace ZedLabs.MessageQueue.Tests;

// The Service Bus for every test in the Azure Service Bus collection (ServiceBusTestTarget decides which): the
// namespace in MQ_ASB_CONNECTION_STRING, or the emulator and its SQL Server started with Testcontainers once the owner
// sets MQ_ASB_ACCEPT_EULA=Y. The emulator declares its queue and topic from docker/servicebus-emulator-config.json.
// xunit creates collection fixtures only when a test in the collection runs, so `--filter Category=Unit` never
// touches Docker. No Docker: skipped, unless MQ_REQUIRE_DOCKER is "true" or "1", where it is a failure.
public sealed class ServiceBusEmulatorFixture : IAsyncLifetime
{
    const string RequireDockerVariable = "MQ_REQUIRE_DOCKER";
    const string SqlAlias = "servicebus-sql";

    INetwork? network;
    MsSqlContainer? sql;
    ServiceBusContainer? emulator;
    string? connectionString;
    string? unavailableReason;

    static bool DockerRequired =>
        Environment.GetEnvironmentVariable(RequireDockerVariable)?.Trim() is { } value
        && (value == "1" || value.Equals("true", StringComparison.OrdinalIgnoreCase));

    public async ValueTask InitializeAsync()
    {
        var target = ServiceBusTestTarget.Resolve(Environment.GetEnvironmentVariable);
        if (!target.StartEmulator)
        {
            connectionString = target.ConnectionString;
            unavailableReason = target.SkipReason;
            return;
        }

        try
        {
            network = new NetworkBuilder().Build();
            // A fresh password per run: the SQL Server is reachable only on the private network, by the emulator.
            var password = $"Sb1!{Guid.NewGuid():N}";
            sql = new MsSqlBuilder(TestBrokerImage.ReadServiceBus("sql"))
                .WithPassword(password)
                .WithNetwork(network)
                .WithNetworkAliases(SqlAlias)
                .Build();
            emulator = new ServiceBusBuilder(TestBrokerImage.ReadServiceBus("emulator"))
                .WithAcceptLicenseAgreement(true)
                .WithMsSqlContainer(network, sql, SqlAlias, password)
                .WithConfig(Path.Combine(AppContext.BaseDirectory, "servicebus-emulator-config.json"))
                .Build();
            await emulator.StartAsync();
        }
        catch (DockerUnavailableException ex) when (!DockerRequired)
        {
            unavailableReason = $"Docker is not available, so the Service Bus emulator cannot start ({ex.Message}). " +
                $"Start Docker, or set {RequireDockerVariable}=true to make this a failure.";
            return;
        }

        connectionString = emulator.GetConnectionString();
        TestContext.Current.SendDiagnosticMessage(
            $"Service Bus emulator {emulator.Id} listening at {emulator.Hostname}:{emulator.GetMappedPublicPort(5672)}");
    }

    public async ValueTask DisposeAsync()
    {
        if (emulator is not null)
            await emulator.DisposeAsync();
        if (sql is not null)
            await sql.DisposeAsync();
        if (network is not null)
            await network.DisposeAsync();
    }

    public bool IsEmulator => emulator is not null;

    public string RequireConnectionString()
    {
        Assert.SkipWhen(connectionString is null, unavailableReason ?? "No Azure Service Bus to test against.");
        return connectionString!;
    }
}

[CollectionDefinition(Name)]
public sealed class AzureServiceBusCollection : ICollectionFixture<ServiceBusEmulatorFixture>
{
    public const string Name = "Azure Service Bus";
}
