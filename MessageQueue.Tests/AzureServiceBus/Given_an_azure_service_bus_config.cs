using AwesomeAssertions;
using Azure.Core;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.AzureServiceBus;
using ESE.MessageQueue.Exceptions;
using ESE.MessageQueue.Extensions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace ESE.MessageQueue.Tests.AzureServiceBus;

// S3: identity (FullyQualifiedNamespace + TokenCredential) or a SAS connection string, exactly one of them.
// S10: no SharedAccessKey in any exception, whatever goes wrong with the connection settings.
[Trait("Category", "Unit")]
public class Given_an_azure_service_bus_config
{
    const string Namespace = "contoso.servicebus.windows.net";
    readonly FakeServiceBus bus = new();

    static AzureServiceBusConfig WithNamespace() => AzureServiceBusTestConfigs.Queue() with { ConnectionString = "", FullyQualifiedNamespace = Namespace };

    sealed class StaticCredential : TokenCredential
    {
        public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken) =>
            new("token", DateTimeOffset.UtcNow.AddHours(1));

        public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken) =>
            ValueTask.FromResult(GetToken(requestContext, cancellationToken));
    }

    [Fact]
    public void When_Config_Has_Default_Values()
    {
        var config = new AzureServiceBusConfig();

        config.SendTimeout.Should().Be(TimeSpan.FromSeconds(30), "it should bound sends by 30 seconds by default");
        config.ShutdownTimeout.Should().Be(TimeSpan.FromSeconds(30), "it should wait 30 seconds for handlers on shutdown by default");
        config.TransactionalRetries.Should().BeTrue("it should send a retry copy and complete the original atomically by default");
        config.TopicName.Should().BeNull("it should have no topic name by default");
        config.SubscriptionName.Should().BeNull("it should have no subscription name by default");
    }

    [Fact]
    public void When_Neither_ConnectionString_Nor_Namespace_Is_Set()
    {
        var act = () => bus.CreateQueue(AzureServiceBusTestConfigs.Queue() with { ConnectionString = "" });

        act.Should().Throw<OptionsValidationException>("it should need a way to reach the namespace")
            .WithMessage("*ConnectionString*FullyQualifiedNamespace*");
    }

    [Fact]
    public void When_Both_ConnectionString_And_Namespace_Are_Set()
    {
        var act = () => bus.CreateQueue(AzureServiceBusTestConfigs.Queue() with { FullyQualifiedNamespace = Namespace });

        act.Should().Throw<OptionsValidationException>("it should not guess which of two credentials to use")
            .WithMessage("*exactly one*");
    }

    [Theory]
    [InlineData("sb://contoso.servicebus.windows.net/")]
    [InlineData("contoso servicebus")]
    public void When_The_Namespace_Is_Not_A_Host_Name(string fullyQualifiedNamespace)
    {
        var act = () => bus.CreateQueue(WithNamespace() with { FullyQualifiedNamespace = fullyQualifiedNamespace });

        act.Should().Throw<OptionsValidationException>("it should expect a host name such as contoso.servicebus.windows.net")
            .WithMessage("*FullyQualifiedNamespace*");
    }

    [Fact]
    public void When_ShutdownTimeout_Is_Zero()
    {
        var act = () => bus.CreateQueue(AzureServiceBusTestConfigs.Queue() with { ShutdownTimeout = TimeSpan.Zero });

        act.Should().Throw<OptionsValidationException>("it should need time to drain handlers").WithMessage("*ShutdownTimeout*");
    }

    [Fact]
    public async Task When_Using_A_Namespace_Without_A_Credential()
    {
        await using var queue = bus.CreateQueue(WithNamespace());

        bus.Created.Should().ContainSingle("it should build the client once").Which.Credential.Should().BeNull(
            "it should leave the default (DefaultAzureCredential) to the client factory");
    }

    [Fact]
    public async Task When_A_Credential_Is_Supplied()
    {
        var credential = new StaticCredential();

        await using var queue = bus.CreateQueue(WithNamespace(), credential: credential);

        bus.Created.Single().Credential.Should().BeSameAs(credential, "it should use the credential it was given");
    }

    [Fact]
    public async Task When_A_Credential_Is_Registered_In_The_Container()
    {
        var credential = new StaticCredential();
        var services = new ServiceCollection();
        services.AddSingleton<TokenCredential>(credential);
        services.AddSingleton<IServiceBusTransportFactory>(bus);
        services.AddMessageQueue<TestMessage>(new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MessageQueue:Provider"] = "AzureServiceBus",
            ["MessageQueue:FullyQualifiedNamespace"] = Namespace,
            ["MessageQueue:QueueName"] = "orders"
        }).Build());
        await using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<IMessageQueue<TestMessage>>();

        bus.Created.Single().Credential.Should().BeSameAs(credential, "it should use the TokenCredential registered in the container");
    }

    [Fact]
    public async Task When_The_Client_Is_Built_From_A_Namespace()
    {
        await using var client = ServiceBusTransportFactory.CreateClient(WithNamespace(), new StaticCredential());

        client.FullyQualifiedNamespace.Should().Be(Namespace, "it should connect to the configured namespace with the credential");
    }

    [Fact]
    public async Task When_The_Client_Is_Built_From_A_Namespace_With_The_Default_Credential()
    {
        await using var client = ServiceBusTransportFactory.CreateClient(WithNamespace(), credential: null);

        client.FullyQualifiedNamespace.Should().Be(Namespace, "it should fall back to DefaultAzureCredential without connecting");
    }

    [Fact]
    public async Task When_The_Client_Is_Built_From_A_Connection_String()
    {
        await using var client = ServiceBusTransportFactory.CreateClient(AzureServiceBusTestConfigs.Queue(), credential: null);

        client.FullyQualifiedNamespace.Should().Be("test.servicebus.windows.net", "it should read the namespace from the connection string");
    }

    [Theory]
    [InlineData("Endpoint=not a uri;SharedAccessKeyName=k;SharedAccessKey=" + AzureServiceBusTestConfigs.Secret)]
    [InlineData("SharedAccessKeyName=k;SharedAccessKey=" + AzureServiceBusTestConfigs.Secret)]
    [InlineData("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKey=" + AzureServiceBusTestConfigs.Secret)]
    [InlineData("garbage" + AzureServiceBusTestConfigs.Secret + ";;==")]
    public void When_A_Connection_String_With_A_Secret_Is_Malformed(string connectionString)
    {
        var act = () => bus.CreateQueue(AzureServiceBusTestConfigs.Queue() with { ConnectionString = connectionString });

        act.Should().Throw<OptionsValidationException>("it should reject the connection string at validation")
            .Which.ToString().Should().NotContain(AzureServiceBusTestConfigs.Secret, "it should never leak the SharedAccessKey");
    }

    [Theory]
    [InlineData("Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;")]
    [InlineData("Endpoint=sb://contoso.servicebus.windows.net/;SharedAccessSignature=SharedAccessSignature sr=x&sig=y&se=1&skn=k")]
    public async Task When_A_Connection_String_Is_Valid(string connectionString)
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue() with { ConnectionString = connectionString });

        bus.Created.Should().ContainSingle("it should accept the emulator's and shared access signature connection strings");
    }

    [Fact]
    public void When_The_Client_Cannot_Be_Created()
    {
        var logger = new RecordingLogger();
        bus.CreateFailure = new ArgumentException($"bad connection string {AzureServiceBusTestConfigs.ConnectionString}");

        var act = () => bus.CreateQueue(AzureServiceBusTestConfigs.Queue(), logger);

        act.Should().Throw<MessageQueueException>("it should report the failure")
            .Which.ToString().Should().NotContain(AzureServiceBusTestConfigs.Secret, "it should never leak the SharedAccessKey, even if the SDK echoes it");
        logger.Entries.Select(e => e.Message + e.Exception).Should().NotContain(m => m.Contains(AzureServiceBusTestConfigs.Secret),
            "it should not log the SharedAccessKey either");
    }

    [Fact]
    public void When_Printing_The_Config()
    {
        AzureServiceBusTestConfigs.Queue().ToString().Should().NotContain(AzureServiceBusTestConfigs.Secret, "it should redact the connection string");
    }

    [Fact]
    public void When_A_Credential_Comes_With_A_Connection_String()
    {
        var act = () => bus.CreateQueue(AzureServiceBusTestConfigs.Queue(), credential: new StaticCredential());

        act.Should().Throw<ConfigurationException>("it should not silently ignore a credential the caller passed")
            .WithMessage("*FullyQualifiedNamespace*");
        bus.Created.Should().BeEmpty("it should fail before building a client");
    }

    [Fact]
    public async Task When_A_Container_Credential_Exists_But_The_Queue_Uses_A_Connection_String()
    {
        var services = new ServiceCollection();
        services.AddSingleton<TokenCredential>(new StaticCredential());
        services.AddSingleton<IServiceBusTransportFactory>(bus);
        services.AddMessageQueue<TestMessage>(new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MessageQueue:Provider"] = "AzureServiceBus",
            ["MessageQueue:ConnectionString"] = AzureServiceBusTestConfigs.ConnectionString,
            ["MessageQueue:QueueName"] = "orders"
        }).Build());
        await using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<IMessageQueue<TestMessage>>();

        bus.Created.Single().Credential.Should().BeNull("it should leave a container credential to other components when a connection string is configured");
    }

    [Fact]
    public void When_Resolving_A_Supplied_Credential()
    {
        var credential = new StaticCredential();

        ServiceBusTransportFactory.ResolveCredential(credential).Should().BeSameAs(credential, "it should use the credential it was given");
    }

    [Fact]
    public void When_Resolving_The_Default_Credential()
    {
        ServiceBusTransportFactory.ResolveCredential(null).Should().BeSameAs(ServiceBusTransportFactory.ResolveCredential(null),
            "it should share one DefaultAzureCredential (and its token cache) across queues");
    }
}
