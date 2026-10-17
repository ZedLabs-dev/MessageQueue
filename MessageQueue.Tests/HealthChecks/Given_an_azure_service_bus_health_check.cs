using AwesomeAssertions;
using Azure.Messaging.ServiceBus;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.AzureServiceBus;
using ESE.MessageQueue.Extensions;
using ESE.MessageQueue.HealthChecks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ESE.MessageQueue.Tests.HealthChecks;

// S4: the default check needs only Listen rights (a peek on the queue's own client), never Manage, and never creates
// a client per probe. S5: no exception text, host or namespace in the result.
[Trait("Category", "Unit")]
public class Given_an_azure_service_bus_health_check : IAsyncDisposable
{
    readonly FakeServiceBus bus = new();

    // The fake stands in for the SDK client; disposing it with the test class keeps CA1001 honest.
    public ValueTask DisposeAsync() => bus.DisposeAsync();

    static Task<HealthCheckResult> CheckAsync(AzureServiceBusHealthCheck check) =>
        check.CheckHealthAsync(new HealthCheckContext { Registration = new HealthCheckRegistration("asb", check, null, null) },
            TestContext.Current.CancellationToken);

    [Fact]
    public async Task When_The_Queue_Can_Be_Peeked()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        var check = new AzureServiceBusHealthCheck(queue);

        var results = new[] { await CheckAsync(check), await CheckAsync(check), await CheckAsync(check) };

        results.Should().AllSatisfy(r => r.Status.Should().Be(HealthStatus.Healthy, "it should report a reachable queue as healthy"));
        bus.Peekers.Should().ContainSingle("it should reuse one receiver on the queue's own client for every probe")
            .Which.Peeks.Should().Be(3, "it should peek once per probe");
        bus.Peekers.Single().EntityPath.Should().Be("orders", "it should peek the queue it consumes from");
        results.Should().AllSatisfy(r => r.Data.Should().BeEmpty("it should put no topology in the result"));
    }

    [Fact]
    public async Task When_Consuming_From_A_Subscription()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Subscription());

        await CheckAsync(new AzureServiceBusHealthCheck(queue));

        bus.Peekers.Single().SubscriptionName.Should().Be("billing", "it should peek the subscription it consumes from");
    }

    [Fact]
    public async Task When_Peeking_Fails()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        var check = new AzureServiceBusHealthCheck(queue);
        await CheckAsync(check);
        bus.Peekers.Single().Failure = new ServiceBusException("sb://secret-host.servicebus.windows.net: Unauthorized", ServiceBusFailureReason.ServiceCommunicationProblem);

        var result = await CheckAsync(check);

        result.Status.Should().Be(HealthStatus.Unhealthy, "it should report a failed peek");
        result.Description.Should().NotContain("secret-host", "it should not echo the exception text");
        result.Data.Should().BeEmpty("it should put no exception text in the data");
        result.Exception.Should().BeOfType<ServiceBusException>("it should attach the exception for the host's logging");
    }

    [Fact]
    public async Task When_The_Processor_Has_Stopped()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue());
        await queue.SubscribeAsync(AzureServiceBusTestConfigs.Returns(MessageResult.Acknowledge), TestContext.Current.CancellationToken);
        bus.Processor.IsProcessing = false;

        var result = await CheckAsync(new AzureServiceBusHealthCheck(queue));

        result.Status.Should().Be(HealthStatus.Unhealthy, "it should report a subscription whose processor is not running");
    }

    [Fact]
    public async Task When_Registered_For_A_Queue_In_The_Container()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IServiceBusTransportFactory>(bus);
        services.AddMessageQueue<TestMessage>(new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MessageQueue:Provider"] = "AzureServiceBus",
            ["MessageQueue:ConnectionString"] = AzureServiceBusTestConfigs.ConnectionString,
            ["MessageQueue:QueueName"] = "orders"
        }).Build());
        services.AddHealthChecks().AddAzureServiceBusHealthCheck<TestMessage>();
        await using var provider = services.BuildServiceProvider();
        var health = provider.GetRequiredService<HealthCheckService>();

        var first = await health.CheckHealthAsync(TestContext.Current.CancellationToken);
        var second = await health.CheckHealthAsync(TestContext.Current.CancellationToken);

        first.Status.Should().Be(HealthStatus.Healthy, "it should probe through the registered queue");
        second.Status.Should().Be(HealthStatus.Healthy, "it should keep probing through the registered queue");
        bus.Created.Should().ContainSingle("it should reuse the queue's client instead of creating one per probe");
        bus.Peekers.Should().ContainSingle("it should reuse one receiver for every probe");
    }

    [Fact]
    public void When_Registered_With_A_Connection_String()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddHealthChecks().AddAzureServiceBusHealthCheck(AzureServiceBusTestConfigs.ConnectionString, "orders");
        using var provider = services.BuildServiceProvider();
        var registration = provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<HealthCheckServiceOptions>>().Value.Registrations.Single();

        var first = registration.Factory(provider);
        var second = registration.Factory(provider);

        first.Should().BeSameAs(second, "it should keep one client for every probe instead of creating one each time");
        first.Should().BeOfType<AzureServiceBusHealthCheck>("it should use the least-privilege peek check by default");
    }

    [Fact]
    public void When_Admin_Metrics_Are_Wanted()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddHealthChecks().AddAzureServiceBusMetricsHealthCheck(AzureServiceBusTestConfigs.ConnectionString, "orders");
        using var provider = services.BuildServiceProvider();
        var registration = provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<HealthCheckServiceOptions>>().Value.Registrations.Single();

        registration.Factory(provider).Should().BeSameAs(registration.Factory(provider), "it should reuse its administration client")
            .And.BeOfType<AzureServiceBusMetricsHealthCheck>("it should offer the Manage-rights metrics check only when asked for");
    }

    sealed class StaticCredential : Azure.Core.TokenCredential
    {
        public override Azure.Core.AccessToken GetToken(Azure.Core.TokenRequestContext requestContext, CancellationToken cancellationToken) =>
            new("token", DateTimeOffset.UtcNow.AddHours(1));

        public override ValueTask<Azure.Core.AccessToken> GetTokenAsync(Azure.Core.TokenRequestContext requestContext, CancellationToken cancellationToken) =>
            ValueTask.FromResult(GetToken(requestContext, cancellationToken));
    }

    // B12: the standalone check peeks the subscription when one is given.
    [Fact]
    public async Task When_Checking_A_Subscription_By_Connection_String()
    {
        await using var check = new AzureServiceBusHealthCheck(AzureServiceBusTestConfigs.ConnectionString, "events", "billing");

        check.Receiver.EntityPath.Should().Be("events/Subscriptions/billing", "it should peek the subscription, not the topic");
    }

    [Fact]
    public async Task When_Checking_With_Identity()
    {
        await using var check = new AzureServiceBusHealthCheck("contoso.servicebus.windows.net", new StaticCredential(), "orders");

        check.Receiver.FullyQualifiedNamespace.Should().Be("contoso.servicebus.windows.net", "it should reach the namespace with the credential");
        check.Receiver.EntityPath.Should().Be("orders", "it should peek the queue");
    }

    [Fact]
    public void When_Registered_With_Identity()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddHealthChecks().AddAzureServiceBusHealthCheck("contoso.servicebus.windows.net", new StaticCredential(), "orders");
        using var provider = services.BuildServiceProvider();
        var registration = provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<HealthCheckServiceOptions>>().Value.Registrations.Single();

        registration.Factory(provider).Should().BeSameAs(registration.Factory(provider), "it should keep one client for every probe")
            .And.BeOfType<AzureServiceBusHealthCheck>("it should register the peek check");
    }

    [Fact]
    public void When_Admin_Metrics_Are_Wanted_With_Identity()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddHealthChecks().AddAzureServiceBusMetricsHealthCheck("contoso.servicebus.windows.net", new StaticCredential(), "orders");
        using var provider = services.BuildServiceProvider();
        var registration = provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<HealthCheckServiceOptions>>().Value.Registrations.Single();

        registration.Factory(provider).Should().BeSameAs(registration.Factory(provider), "it should reuse its administration client")
            .And.BeOfType<AzureServiceBusMetricsHealthCheck>("it should offer the metrics check with identity too");
    }
}
