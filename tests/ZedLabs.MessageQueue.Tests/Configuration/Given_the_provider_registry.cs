using AwesomeAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using ZedLabs.MessageQueue.Abstractions;
using ZedLabs.MessageQueue.Exceptions;
using ZedLabs.MessageQueue.Extensions;
using ZedLabs.MessageQueue.Providers;
using ZedLabs.MessageQueue.RabbitMQ;

namespace ZedLabs.MessageQueue.Tests.Configuration;

// ADR 0005 / 0015: the Provider setting picks among the providers registered before AddMessageQueue<T>.
[Trait("Category", "Unit")]
public class Given_the_provider_registry
{
    static IConfiguration Configuration(string provider) =>
        new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MessageQueue:Provider"] = provider,
            ["MessageQueue:ConnectionString"] = "amqp://localhost",
            ["MessageQueue:QueueName"] = "orders"
        }).Build();

    [Fact]
    public void When_The_Provider_Is_Not_Registered()
    {
        var act = () => new ServiceCollection().AddMessageQueue<TestMessage>(Configuration("RabbitMq"));

        act.Should().Throw<ConfigurationException>("it should fail when the named provider's package was not registered")
            .WithMessage("*'RabbitMq' is not registered*Registered providers: InMemory.*AddRabbitMqProvider*");
    }

    [Fact]
    public void When_Listing_The_Registered_Providers()
    {
        var services = new ServiceCollection().AddRabbitMqProvider().AddAzureServiceBusProvider();

        var act = () => services.AddMessageQueue<TestMessage>(Configuration("Kafka"));

        act.Should().Throw<ConfigurationException>("it should name every provider that could have been selected")
            .WithMessage("*'Kafka'*Registered providers: InMemory, RabbitMq, AzureServiceBus*");
    }

    [Fact]
    public void When_The_Provider_Is_Missing()
    {
        var act = () => new ServiceCollection().AddRabbitMqProvider().AddMessageQueue<TestMessage>(Configuration(" "));

        act.Should().Throw<ConfigurationException>("it should never default to a broker")
            .WithMessage("MessageQueue:Provider is required. Registered providers: InMemory, RabbitMq");
    }

    [Fact]
    public void When_A_Registered_Provider_Is_Named()
    {
        var services = new ServiceCollection().AddRabbitMqProvider();

        services.AddMessageQueue<TestMessage>(Configuration("RABBITMQ"));

        services.Should().Contain(d => d.ServiceType == typeof(RabbitMqMessageQueue<TestMessage>),
            "it should let the registered provider register its queue, matching the name case-insensitively");
        services.Should().Contain(d => d.ServiceType == typeof(IMessageQueue<TestMessage>), "it should expose the queue as IMessageQueue<T>");
    }

    [Fact]
    public void When_A_Provider_Is_Registered_Twice()
    {
        var services = new ServiceCollection().AddRabbitMqProvider().AddRabbitMqProvider();

        services.Count(d => d.ServiceType == typeof(IMessageQueueProvider)).Should().Be(1, "it should treat a repeated registration as a no-op");
    }

    [Fact]
    public void When_Two_Providers_Claim_One_Name()
    {
        var services = new ServiceCollection().AddRabbitMqProvider();

        var act = () => services.AddMessageQueueProvider(new NamedProvider("rabbitmq"));

        act.Should().Throw<ConfigurationException>("it should not let a second provider take over a registered name")
            .WithMessage("*'RabbitMq' is already registered*");
    }

    [Fact]
    public void When_A_Provider_Claims_The_Built_In_Name()
    {
        var act = () => new ServiceCollection().AddMessageQueueProvider(new NamedProvider("inmemory"));

        act.Should().Throw<ConfigurationException>("it should not let a provider shadow the built-in in-memory provider")
            .WithMessage("*'InMemory' is already registered*");
    }

    [Fact]
    public void When_A_Provider_Registers_No_Queue()
    {
        var services = new ServiceCollection().AddMessageQueueProvider(new NamedProvider("Lazy"));

        var act = () => services.AddMessageQueue<TestMessage>(Configuration("Lazy"));

        act.Should().Throw<ConfigurationException>("it should not leave the message type silently without a queue")
            .WithMessage("Provider 'Lazy' registered no queue for *TestMessage");
    }

    [Fact]
    public void When_A_Provider_Name_Is_Blank()
    {
        var act = () => new ServiceCollection().AddMessageQueueProvider(new NamedProvider(" "));

        act.Should().Throw<ArgumentException>("it should refuse a provider that cannot be selected by name");
    }

    [Fact]
    public void When_A_Provider_Is_Registered_As_A_Type()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMessageQueueProvider, LoopbackLikeProvider>();

        var act = () => services.AddMessageQueue<TestMessage>(Configuration("Loopback"));

        act.Should().Throw<ConfigurationException>("it should not silently ignore a provider it cannot see at registration")
            .WithMessage("*AddMessageQueueProvider*");
    }

    sealed class LoopbackLikeProvider : IMessageQueueProvider
    {
        public string Name => "Loopback";

        public void AddQueue<T>(MessageQueueRegistration<T> registration) where T : class { }
    }

    sealed class NamedProvider(string name) : IMessageQueueProvider
    {
        public string Name => name;

        public void AddQueue<T>(MessageQueueRegistration<T> registration) where T : class { }
    }
}
