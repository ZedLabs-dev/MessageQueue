using AwesomeAssertions;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Exceptions;
using ESE.MessageQueue.Extensions;
using ESE.MessageQueue.InMemory;
using ESE.MessageQueue.RabbitMQ;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace ESE.MessageQueue.Tests.Configuration;

// Startup validation is exercised through IStartupValidator, which is what the host runs for ValidateOnStart.
// Broker-backed queues are never resolved here: that would try to connect.
[Trait("Category", "Unit")]
public class Given_a_message_queue_configuration
{
    public sealed record Payment(string Id);

    static readonly Dictionary<string, string?> ValidRabbitMq = new()
    {
        ["MessageQueue:Provider"] = "RabbitMq",
        ["MessageQueue:ConnectionString"] = "amqp://localhost",
        ["MessageQueue:QueueName"] = "test-queue"
    };

    static readonly Dictionary<string, string?> ValidAzureServiceBus = new()
    {
        ["MessageQueue:Provider"] = "AzureServiceBus",
        ["MessageQueue:ConnectionString"] = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=" + AzureServiceBusTestConfigs.Secret,
        ["MessageQueue:QueueName"] = "test-queue"
    };

    static Dictionary<string, string?> With(Dictionary<string, string?> baseline, params (string Key, string? Value)[] overrides)
    {
        var values = new Dictionary<string, string?>(baseline);
        foreach (var (key, value) in overrides)
        {
            values[$"MessageQueue:{key}"] = value;
        }
        return values;
    }

    static ServiceProvider Register(Dictionary<string, string?> values)
    {
        var services = new ServiceCollection();
        services.AddRabbitMqProvider().AddAzureServiceBusProvider();
        services.AddMessageQueue<TestMessage>(Configuration(values));
        return services.BuildServiceProvider();
    }

    static IConfiguration Configuration(Dictionary<string, string?> values) =>
        new ConfigurationBuilder().AddInMemoryCollection(values).Build();

    static Action StartupValidation(ServiceProvider provider) =>
        () => provider.GetRequiredService<IStartupValidator>().Validate();

    static void ShouldFailStartup(Dictionary<string, string?> values, string expectedMessage, string because)
    {
        using var provider = Register(values);

        StartupValidation(provider).Should().Throw<OptionsValidationException>(because).WithMessage(expectedMessage);
    }

    [Fact]
    public void When_Provider_Is_Missing()
    {
        var values = new Dictionary<string, string?>(ValidRabbitMq);
        values.Remove("MessageQueue:Provider");

        var act = () => new ServiceCollection().AddRabbitMqProvider().AddAzureServiceBusProvider().AddMessageQueue<TestMessage>(Configuration(values));

        act.Should().Throw<ConfigurationException>("it should never silently default to a broker")
            .WithMessage("*Provider*");
    }

    [Fact]
    public void When_Provider_Is_Unknown()
    {
        var act = () => new ServiceCollection().AddRabbitMqProvider().AddAzureServiceBusProvider().AddMessageQueue<TestMessage>(Configuration(With(ValidRabbitMq, ("Provider", "Kafka"))));

        act.Should().Throw<ConfigurationException>("it should reject a provider it does not know").WithMessage("*Kafka*");
    }

    [Fact]
    public void When_Provider_Is_A_Number()
    {
        var act = () => new ServiceCollection().AddRabbitMqProvider().AddAzureServiceBusProvider().AddMessageQueue<TestMessage>(Configuration(With(ValidRabbitMq, ("Provider", "7"))));

        act.Should().Throw<ConfigurationException>("it should not accept an enum number that names no provider");
    }

    [Theory]
    [InlineData("1")]
    [InlineData("+1")]
    [InlineData(" +2")]
    [InlineData("RabbitMq,AzureServiceBus")]
    public void When_Provider_Is_Not_A_Provider_Name(string provider)
    {
        var act = () => new ServiceCollection().AddRabbitMqProvider().AddAzureServiceBusProvider().AddMessageQueue<TestMessage>(Configuration(With(ValidRabbitMq, ("Provider", provider))));

        act.Should().Throw<ConfigurationException>("it should accept only a provider's name, not numbers or combinations");
    }

    [Fact]
    public void When_Provider_Name_Has_Other_Casing_And_Spaces()
    {
        using var provider = Register(new Dictionary<string, string?>
        {
            ["MessageQueue:Provider"] = " inmemory ",
            ["MessageQueue:QueueName"] = "local"
        });

        provider.GetRequiredService<IMessageQueue<TestMessage>>().Should().BeOfType<InMemoryMessageQueue<TestMessage>>(
            "it should match provider names case-insensitively, ignoring surrounding spaces");
    }

    [Theory]
    [InlineData("1")]
    [InlineData("+1")]
    [InlineData("Json,MessagePack")]
    public void When_SerializationFormat_Is_Not_A_Format_Name(string format) =>
        ShouldFailStartup(With(ValidRabbitMq, ("SerializationFormat", format)), "*SerializationFormat*",
            "it should accept only a format's name, not numbers or combinations");

    [Theory]
    [InlineData("2")]
    [InlineData("+0")]
    [InlineData("Fixed,Linear")]
    public void When_Retry_Strategy_Is_Not_A_Strategy_Name(string strategy) =>
        ShouldFailStartup(With(ValidRabbitMq, ("Retry:Strategy", strategy)), "*Strategy*",
            "it should accept only a strategy's name, not numbers or combinations");

    [Theory]
    [InlineData("Tls:ClientCertificateStoreName", "My,Root")]
    [InlineData("Tls:ClientCertificateStoreName", "5")]
    [InlineData("Tls:ClientCertificateStoreLocation", "2")]
    [InlineData("Tls:AcceptablePolicyErrors", "4")]
    public void When_A_Provider_Enum_Setting_Is_Not_A_Name(string key, string value) =>
        ShouldFailStartup(With(ValidRabbitMq, (key, value)), $"*{key} '{value}' is not supported*",
            "it should accept only names for a provider's enum settings too, not numbers or combinations");

    [Fact]
    public void When_A_Flags_Setting_Combines_Names()
    {
        using var provider = Register(With(ValidRabbitMq,
            ("Tls:AcceptablePolicyErrors", "RemoteCertificateNameMismatch, RemoteCertificateChainErrors"),
            ("Tls:AllowInsecureCertificates", "true"),
            ("Tls:ClientCertificateStoreName", "my"),
            ("Tls:ClientCertificateStoreLocation", " LocalMachine ")));

        StartupValidation(provider).Should().NotThrow("it should accept names, in any case, and combinations of names for a flags setting");
    }

    [Fact]
    public void When_Using_The_Legacy_RetryAttempts_Setting()
    {
        using var provider = Register(With(ValidRabbitMq, ("RetryAttempts", "3")));

        StartupValidation(provider).Should().Throw<InvalidOperationException>(
            "it should reject a setting that no longer exists instead of silently ignoring it").WithMessage("*RetryAttempts*");
    }

    [Fact]
    public void When_Using_A_Removed_AzureServiceBus_Setting()
    {
        using var provider = Register(With(ValidAzureServiceBus, ("EnablePartitioning", "true")));

        StartupValidation(provider).Should().Throw<InvalidOperationException>(
            "it should reject a setting that no longer exists").WithMessage("*EnablePartitioning*");
    }

    [Fact]
    public void When_Registering_The_Same_Message_Type_Twice()
    {
        var services = new ServiceCollection();
        services.AddRabbitMqProvider().AddAzureServiceBusProvider();
        services.AddMessageQueue<TestMessage>(Configuration(ValidRabbitMq));

        var act = () => services.AddInMemoryMessageQueue<TestMessage>();

        act.Should().Throw<ConfigurationException>("it should not let a second registration silently replace the first")
            .WithMessage($"*{nameof(TestMessage)}*");
    }

    [Fact]
    public void When_AzureServiceBus_Subscription_Has_No_Topic() =>
        ShouldFailStartup(With(ValidAzureServiceBus, ("SubscriptionName", "billing")), "*TopicName*",
            "it should reject a subscription without the topic it belongs to");

    [Fact]
    public void When_InMemory_QueueName_Is_Missing()
    {
        using var provider = Register(new Dictionary<string, string?> { ["MessageQueue:Provider"] = "InMemory" });

        StartupValidation(provider).Should().Throw<OptionsValidationException>("it should require a queue name, not invent one")
            .WithMessage("*QueueName*");
    }

    [Fact]
    public void When_InMemory_ShutdownTimeout_Is_Zero() =>
        ShouldFailStartup(new Dictionary<string, string?>
        {
            ["MessageQueue:Provider"] = "InMemory",
            ["MessageQueue:QueueName"] = "local",
            ["MessageQueue:ShutdownTimeout"] = "00:00:00"
        }, "*ShutdownTimeout*", "it should require a positive shutdown timeout");

    [Fact]
    public void When_Provider_Is_InMemory()
    {
        using var provider = Register(new Dictionary<string, string?>
        {
            ["MessageQueue:Provider"] = "InMemory",
            ["MessageQueue:QueueName"] = "local"
        });

        StartupValidation(provider).Should().NotThrow("it should need no connection string");
        provider.GetRequiredService<IMessageQueue<TestMessage>>().Should().BeOfType<InMemoryMessageQueue<TestMessage>>(
            "it should select the in-memory provider from configuration");
    }

    [Fact]
    public void When_RabbitMQ_Configuration_Is_Valid()
    {
        using var provider = Register(ValidRabbitMq);

        StartupValidation(provider).Should().NotThrow("it should accept valid configuration");
    }

    [Fact]
    public void When_AzureServiceBus_Configuration_Is_Valid()
    {
        using var provider = Register(ValidAzureServiceBus);

        StartupValidation(provider).Should().NotThrow("it should accept valid configuration");
    }

    [Fact]
    public void When_RabbitMQ_ConnectionString_Is_Empty() =>
        ShouldFailStartup(With(ValidRabbitMq, ("ConnectionString", "")), "*ConnectionString*",
            "it should fail fast when ConnectionString is empty");

    [Fact]
    public void When_QueueName_Is_Empty() =>
        ShouldFailStartup(With(ValidRabbitMq, ("QueueName", "")), "*QueueName*", "it should fail fast when QueueName is empty");

    [Fact]
    public void When_RabbitMQ_PrefetchCount_Is_Zero() =>
        ShouldFailStartup(With(ValidRabbitMq, ("PrefetchCount", "0")), "*PrefetchCount*", "it should require a prefetch of at least 1");

    [Fact]
    public void When_RabbitMQ_PrefetchCount_Exceeds_The_Protocol_Limit() =>
        ShouldFailStartup(With(ValidRabbitMq, ("PrefetchCount", "70000")), "*PrefetchCount*",
            "it should reject a prefetch that does not fit the AMQP field");

    [Fact]
    public void When_RabbitMQ_PublisherChannelPoolSize_Is_Zero() =>
        ShouldFailStartup(With(ValidRabbitMq, ("PublisherChannelPoolSize", "0")), "*PublisherChannelPoolSize*",
            "it should require at least one publisher channel");

    [Fact]
    public void When_RabbitMQ_MaxOutstandingConfirms_Is_Zero() =>
        ShouldFailStartup(With(ValidRabbitMq, ("MaxOutstandingConfirms", "0")), "*MaxOutstandingConfirms*",
            "it should require room for at least one outstanding confirm");

    [Fact]
    public void When_RabbitMQ_ConfirmTimeout_Is_Zero() =>
        ShouldFailStartup(With(ValidRabbitMq, ("ConfirmTimeout", "00:00:00")), "*ConfirmTimeout*",
            "it should require a positive confirm timeout");

    [Fact]
    public void When_MaxConcurrentCalls_Is_Zero() =>
        ShouldFailStartup(With(ValidRabbitMq, ("MaxConcurrentCalls", "0")), "*MaxConcurrentCalls*",
            "it should require at least one concurrent handler");

    [Fact]
    public void When_MaxMessageSizeBytes_Is_Zero() =>
        ShouldFailStartup(With(ValidRabbitMq, ("MaxMessageSizeBytes", "0")), "*MaxMessageSizeBytes*",
            "it should require a positive message size limit");

    [Fact]
    public void When_SerializationFormat_Is_Unknown() =>
        ShouldFailStartup(With(ValidRabbitMq, ("SerializationFormat", "9")), "*SerializationFormat*",
            "it should reject a serialization format that does not exist");

    [Fact]
    public void When_AzureServiceBus_ConnectionString_Is_Empty() =>
        ShouldFailStartup(With(ValidAzureServiceBus, ("ConnectionString", "")), "*ConnectionString*",
            "it should fail fast when ConnectionString is empty");

    [Fact]
    public void When_AzureServiceBus_MaxBatchSize_Is_Zero() =>
        ShouldFailStartup(With(ValidAzureServiceBus, ("MaxBatchSize", "0")), "*MaxBatchSize*",
            "it should require a batch size of at least 1");

    [Theory]
    [InlineData("SenderPoolSize")]
    [InlineData("MaxConcurrentSends")]
    public void When_Using_A_Removed_AzureServiceBus_Sender_Pool_Setting(string setting)
    {
        using var provider = Register(With(ValidAzureServiceBus, (setting, "4")));

        StartupValidation(provider).Should().Throw<InvalidOperationException>(
            "it should reject sender-pool settings that no longer exist").WithMessage($"*{setting}*");
    }

    [Fact]
    public void When_AzureServiceBus_Uses_A_Namespace()
    {
        using var provider = Register(With(ValidAzureServiceBus, ("ConnectionString", ""), ("FullyQualifiedNamespace", "contoso.servicebus.windows.net")));

        StartupValidation(provider).Should().NotThrow("it should accept identity instead of a connection string");
    }

    [Fact]
    public void When_AzureServiceBus_ShutdownTimeout_Is_Zero() =>
        ShouldFailStartup(With(ValidAzureServiceBus, ("ShutdownTimeout", "00:00:00")), "*ShutdownTimeout*",
            "it should need time to drain handlers");

    [Fact]
    public void When_AzureServiceBus_ConnectionString_Has_A_Secret_And_Is_Malformed()
    {
        using var provider = Register(With(ValidAzureServiceBus, ("ConnectionString", "Endpoint=not a uri;SharedAccessKey=" + AzureServiceBusTestConfigs.Secret)));

        StartupValidation(provider).Should().Throw<OptionsValidationException>("it should reject the connection string at startup")
            .Which.ToString().Should().NotContain(AzureServiceBusTestConfigs.Secret, "it should never leak the SharedAccessKey");
    }

    [Fact]
    public void When_AzureServiceBus_Topic_Has_No_Subscription() =>
        ShouldFailStartup(With(ValidAzureServiceBus, ("TopicName", "orders")), "*SubscriptionName*",
            "it should require a subscription to consume from a topic");

    [Fact]
    public void When_AzureServiceBus_Topic_Has_A_Subscription()
    {
        using var provider = Register(With(ValidAzureServiceBus, ("TopicName", "orders"), ("SubscriptionName", "billing")));

        StartupValidation(provider).Should().NotThrow("it should accept a topic with a subscription");
    }

    [Fact]
    public void When_Retry_MaxRetries_Is_Negative() =>
        ShouldFailStartup(With(ValidRabbitMq, ("Retry:MaxRetries", "-1")), "*MaxRetries*", "it should reject a negative retry count");

    [Fact]
    public void When_Retry_MaxRetries_Exceeds_The_Limit() =>
        ShouldFailStartup(With(ValidRabbitMq, ("Retry:MaxRetries", "5000")), "*MaxRetries*",
            "it should cap the retry count so a typo cannot keep a poison message cycling for days");

    [Fact]
    public void When_Retry_InitialDelay_Is_Negative() =>
        ShouldFailStartup(With(ValidRabbitMq, ("Retry:InitialDelay", "-00:00:01")), "*InitialDelay*",
            "it should reject a negative delay");

    [Fact]
    public void When_Retry_MaxDelay_Is_Below_InitialDelay() =>
        ShouldFailStartup(With(ValidRabbitMq, ("Retry:InitialDelay", "00:01:00"), ("Retry:MaxDelay", "00:00:10")), "*MaxDelay*",
            "it should require MaxDelay to be at least InitialDelay");

    [Fact]
    public void When_Retry_MaxDelay_Exceeds_One_Day() =>
        ShouldFailStartup(With(ValidRabbitMq, ("Retry:MaxDelay", "2.00:00:00")), "*MaxDelay*",
            "it should cap the backoff at one day, inside every transport's delay limit");

    [Fact]
    public void When_Retry_JitterFactor_Exceeds_One() =>
        ShouldFailStartup(With(ValidRabbitMq, ("Retry:JitterFactor", "1.5")), "*JitterFactor*",
            "it should keep jitter within 100% of the delay");

    [Fact]
    public void When_Retry_BackoffMultiplier_Is_Below_One() =>
        ShouldFailStartup(With(ValidRabbitMq, ("Retry:BackoffMultiplier", "0.5")), "*BackoffMultiplier*",
            "it should not let exponential backoff shrink");

    [Fact]
    public void When_Retry_Uses_A_Custom_Strategy_Without_A_Calculator() =>
        ShouldFailStartup(With(ValidRabbitMq, ("Retry:Strategy", "Custom")), "*CustomDelayCalculator*",
            "it should require a calculator for the custom strategy");

    [Fact]
    public void When_Retry_Is_Bound_From_Configuration()
    {
        using var provider = Register(With(ValidRabbitMq, ("Retry:MaxRetries", "7"), ("Retry:InitialDelay", "00:00:05")));

        var config = provider.GetRequiredService<IOptionsMonitor<RabbitMqConfig>>()
            .Get(MessageQueueServiceCollectionExtensions.OptionsName<TestMessage>());

        config.Retry.MaxRetries.Should().Be(7, "it should bind the retry policy from the Retry section");
        config.Retry.InitialDelay.Should().Be(TimeSpan.FromSeconds(5), "it should bind the retry delay");
        config.Retry.MaxDelay.Should().Be(TimeSpan.FromSeconds(30), "it should keep defaults for unset retry values");
    }

    [Fact]
    public void When_Registering_Queues_For_Two_Message_Types()
    {
        var configuration = Configuration(new Dictionary<string, string?>
        {
            ["Orders:Provider"] = "RabbitMq",
            ["Orders:ConnectionString"] = "amqp://localhost",
            ["Orders:QueueName"] = "orders",
            ["Payments:Provider"] = "InMemory",
            ["Payments:QueueName"] = "payments"
        });
        var services = new ServiceCollection();
        services.AddRabbitMqProvider().AddAzureServiceBusProvider();
        services.AddMessageQueue<TestMessage>(configuration, "Orders");
        services.AddMessageQueue<Payment>(configuration, "Payments");
        using var provider = services.BuildServiceProvider();

        StartupValidation(provider).Should().NotThrow("it should validate each queue's own section");
        provider.GetRequiredService<IOptionsMonitor<RabbitMqConfig>>().Get(MessageQueueServiceCollectionExtensions.OptionsName<TestMessage>())
            .QueueName.Should().Be("orders", "it should bind the first message type to its own section");
        provider.GetRequiredService<IMessageQueue<Payment>>().Should().BeOfType<InMemoryMessageQueue<Payment>>(
            "it should let each message type choose its own provider");
    }

    [Fact]
    public void When_One_Of_Two_Queues_Is_Invalid()
    {
        var configuration = Configuration(new Dictionary<string, string?>
        {
            ["Orders:Provider"] = "InMemory",
            ["Orders:QueueName"] = "orders",
            ["Payments:Provider"] = "InMemory",
            ["Payments:QueueName"] = "payments",
            ["Payments:MaxConcurrentCalls"] = "0"
        });
        var services = new ServiceCollection();
        services.AddRabbitMqProvider().AddAzureServiceBusProvider();
        services.AddMessageQueue<TestMessage>(configuration, "Orders");
        services.AddMessageQueue<Payment>(configuration, "Payments");
        using var provider = services.BuildServiceProvider();

        StartupValidation(provider).Should().Throw<OptionsValidationException>("it should validate every registered queue")
            .WithMessage("*MaxConcurrentCalls*");
    }

    [Fact]
    public void When_Resolving_The_Publisher_And_Subscriber()
    {
        using var provider = Register(new Dictionary<string, string?>
        {
            ["MessageQueue:Provider"] = "InMemory",
            ["MessageQueue:QueueName"] = "local"
        });

        var queue = provider.GetRequiredService<IMessageQueue<TestMessage>>();

        provider.GetRequiredService<IMessagePublisher<TestMessage>>().Should().BeSameAs(queue,
            "it should expose the queue as its publisher");
        provider.GetRequiredService<IMessageSubscriber<TestMessage>>().Should().BeSameAs(queue,
            "it should expose the queue as its subscriber");
    }
}
