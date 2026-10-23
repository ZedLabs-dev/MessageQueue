using AwesomeAssertions;
using ESE.MessageQueue.Extensions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace ESE.MessageQueue.Tests.Configuration;

// One spec per ValidateOnStart rule that had none: the setting is out of range, and startup fails naming it.
[Trait("Category", "Unit")]
public class Given_every_startup_rule
{
    static readonly Dictionary<string, string?> RabbitMq = new()
    {
        ["MessageQueue:Provider"] = "RabbitMq",
        ["MessageQueue:ConnectionString"] = "amqp://localhost",
        ["MessageQueue:QueueName"] = "orders"
    };

    static readonly Dictionary<string, string?> AzureServiceBus = new()
    {
        ["MessageQueue:Provider"] = "AzureServiceBus",
        ["MessageQueue:ConnectionString"] = AzureServiceBusTestConfigs.ConnectionString,
        ["MessageQueue:QueueName"] = "orders"
    };

    static Action Startup(Dictionary<string, string?> baseline, string key, string value)
    {
        var values = new Dictionary<string, string?>(baseline) { [$"MessageQueue:{key}"] = value };
        var services = new ServiceCollection().AddRabbitMqProvider().AddAzureServiceBusProvider();
        services.AddMessageQueue<TestMessage>(new ConfigurationBuilder().AddInMemoryCollection(values).Build());
        var provider = services.BuildServiceProvider();
        return () =>
        {
            using (provider)
                provider.GetRequiredService<IStartupValidator>().Validate();
        };
    }

    [Theory]
    [InlineData("HeartbeatInterval", "-00:00:01")]
    [InlineData("NetworkRecoveryInterval", "00:00:00")]
    [InlineData("QuorumInitialGroupSize", "0")]
    [InlineData("Port", "65536")]
    [InlineData("Port", "-1")]
    [InlineData("ExchangeType", "")]
    [InlineData("ExchangeType", "Direct")]
    [InlineData("ExchangeType", "x-")]
    [InlineData("PublisherChannelPoolSize", "65")]
    [InlineData("MaxConcurrentCalls", "1001")]
    [InlineData("Retry:BackoffMultiplier", "100.5")]
    [InlineData("Retry:BackoffMultiplier", "NaN")]
    [InlineData("Retry:JitterFactor", "-0.1")]
    [InlineData("Retry:JitterFactor", "NaN")]
    public void When_A_RabbitMq_Setting_Is_Out_Of_Range(string key, string value) =>
        Startup(RabbitMq, key, value).Should().Throw<OptionsValidationException>($"it should reject {key} = '{value}' at startup")
            .WithMessage($"*{key.Split(':')[^1]}*");

    [Theory]
    [InlineData("SendTimeout", "00:00:00")]
    [InlineData("MessageTimeToLive", "00:00:00")]
    [InlineData("MaxAutoLockRenewalDuration", "-00:00:01")]
    [InlineData("PrefetchCount", "-1")]
    [InlineData("MaxConcurrentCalls", "1001")]
    public void When_An_Azure_Service_Bus_Setting_Is_Out_Of_Range(string key, string value) =>
        Startup(AzureServiceBus, key, value).Should().Throw<OptionsValidationException>($"it should reject {key} = '{value}' at startup")
            .WithMessage($"*{key}*");

    // B16: settings removed in Phase 4 fail strict binding instead of being ignored.
    [Theory]
    [InlineData("RetryAttempts", "3")]
    [InlineData("EnablePartitioning", "true")]
    [InlineData("RequiresDuplicateDetection", "true")]
    [InlineData("DuplicateDetectionHistoryTimeWindow", "00:10:00")]
    [InlineData("BatchFlushInterval", "00:00:01")]
    [InlineData("SenderPoolSize", "4")]
    [InlineData("MaxConcurrentSends", "4")]
    public void When_A_Removed_Setting_Is_Configured(string key, string value)
    {
        Startup(AzureServiceBus, key, value).Should().Throw<InvalidOperationException>(
            $"it should fail on the removed setting {key} instead of silently ignoring it").WithMessage($"*{key}*");
        Startup(RabbitMq, key, value).Should().Throw<InvalidOperationException>(
            $"it should fail on the removed setting {key} for RabbitMQ too").WithMessage($"*{key}*");
    }

    [Fact]
    public void When_The_Removed_Azure_Dead_Letter_Queue_Name_Is_Configured() =>
        Startup(AzureServiceBus, "DeadLetterQueueName", "custom-dlq").Should().Throw<InvalidOperationException>(
            "it should reject DeadLetterQueueName on Service Bus, whose dead-letter queue is the fixed subqueue").WithMessage("*DeadLetterQueueName*");
}
