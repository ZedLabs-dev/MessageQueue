using AwesomeAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ZedLabs.MessageQueue.AzureServiceBus;
using ZedLabs.MessageQueue.Retry;

namespace ZedLabs.MessageQueue.Tests.AzureServiceBus;

// The ServiceBusClient connects lazily, so constructing the real queue needs no broker.
[Trait("Category", "Unit")]
public class Given_an_azure_service_bus_queue
{
    readonly AzureServiceBusConfig config = new()
    {
        ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=test-key",
        QueueName = "test-queue",
        Retry = new RetryPolicy { MaxRetries = 2 },
        EnableDeadLetterQueue = true,
        MessageTimeToLive = TimeSpan.FromDays(1),
        MaxConcurrentCalls = 5,
        PrefetchCount = 10
    };

    [Fact]
    public void When_ConnectionString_Is_Invalid()
    {
        var invalidConfig = config with { ConnectionString = "invalid-connection-string" };

        var action = () => new AzureServiceBusMessageQueue<TestMessage>(invalidConfig);

        action.Should().Throw<OptionsValidationException>("it should reject a malformed connection string before building a client")
            .WithMessage("*ConnectionString*");
    }

    [Fact]
    public void When_Config_Is_Valid()
    {
        var action = () =>
        {
            using var queue = new AzureServiceBusMessageQueue<TestMessage>(config);
        };

        action.Should().NotThrow("it should create instance with valid configuration");
    }

    [Fact]
    public async Task When_The_Retry_Policy_Never_Retries()
    {
        var logger = new RecordingLogger();

        await using var queue = new AzureServiceBusMessageQueue<TestMessage>(config with { Retry = RetryPolicy.None }, logger: logger);

        logger.Entries.Should().NotContain(e => e.Level == LogLevel.Warning, "it should not warn when there is no backoff to honour");
    }
}
