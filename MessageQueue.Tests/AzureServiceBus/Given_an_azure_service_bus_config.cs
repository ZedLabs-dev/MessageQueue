using AwesomeAssertions;
using ESE.MessageQueue.AzureServiceBus;

namespace ESE.MessageQueue.Tests.AzureServiceBus;

[Trait("Category", "Unit")]
public class Given_an_azure_service_bus_config
{
    readonly AzureServiceBusConfig config = new()
    {
        ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=test-key",
        QueueName = "test-queue"
    };

    [Fact]
    public void When_Config_Has_SenderPoolSize()
    {
        var poolConfig = config with { SenderPoolSize = 4 };

        poolConfig.SenderPoolSize.Should().Be(4, "it should have the configured sender pool size");
    }

    [Fact]
    public void When_Config_Has_MaxConcurrentSends()
    {
        var concurrentConfig = config with { MaxConcurrentSends = 500 };

        concurrentConfig.MaxConcurrentSends.Should().Be(500, "it should have the configured max concurrent sends");
    }

    [Fact]
    public void When_Config_Has_SendTimeout()
    {
        var timeoutConfig = config with { SendTimeout = TimeSpan.FromSeconds(60) };

        timeoutConfig.SendTimeout.Should().Be(TimeSpan.FromSeconds(60), "it should have the configured send timeout");
    }

    [Fact]
    public void When_Config_Has_TopicName()
    {
        var topicConfig = config with { TopicName = "test-topic", SubscriptionName = "test-subscription" };

        topicConfig.TopicName.Should().Be("test-topic", "it should have the configured topic name");
        topicConfig.SubscriptionName.Should().Be("test-subscription", "it should have the configured subscription name");
    }

    [Fact]
    public void When_Config_Has_Default_Values()
    {
        config.SenderPoolSize.Should().Be(2, "it should have default sender pool size of 2");
        config.MaxConcurrentSends.Should().Be(1000, "it should have default max concurrent sends of 1000");
        config.SendTimeout.Should().Be(TimeSpan.FromSeconds(30), "it should have default send timeout of 30 seconds");
        config.TopicName.Should().BeNull("it should have no topic name by default");
        config.SubscriptionName.Should().BeNull("it should have no subscription name by default");
    }
}
