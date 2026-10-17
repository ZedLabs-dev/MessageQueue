using AwesomeAssertions;
using ESE.MessageQueue.AzureServiceBus;
using ESE.MessageQueue.InMemory;
using ESE.MessageQueue.RabbitMQ;
using Microsoft.Extensions.Options;

namespace ESE.MessageQueue.Tests.Configuration;

// Constructors validate with the same rules as startup validation, so a bad config fails fast instead of
// hanging on a connection attempt or misbehaving later.
[Trait("Category", "Unit")]
public class Given_a_directly_constructed_queue
{
    [Fact]
    public void When_The_RabbitMQ_Config_Is_Invalid()
    {
        var act = () => new RabbitMqMessageQueue<TestMessage>(new RabbitMqConfig { QueueName = "orders", PrefetchCount = 0 });

        act.Should().Throw<OptionsValidationException>("it should validate before connecting")
            .WithMessage("*ConnectionString*").WithMessage("*PrefetchCount*");
    }

    [Fact]
    public void When_The_AzureServiceBus_Config_Is_Invalid()
    {
        var act = () => new AzureServiceBusMessageQueue<TestMessage>(new AzureServiceBusConfig
        {
            ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=v",
            QueueName = "orders",
            TopicName = "events"
        });

        act.Should().Throw<OptionsValidationException>("it should apply the provider's validator").WithMessage("*SubscriptionName*");
    }

    [Fact]
    public void When_The_InMemory_Config_Is_Invalid()
    {
        var act = () => new InMemoryMessageQueue<TestMessage>(new InMemoryConfig { QueueName = "orders", MaxConcurrentCalls = 0 });

        act.Should().Throw<OptionsValidationException>("it should validate the base settings").WithMessage("*MaxConcurrentCalls*");
    }

    [Fact]
    public void When_The_InMemory_Config_Has_No_QueueName()
    {
        var act = () => new InMemoryMessageQueue<TestMessage>(new InMemoryConfig());

        act.Should().Throw<OptionsValidationException>("it should require a queue name").WithMessage("*QueueName*");
    }

    [Fact]
    public async Task When_No_InMemory_Config_Is_Given()
    {
        await using var queue = new InMemoryMessageQueue<TestMessage>();

        queue.Capabilities.Durable.Should().BeFalse("it should construct with defaults named after the message type");
    }
}
