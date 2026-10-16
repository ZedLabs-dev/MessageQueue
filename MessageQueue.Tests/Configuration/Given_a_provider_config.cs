using AwesomeAssertions;
using ESE.MessageQueue.AzureServiceBus;
using ESE.MessageQueue.RabbitMQ;

namespace ESE.MessageQueue.Tests.Configuration;

[Trait("Category", "Unit")]
public class Given_a_provider_config
{
    [Fact]
    public void When_Printing_A_RabbitMQ_Config()
    {
        var config = new RabbitMqConfig { ConnectionString = "amqp://user:S3CRET@rabbit:5672/", QueueName = "orders" };

        var text = config.ToString();

        text.Should().NotContain("S3CRET", "it should never print the connection string's password");
        text.Should().Contain("QueueName = orders", "it should still print the other settings");
        text.Should().Contain("ConnectionString = ***", "it should show that a connection string is set");
    }

    [Fact]
    public void When_Printing_An_AzureServiceBus_Config()
    {
        var config = new AzureServiceBusConfig
        {
            ConnectionString = "Endpoint=sb://x.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=S3CRET",
            QueueName = "orders",
            TopicName = "events"
        };

        var text = config.ToString();

        text.Should().NotContain("S3CRET", "it should never print the shared access key");
        text.Should().Contain("TopicName = events", "it should still print the other settings");
    }
}
