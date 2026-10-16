using AwesomeAssertions;
using ESE.MessageQueue.Exceptions;
using ESE.MessageQueue.RabbitMQ;

namespace ESE.MessageQueue.Tests.RabbitMQ;

[Trait("Category", "Unit")]
public class Given_an_unreachable_rabbitmq_broker
{
    [Fact]
    public void When_ConnectionString_Is_Invalid()
    {
        var config = new RabbitMqConfig
        {
            ConnectionString = "amqp://invalid:invalid@127.0.0.1:1/",
            QueueName = $"test-queue-{Guid.NewGuid():N}"
        };

        var action = () => new RabbitMqMessageQueue<TestMessage>(config);

        action.Should().Throw<MessageQueueException>("it should fail when connection cannot be established")
            .WithMessage("*Failed to initialize RabbitMQ connection*");
    }
}
