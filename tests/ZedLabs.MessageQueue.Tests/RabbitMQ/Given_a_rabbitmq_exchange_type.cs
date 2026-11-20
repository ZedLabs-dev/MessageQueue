using AwesomeAssertions;
using Microsoft.Extensions.Options;
using ZedLabs.MessageQueue.RabbitMQ;

namespace ZedLabs.MessageQueue.Tests.RabbitMQ;

// ExchangeType is passed to exchange.declare as is: RabbitMQ knows the four built-in types and plugin types named
// "x-...", all lowercase and case-sensitive. Anything else would fail only on first connect, as a channel error.
[Trait("Category", "Unit")]
public class Given_a_rabbitmq_exchange_type
{
    static ValidateOptionsResult Validate(string exchangeType) =>
        new RabbitMqConfigValidator().Validate(null, new RabbitMqConfig
        {
            ConnectionString = "amqp://localhost/",
            QueueName = "orders",
            ExchangeType = exchangeType
        });

    [Theory]
    [InlineData("direct")]
    [InlineData("fanout")]
    [InlineData("topic")]
    [InlineData("headers")]
    [InlineData("x-consistent-hash")]
    [InlineData("x-delayed-message")]
    public void When_The_Type_Is_Known_To_RabbitMQ(string exchangeType) =>
        Validate(exchangeType).Succeeded.Should().BeTrue($"it should accept the exchange type '{exchangeType}'");

    [Theory]
    [InlineData("Direct")]
    [InlineData("TOPIC")]
    [InlineData("fan-out")]
    [InlineData(" direct")]
    [InlineData("x-")]
    [InlineData("X-consistent-hash")]
    [InlineData("delayed")]
    public void When_The_Type_Is_Unknown(string exchangeType)
    {
        var result = Validate(exchangeType);

        result.Failed.Should().BeTrue($"it should reject the exchange type '{exchangeType}' at startup instead of on first connect");
        result.FailureMessage.Should().Contain($"ExchangeType '{exchangeType}' is not supported", "it should name the setting and the value")
            .And.Contain("direct, fanout, topic or headers", "it should list the built-in types")
            .And.Contain("'x-'", "it should say how plugin types are named");
    }

    [Fact]
    public void When_The_Type_Is_Empty() =>
        Validate(string.Empty).FailureMessage.Should().NotContain("is not supported",
            "it should leave an empty value to the [Required] rule instead of reporting it twice");
}
