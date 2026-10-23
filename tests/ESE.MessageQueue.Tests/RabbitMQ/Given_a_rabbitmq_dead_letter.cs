using System.Text;
using AwesomeAssertions;
using ESE.MessageQueue.RabbitMQ;
using RabbitMQ.Client;

namespace ESE.MessageQueue.Tests.RabbitMQ;

// ADR 0023, without a broker: what the dead-letter operations read from a RabbitMQ dead letter, and what a replay
// publishes. A replay is a fresh first delivery: no attempt, reason, death or other "x-" header survives.
[Trait("Category", "Unit")]
public sealed class Given_a_rabbitmq_dead_letter
{
    static BasicProperties DeadLetter() => new()
    {
        MessageId = "order-1",
        CorrelationId = "corr-1",
        ContentType = "application/json",
        Timestamp = new AmqpTimestamp(1_700_000_000),
        Persistent = true,
        Headers = new Dictionary<string, object?>
        {
            ["tenant"] = Encoding.UTF8.GetBytes("acme"),
            ["traceparent"] = Encoding.UTF8.GetBytes("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
            ["tracestate"] = Encoding.UTF8.GetBytes("k=v"),
            ["x-delivery-attempt"] = Encoding.UTF8.GetBytes("3"),
            ["x-ese-dead-letter-reason"] = Encoding.UTF8.GetBytes("MaxRetriesExceeded"),
            ["x-ese-dead-letter-detail"] = Encoding.UTF8.GetBytes("System.TimeoutException"),
            ["x-ese-dead-lettered-at"] = 1_700_000_123_456L,
            ["X-Custom-Broker"] = "drop me",
            ["x-death"] = new List<object?> { new Dictionary<string, object?> { ["reason"] = "rejected", ["time"] = new AmqpTimestamp(1_600_000_000) } },
            ["x-first-death-reason"] = Encoding.UTF8.GetBytes("rejected")
        }
    };

    [Fact]
    public void When_Reading_A_Dead_Letter_The_Library_Wrote()
    {
        var metadata = RabbitMqDeadLetterStore.Metadata(DeadLetter());

        metadata.MessageId.Should().Be("order-1", "it should read the message id");
        metadata.CorrelationId.Should().Be("corr-1", "it should read the correlation id");
        metadata.ReasonCode.Should().Be("MaxRetriesExceeded", "it should prefer the library's reason over the broker's");
        metadata.ReasonDetail.Should().Be("System.TimeoutException", "it should read the detail");
        metadata.DeliveryAttempt.Should().Be(3, "it should read the attempt it died on");
        metadata.EnqueuedAt.Should().Be(DateTimeOffset.FromUnixTimeSeconds(1_700_000_000), "it should read the publish time");
        metadata.DeadLetteredAt.Should().Be(DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_123_456L),
            "it should prefer the library's dead-letter time over the broker's death history");
        metadata.Headers.Should().Equal(new Dictionary<string, string> { ["tenant"] = "acme" },
            "it should report only application headers, never trace, broker or library ones");
    }

    [Fact]
    public void When_Reading_A_Dead_Letter_The_Broker_Wrote()
    {
        var deathTime = new AmqpTimestamp(1_700_000_500);
        var properties = new BasicProperties
        {
            Headers = new Dictionary<string, object?>
            {
                ["x-first-death-reason"] = Encoding.UTF8.GetBytes("delivery_limit"),
                ["x-death"] = new List<object?>
                {
                    new Dictionary<string, object?> { ["reason"] = Encoding.UTF8.GetBytes("delivery_limit"), ["time"] = deathTime },
                    new Dictionary<string, object?> { ["reason"] = Encoding.UTF8.GetBytes("expired"), ["time"] = new AmqpTimestamp(1_600_000_000) }
                },
                ["x-delivery-attempt"] = Encoding.UTF8.GetBytes("-4")
            }
        };

        var metadata = RabbitMqDeadLetterStore.Metadata(properties);

        metadata.MessageId.Should().BeEmpty("it should report a missing message id as empty");
        metadata.ReasonCode.Should().Be("delivery_limit", "it should fall back to the broker's reason");
        metadata.ReasonDetail.Should().BeNull("it should report no detail when there is none");
        metadata.DeadLetteredAt.Should().Be(DateTimeOffset.FromUnixTimeSeconds(1_700_000_500),
            "it should fall back to the broker's latest death (the first x-death entry)");
        metadata.DeliveryAttempt.Should().BeNull("it should not report an attempt that is not a positive number");
        metadata.EnqueuedAt.Should().BeNull("it should report no publish time when none was set");
    }

    [Fact]
    public void When_Building_The_Replay()
    {
        var replay = RabbitMqDeadLetterStore.ReplayProperties(DeadLetter());

        replay.MessageId.Should().Be("order-1", "it should keep the message id, the idempotency key");
        replay.CorrelationId.Should().Be("corr-1", "it should keep the correlation id");
        replay.ContentType.Should().Be("application/json", "it should keep the content type");
        replay.Timestamp.Should().Be(new AmqpTimestamp(1_700_000_000), "it should keep the publish time");
        replay.Persistent.Should().BeTrue("it should stay durable");
        replay.Headers!.Keys.Should().BeEquivalentTo(["tenant", "traceparent", "tracestate"],
            "it should keep application and trace headers and drop every x- header, whatever its case, so the replay is a fresh first delivery");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void When_Declaring_The_Dead_Letter_Queue(bool quorum)
    {
        var config = new RabbitMqConfig { ConnectionString = "amqp://localhost", QueueName = "orders", UseQuorumQueues = quorum, QuorumInitialGroupSize = 3 };

        var arguments = RabbitMqDeadLetterStore.DeadLetterQueueArguments(config);

        if (quorum)
        {
            arguments.Should().Contain("x-queue-type", "quorum", "it should declare a quorum dead-letter queue for quorum queues");
            arguments.Should().Contain("x-delivery-limit", -1,
                "it should lift the quorum delivery limit, so returning a dead letter after a lost channel can never drop it");
        }
        else
        {
            arguments.Should().BeEmpty("it should declare a classic dead-letter queue without arguments");
        }
    }
}
