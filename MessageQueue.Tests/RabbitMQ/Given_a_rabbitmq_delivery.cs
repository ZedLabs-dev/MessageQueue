using System.Text;
using AwesomeAssertions;
using ESE.MessageQueue.RabbitMQ;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ESE.MessageQueue.Tests.RabbitMQ;

// How broker metadata becomes a MessageContext, and what a retry or dead-letter copy carries. Attempt headers are
// trusted only where the library or the broker set them: x-delivery-attempt on messages that come back from this
// queue's retry queues, x-delivery-count on redeliveries.
[Trait("Category", "Unit")]
public class Given_a_rabbitmq_delivery
{
    const string RetryPrefix = "orders.retry.";

    static BasicDeliverEventArgs Delivery(bool redelivered = false, params (string Key, object? Value)[] headers) =>
        new("consumer", 1, redelivered, "exchange", "queue",
            new BasicProperties
            {
                MessageId = "m-1",
                CorrelationId = "c-1",
                Headers = headers.Length == 0 ? null : headers.ToDictionary(h => h.Key, h => h.Value)
            },
            ReadOnlyMemory<byte>.Empty);

    static byte[] Utf8(string value) => Encoding.UTF8.GetBytes(value);

    static int Attempt(BasicDeliverEventArgs delivery, bool quorum = true) =>
        RabbitMqMessageQueue<TestMessage>.ToContext(delivery, RetryPrefix, quorum).DeliveryAttempt;

    static readonly (string, object?) FromRetryQueue = ("x-first-death-queue", Utf8("orders.retry.2000"));

    [Fact]
    public void When_It_Is_The_First_Delivery() =>
        Attempt(Delivery()).Should().Be(1, "it should start at attempt 1");

    [Fact]
    public void When_It_Comes_Back_From_A_Retry_Queue() =>
        Attempt(Delivery(false, ("x-delivery-attempt", Utf8("3")), FromRetryQueue)).Should()
            .Be(3, "it should read the attempt the library's retry copy carries");

    [Fact]
    public void When_A_Publisher_Sets_The_Attempt_Header() =>
        Attempt(Delivery(false, ("x-delivery-attempt", Utf8("100")))).Should()
            .Be(1, "it should ignore an attempt header that did not come through one of this queue's retry queues");

    [Fact]
    public void When_The_Attempt_Header_Claims_Another_Queues_Retry_Queue() =>
        Attempt(Delivery(false, ("x-delivery-attempt", Utf8("100")), ("x-first-death-queue", Utf8("payments.retry.2000")))).Should()
            .Be(1, "it should trust only this queue's retry queues");

    [Fact]
    public void When_A_Publisher_Sets_The_Delivery_Count() =>
        Attempt(Delivery(false, ("x-delivery-count", 100L))).Should()
            .Be(1, "it should ignore a delivery count on a message the broker has not redelivered");

    [Fact]
    public void When_A_Quorum_Queue_Redelivers_A_Retry() =>
        Attempt(Delivery(true, ("x-delivery-attempt", Utf8("3")), FromRetryQueue, ("x-delivery-count", 2L))).Should()
            .Be(5, "it should add every unsettled redelivery the quorum queue counted");

    [Fact]
    public void When_A_Classic_Queue_Redelivers_A_Message_With_A_Delivery_Count() =>
        Attempt(Delivery(true, ("x-delivery-count", 100L)), quorum: false).Should()
            .Be(2, "it should ignore x-delivery-count on a classic queue, where only a publisher could have set it");

    [Fact]
    public void When_A_Classic_Queue_Redelivers() =>
        Attempt(Delivery(true, ("x-delivery-attempt", Utf8("2")), FromRetryQueue)).Should()
            .Be(3, "it should count a redelivery flagged by a classic queue as one used attempt");

    [Theory]
    [InlineData("0")]
    [InlineData("-4")]
    [InlineData("many")]
    [InlineData("99999999999999999999")]
    public void When_The_Attempt_Header_Is_Invalid(string value) =>
        Attempt(Delivery(false, ("x-delivery-attempt", Utf8(value)), FromRetryQueue)).Should().Be(1, "it should fall back to the first attempt");

    [Theory]
    [InlineData(long.MaxValue)]
    [InlineData(long.MinValue)]
    public void When_The_Timestamp_Is_Out_Of_Range(long unixTime)
    {
        var delivery = new BasicDeliverEventArgs("consumer", 1, false, "exchange", "queue",
            new BasicProperties { MessageId = "m-1", Timestamp = new AmqpTimestamp(unixTime) }, ReadOnlyMemory<byte>.Empty);

        var act = () => RabbitMqMessageQueue<TestMessage>.ToContext(delivery, RetryPrefix, quorum: true);

        act.Should().NotThrow("it should not let a malformed timestamp stop the delivery")
            .Which.Timestamp.Should().Be(default, "it should leave a timestamp it cannot represent unset");
    }

    [Fact]
    public void When_The_Delivery_Carries_Broker_And_Library_Headers()
    {
        var context = RabbitMqMessageQueue<TestMessage>.ToContext(Delivery(false,
            ("tenant", Utf8("acme")),
            ("priority", 5),
            ("x-death", new List<object>()),
            ("x-first-death-reason", Utf8("expired")),
            ("x-dotnet-pub-seq-no", 7UL),
            ("x-ese-dead-letter-reason", Utf8("Rejected")),
            ("x-delivery-attempt", Utf8("2")),
            ("traceparent", Utf8("00-abc-def-01"))), RetryPrefix, quorum: true);

        context.Headers.Should().BeEquivalentTo(new Dictionary<string, string> { ["tenant"] = "acme", ["priority"] = "5" },
            "it should pass application headers and strip broker, client, telemetry and library headers");
        context.MessageId.Should().Be("m-1", "it should map the message id");
        context.CorrelationId.Should().Be("c-1", "it should map the correlation id");
    }

    [Fact]
    public void When_Copying_A_Message_For_A_Retry()
    {
        var original = new BasicProperties
        {
            MessageId = "m-1",
            CorrelationId = "c-1",
            ContentType = "application/json",
            Headers = new Dictionary<string, object?>
            {
                ["tenant"] = Utf8("acme"),
                ["traceparent"] = Utf8("00-abc-def-01"),
                ["x-foreign"] = Utf8("1"),
                ["x-death"] = new List<object>(),
                ["x-delivery-attempt"] = Utf8("1")
            }
        };

        var copy = RabbitMqMessageQueue<TestMessage>.CopyProperties(original, attempt: 2);

        copy.Headers.Should().ContainKeys(["tenant", "traceparent"], "it should keep application and trace headers");
        copy.Headers!.Keys.Should().NotContain(["x-foreign", "x-death"], "it should not carry broker or foreign x- headers into the copy");
        copy.Headers["x-delivery-attempt"].Should().Be("2", "it should carry the next attempt");
        copy.Persistent.Should().BeTrue("it should keep the copy durable");
        copy.MessageId.Should().Be("m-1", "it should keep the message id for de-duplication");
    }
}
