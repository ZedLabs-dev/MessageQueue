using AwesomeAssertions;
using ESE.MessageQueue.AzureServiceBus;

namespace ESE.MessageQueue.Tests.AzureServiceBus;

// Mapping a received message to MessageContext, in particular the attempt number. Library-written properties count
// only when they are consistent with the broker's DeliveryCount; otherwise DeliveryCount alone is the attempt.
[Trait("Category", "Unit")]
public class Given_an_azure_service_bus_delivery
{
    static Dictionary<string, object> Library(int attempt, int expectedDeliveryCount, string? originalId = null)
    {
        var properties = new Dictionary<string, object>
        {
            [ServiceBusMessages.AttemptProperty] = attempt,
            [ServiceBusMessages.DeliveryCountProperty] = expectedDeliveryCount
        };
        if (originalId is not null)
            properties[ServiceBusMessages.OriginalMessageIdProperty] = originalId;
        return properties;
    }

    [Fact]
    public void When_Mapping_A_Received_Message_To_A_Context()
    {
        var enqueued = new DateTimeOffset(2026, 10, 16, 12, 0, 0, TimeSpan.Zero);
        var received = FakeServiceBus.Message("order-1", deliveryCount: 3, enqueuedTime: enqueued, correlationId: "checkout-1",
            properties: new Dictionary<string, object> { ["tenant"] = "acme", ["priority"] = 5 });

        var context = ServiceBusMessages.ToContext(received);

        context.MessageId.Should().Be("order-1", "it should use the broker's message id");
        context.CorrelationId.Should().Be("checkout-1", "it should use the broker's correlation id");
        context.Timestamp.Should().Be(enqueued, "it should use the enqueue time");
        context.DeliveryAttempt.Should().Be(3, "it should use the broker's delivery count when the library has not rescheduled the message");
        context.Headers.Should().Contain("tenant", "acme", "it should expose string application properties as headers");
        context.Headers.Should().Contain("priority", "5", "it should format other application properties invariantly");
    }

    [Fact]
    public void When_A_Retry_Copy_Is_Delivered()
    {
        var received = FakeServiceBus.Message("copy-id", deliveryCount: 1, properties: Library(attempt: 3, expectedDeliveryCount: 1, originalId: "order-1"));

        var context = ServiceBusMessages.ToContext(received);

        context.DeliveryAttempt.Should().Be(3, "it should continue the attempt count the library wrote on its retry copy");
        context.MessageId.Should().Be("order-1", "it should keep the original message id across retry copies");
    }

    [Fact]
    public void When_A_Retry_Copy_Lost_Its_Lock()
    {
        var received = FakeServiceBus.Message("copy-id", deliveryCount: 2, properties: Library(attempt: 3, expectedDeliveryCount: 1, originalId: "order-1"));

        ServiceBusMessages.ToContext(received).DeliveryAttempt.Should().Be(4,
            "it should count a broker redelivery of a retry copy as one more attempt");
    }

    [Fact]
    public void When_A_Subscription_Retry_Is_Redelivered()
    {
        var received = FakeServiceBus.Message("order-1", deliveryCount: 2, properties: Library(attempt: 2, expectedDeliveryCount: 2));

        ServiceBusMessages.ToContext(received).DeliveryAttempt.Should().Be(2,
            "it should not count an abandon that already updated the attempt twice");
    }

    [Fact]
    public void When_The_Attempt_Property_Comes_Without_A_Delivery_Count()
    {
        var received = FakeServiceBus.Message("order-1", deliveryCount: 1, properties: new Dictionary<string, object>
        {
            [ServiceBusMessages.AttemptProperty] = 5,
            [ServiceBusMessages.OriginalMessageIdProperty] = "someone-else"
        });

        var context = ServiceBusMessages.ToContext(received);

        context.DeliveryAttempt.Should().Be(1, "it should ignore an attempt property the library did not write");
        context.MessageId.Should().Be("order-1", "it should ignore an original id the library did not write");
    }

    [Fact]
    public void When_The_Delivery_Count_Property_Is_Ahead_Of_The_Broker()
    {
        var received = FakeServiceBus.Message("order-1", deliveryCount: 1, properties: Library(attempt: 5, expectedDeliveryCount: 4));

        ServiceBusMessages.ToContext(received).DeliveryAttempt.Should().Be(1,
            "it should distrust library properties that disagree with the broker's delivery count");
    }

    [Theory]
    [InlineData("three")]
    [InlineData("0")]
    [InlineData("-2")]
    public void When_The_Attempt_Property_Is_Not_A_Positive_Number(string attempt)
    {
        var received = FakeServiceBus.Message("order-1", deliveryCount: 2, properties: new Dictionary<string, object>
        {
            [ServiceBusMessages.AttemptProperty] = attempt,
            [ServiceBusMessages.DeliveryCountProperty] = 1
        });

        ServiceBusMessages.ToContext(received).DeliveryAttempt.Should().Be(2, "it should fall back to the broker's delivery count");
    }

    [Fact]
    public void When_Library_And_Diagnostic_Properties_Are_Present()
    {
        var properties = Library(attempt: 2, expectedDeliveryCount: 1, originalId: "order-1");
        properties["tenant"] = "acme";
        properties["traceparent"] = "00-abc-def-01";
        properties["Diagnostic-Id"] = "00-abc-def-01";
        properties["x-foreign"] = "broker";

        var headers = ServiceBusMessages.ToContext(FakeServiceBus.Message(properties: properties)).Headers;

        headers.Keys.Should().Equal(["tenant"], "it should strip library, broker and SDK tracing properties from the application headers");
    }

    [Fact]
    public void When_The_Attempt_Property_Overflows()
    {
        var received = FakeServiceBus.Message("order-1", deliveryCount: 2, properties: new Dictionary<string, object>
        {
            [ServiceBusMessages.AttemptProperty] = long.MaxValue.ToString(System.Globalization.CultureInfo.InvariantCulture),
            [ServiceBusMessages.DeliveryCountProperty] = 1
        });

        ServiceBusMessages.ToContext(received).DeliveryAttempt.Should().Be(int.MaxValue,
            "it should saturate instead of overflowing to an attempt below 1");
    }

    [Fact]
    public void When_The_Broker_Delivery_Count_Is_At_Its_Maximum()
    {
        var properties = ServiceBusMessages.AttemptProperties(FakeServiceBus.Message("order-1", deliveryCount: int.MaxValue), int.MaxValue);

        properties[ServiceBusMessages.DeliveryCountProperty].Should().Be(int.MaxValue, "it should saturate instead of overflowing");
    }
}
