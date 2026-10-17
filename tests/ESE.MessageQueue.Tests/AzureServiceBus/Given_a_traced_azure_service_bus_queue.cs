using AwesomeAssertions;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.AzureServiceBus;
using ESE.MessageQueue.Extensions;
using ESE.MessageQueue.Telemetry;
using ESE.MessageQueue.Tests.Telemetry;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace ESE.MessageQueue.Tests.AzureServiceBus;

// B10 on Azure Service Bus, through the SDK seams: trace context travels as application properties, survives retry
// copies, and parents the consume span.
[Trait("Category", "Unit")]
public class Given_a_traced_azure_service_bus_queue : IAsyncDisposable
{
    const string TraceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    readonly FakeServiceBus bus = new();

    // The fake stands in for the SDK client; disposing it with the test class keeps CA1001 honest.
    public ValueTask DisposeAsync() => bus.DisposeAsync();
    readonly TestTelemetryProvider telemetry = new();

    [Fact]
    public async Task When_Publishing_A_Message()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue(), telemetry: telemetry);

        await queue.PublishAsync(new TestMessage("1", "x"), TestContext.Current.CancellationToken);

        bus.Sender.Sent.Single().ApplicationProperties.Should().Contain("traceparent", telemetry.InjectedTraceParent,
            "it should propagate the trace context as an application property");
        var publish = telemetry.Publishes.Should().ContainSingle("it should start one publish span").Subject;
        publish.Target.Should().Be(new TelemetryTarget("servicebus", "orders", nameof(TestMessage)) { PublishOperation = "send" },
            "it should name the system, destination and Service Bus's send operation");
        publish.Disposed.Should().BeTrue("it should end the publish span once the message is sent");
        telemetry.Published.Should().ContainSingle("it should record the publish").Which.ErrorType.Should().BeNull("it should record a success");
    }

    [Fact]
    public async Task When_Publishing_To_A_Topic()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Subscription(), telemetry: telemetry);

        await queue.PublishAsync(new TestMessage("1", "x"), TestContext.Current.CancellationToken);

        telemetry.Publishes.Single().Target.Destination.Should().Be("events", "it should name the topic it published to");
    }

    [Fact]
    public async Task When_Publishing_A_Batch()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue(), telemetry: telemetry);

        await queue.PublishAsync([new TestMessage("1", "x"), new TestMessage("2", "y"), new TestMessage("3", "z")], TestContext.Current.CancellationToken);

        telemetry.Publishes.Should().ContainSingle("it should trace a batch as one publish").Which.MessageCount.Should().Be(3,
            "it should record the batch size");
        bus.Sender.Batches.SelectMany(b => b).Should().HaveCount(3)
            .And.AllSatisfy(m => m.ApplicationProperties.Should().Contain("traceparent", telemetry.InjectedTraceParent,
                "it should propagate the trace context on every message of the batch"));
        telemetry.PublishedCount.Should().Be(3, "it should count every message of the batch");
    }

    [Fact]
    public async Task When_A_Publish_Fails()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue(), telemetry: telemetry);
        bus.Sender.SendFailure = _ => new InvalidOperationException("unavailable");

        var publish = () => queue.PublishAsync(new TestMessage("1", "x"), TestContext.Current.CancellationToken);

        await publish.Should().ThrowAsync<ESE.MessageQueue.Exceptions.MessageQueueException>("it should still report the failure to the caller");
        telemetry.Publishes.Single().Error.Should().NotBeNull("it should mark the publish span as failed");
        telemetry.Published.Single().ErrorType.Should().Be(typeof(ESE.MessageQueue.Exceptions.MessageQueueException).FullName,
            "it should record the failed publish with its error type");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_A_Traced_Message_Is_Delivered()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue(), telemetry: telemetry);
        await queue.SubscribeAsync(AzureServiceBusTestConfigs.Returns(MessageResult.Acknowledge), TestContext.Current.CancellationToken);

        var delivery = bus.Processor.Deliver(FakeServiceBus.Message("order-1", properties: new Dictionary<string, object>
        {
            ["traceparent"] = TraceParent,
            ["tracestate"] = "rojo=00f067aa0ba902b7"
        }));
        await delivery.Handled.WaitAsync(TestContext.Current.CancellationToken);

        var consume = telemetry.Consumes.Should().ContainSingle("it should trace the delivery").Subject;
        consume.Parent.Should().Be(TraceContext.Parse(TraceParent, "rojo=00f067aa0ba902b7"), "it should parent the consume span on the publisher's context");
        consume.Target.System.Should().Be("servicebus", "it should name the messaging system");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_A_Traced_Message_Is_Retried()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue(), telemetry: telemetry);
        await queue.SubscribeAsync(AzureServiceBusTestConfigs.Returns(MessageResult.Retry), TestContext.Current.CancellationToken);

        var delivery = bus.Processor.Deliver(FakeServiceBus.Message("order-1", properties: new Dictionary<string, object>
        {
            ["traceparent"] = TraceParent,
            ["tracestate"] = "rojo=00f067aa0ba902b7",
            ["Diagnostic-Id"] = TraceParent
        }));
        await delivery.Handled.WaitAsync(TestContext.Current.CancellationToken);

        var copy = bus.Sender.Sent.Should().ContainSingle("it should send a retry copy").Subject;
        copy.ApplicationProperties.Should().Contain("traceparent", TraceParent, "it should keep the original trace on the retry");
        copy.ApplicationProperties.Should().Contain("tracestate", "rojo=00f067aa0ba902b7", "it should keep the trace state on the retry");
        copy.ApplicationProperties.Should().NotContainKey("Diagnostic-Id", "it should still drop the SDK's own tracing property");
        telemetry.RetriedAttempts.Should().Equal([2], "it should count the retry");
    }

    [Fact]
    public async Task When_Publishing_Names_The_Operation_And_Message()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue(), telemetry: telemetry);

        await queue.PublishAsync(new TestMessage("1", "x"), new PublishOptions { MessageId = "order-1" }, TestContext.Current.CancellationToken);

        var publish = telemetry.Publishes.Single();
        publish.Target.PublishOperation.Should().Be("send", "it should use Service Bus's operation name for a publish");
        publish.MessageId.Should().Be("order-1", "it should name the message on a single-message publish span");
    }

    [Fact]
    public async Task When_A_Batch_Publish_Fails()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Queue(), telemetry: telemetry);
        bus.Sender.SendFailure = _ => new InvalidOperationException("unavailable");

        var publish = () => queue.PublishAsync([new TestMessage("1", "x"), new TestMessage("2", "y")], TestContext.Current.CancellationToken);

        await publish.Should().ThrowAsync<ESE.MessageQueue.Exceptions.MessageQueueException>("it should report the failure to the caller");
        telemetry.Publishes.Single().Error.Should().NotBeNull("it should mark the batch span as failed");
        telemetry.Published.Single().ErrorType.Should().NotBeNull("it should record the failed batch");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Consuming_From_A_Subscription()
    {
        await using var queue = bus.CreateQueue(AzureServiceBusTestConfigs.Subscription(), telemetry: telemetry);
        await queue.PublishAsync(new TestMessage("1", "x"), TestContext.Current.CancellationToken);
        await queue.SubscribeAsync(AzureServiceBusTestConfigs.Returns(MessageResult.Acknowledge), TestContext.Current.CancellationToken);

        var delivery = bus.Processor.Deliver(FakeServiceBus.Message("order-1"));
        await delivery.Handled.WaitAsync(TestContext.Current.CancellationToken);

        var consume = telemetry.Consumes.Single().Target;
        consume.Destination.Should().Be("events", "it should name the topic, so consume and publish telemetry join on it");
        consume.Subscription.Should().Be("billing", "it should name the subscription it consumed from");
        telemetry.Publishes.Single().Target.Destination.Should().Be(consume.Destination, "it should use the same destination on both sides");
    }

    [Fact]
    public void When_Only_The_Sdk_Diagnostic_Id_Is_Present()
    {
        var context = ServiceBusMessages.ToContext(FakeServiceBus.Message(properties: new Dictionary<string, object>
        {
            ["Diagnostic-Id"] = TraceParent
        }));

        context.TraceParent.Should().Be(TraceParent, "it should continue a trace started by the Service Bus SDK's own instrumentation");
    }

    [Fact]
    public void When_Trace_Properties_Differ_In_Case_Or_Type()
    {
        var context = ServiceBusMessages.ToContext(FakeServiceBus.Message(properties: new Dictionary<string, object>
        {
            ["TraceParent"] = System.Text.Encoding.UTF8.GetBytes(TraceParent),
            ["TRACESTATE"] = "rojo=00f067aa0ba902b7"
        }));

        context.TraceParent.Should().Be(TraceParent, "it should match trace properties case-insensitively and read byte values");
        context.TraceState.Should().Be("rojo=00f067aa0ba902b7", "it should match the tracestate case-insensitively");
        context.Headers.Should().BeEmpty("it should still keep trace properties out of the application headers");
    }

    [Fact]
    public void When_A_Trace_Property_Is_Not_Text()
    {
        var context = ServiceBusMessages.ToContext(FakeServiceBus.Message(properties: new Dictionary<string, object>
        {
            ["traceparent"] = 42,
            ["tracestate"] = new string('a', 10_000)
        }));

        context.TraceParent.Should().BeNull("it should ignore a traceparent that is not a string");
        context.TraceState.Should().BeNull("it should not carry an oversized tracestate");
    }

    [Fact]
    public async Task When_Telemetry_Is_Registered_In_The_Container()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ITelemetryProvider>(telemetry);
        services.AddSingleton<IServiceBusTransportFactory>(bus);
        services.AddAzureServiceBusProvider();
        services.AddMessageQueue<TestMessage>(new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MessageQueue:Provider"] = "AzureServiceBus",
            ["MessageQueue:ConnectionString"] = AzureServiceBusTestConfigs.ConnectionString,
            ["MessageQueue:QueueName"] = "orders"
        }).Build());
        await using var provider = services.BuildServiceProvider();

        await provider.GetRequiredService<IMessagePublisher<TestMessage>>().PublishAsync(new TestMessage("1", "x"), TestContext.Current.CancellationToken);

        telemetry.Publishes.Should().ContainSingle("it should use the telemetry provider registered in the container");
    }
}
