using System.Collections.Concurrent;
using System.Diagnostics;
using AwesomeAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using ZedLabs.MessageQueue.Abstractions;
using ZedLabs.MessageQueue.Extensions;
using ZedLabs.MessageQueue.RabbitMQ;
using ZedLabs.MessageQueue.Telemetry;
using ZedLabs.MessageQueue.Telemetry.OpenTelemetry;

namespace ZedLabs.MessageQueue.Tests.RabbitMQ;

// B10 across a real broker: the trace context crosses RabbitMQ in message headers, through the retry queues too.
[Trait("Category", "Integration")]
[Collection(RabbitMqCollection.Name)]
public sealed class Given_a_traced_rabbitmq_queue : IAsyncLifetime
{
    readonly RabbitMqBrokerFixture broker;
    readonly RabbitMqTestQueues queues;
    readonly SpanCapture spans = new();
    readonly MetricCapture metrics;
    string queueName = "";

    public Given_a_traced_rabbitmq_queue(RabbitMqBrokerFixture broker)
    {
        this.broker = broker;
        queues = new RabbitMqTestQueues(broker);
        metrics = new MetricCapture(spans.SourceName);
    }

    public ValueTask InitializeAsync()
    {
        broker.SkipUnlessAvailable();
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        await queues.DisposeAsync();
        metrics.Dispose();
        spans.Dispose();
    }

    // Registered through the container, so the spec also proves that DI hands the queue its telemetry provider.
    ServiceProvider Services(RabbitMqConfig config)
    {
        queues.Track(config);
        var services = new ServiceCollection();
        services.AddMessageQueueTelemetry(spans.SourceName);
        services.AddRabbitMqProvider();
        services.AddMessageQueue<TestMessage>(new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MessageQueue:Provider"] = "RabbitMq",
            ["MessageQueue:ConnectionString"] = config.ConnectionString,
            ["MessageQueue:RequireTls"] = "false",
            ["MessageQueue:QueueName"] = config.QueueName,
            ["MessageQueue:UseQuorumQueues"] = "true",
            ["MessageQueue:QuorumInitialGroupSize"] = "1",
            ["MessageQueue:Retry:MaxRetries"] = "2",
            ["MessageQueue:Retry:InitialDelay"] = "00:00:00.020",
            ["MessageQueue:Retry:MaxDelay"] = "00:00:00.100",
            ["MessageQueue:Retry:UseJitter"] = "false"
        }).Build());
        return services.BuildServiceProvider();
    }

    [Fact(Timeout = 60_000)]
    public async Task When_A_Message_Crosses_The_Broker()
    {
        var config = queues.NewConfig("traced");
        queueName = config.QueueName;
        await using var services = Services(config);
        var queue = services.GetRequiredService<IMessageQueue<TestMessage>>();
        var attempts = new ConcurrentQueue<int>();

        await queue.SubscribeAsync((_, context, _) =>
        {
            attempts.Enqueue(context.DeliveryAttempt);
            return Task.FromResult(context.DeliveryAttempt == 1 ? MessageResult.Retry : MessageResult.Acknowledge);
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("order-1", "content"), TestContext.Current.CancellationToken);
        var consumes = await spans.WaitForAsync(ActivityKind.Consumer, 2);

        var publish = spans.Single(ActivityKind.Producer);
        publish.GetTagItem("messaging.system").Should().Be("rabbitmq", "it should name the broker");
        publish.GetTagItem("messaging.destination.name").Should().Be($"{queueName}:{queueName}",
            "it should name the destination exchange:routing-key, as the conventions require for RabbitMQ");
        publish.GetTagItem("messaging.rabbitmq.destination.routing_key").Should().Be(queueName, "it should record the routing key");
        publish.GetTagItem("messaging.message.id").Should().NotBeNull("it should name the single published message");
        consumes.Should().HaveCount(2, "it should trace the first attempt and the retry");
        consumes.Should().AllSatisfy(c =>
        {
            c.TraceId.Should().Be(publish.TraceId, "it should continue the publisher's trace across the broker");
            c.ParentSpanId.Should().Be(publish.SpanId, "it should parent every attempt, retries included, on the publish span");
            c.GetTagItem("messaging.destination.name").Should().Be(publish.GetTagItem("messaging.destination.name"),
                "it should use the same destination on both sides, so publish and consume telemetry join");
        });
        metrics.Sum("zedlabs.messagequeue.messages.retried").Should().Be(1, "it should count the retry");
        metrics.Sum("messaging.client.sent.messages").Should().Be(1, "it should count the publish");
    }
}
