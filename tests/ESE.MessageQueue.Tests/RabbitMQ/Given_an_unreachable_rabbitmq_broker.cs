using AwesomeAssertions;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Exceptions;
using ESE.MessageQueue.Extensions;
using ESE.MessageQueue.Hosting;
using ESE.MessageQueue.RabbitMQ;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace ESE.MessageQueue.Tests.RabbitMQ;

// Port 1 on the loopback interface refuses connections immediately: no broker, Docker or network needed.
// The queue connects lazily (ADR 0007), so failures surface on first use, as MessageQueueException, without the
// password from the connection string anywhere in the exception chain (S10).
[Trait("Category", "Unit")]
public class Given_an_unreachable_rabbitmq_broker
{
    const string Secret = "S3CRET-PASSWORD";
    const string ConnectionString = $"amqp://app:{Secret}@127.0.0.1:1/";

    static RabbitMqConfig Config() => new() { ConnectionString = ConnectionString, QueueName = $"test-queue-{Guid.NewGuid():N}" };

    [Fact]
    public async Task When_Constructing_The_Queue()
    {
        var act = () => new RabbitMqMessageQueue<TestMessage>(Config());

        act.Should().NotThrow("it should not connect in the constructor");
        await act().DisposeAsync();
    }

    [Fact]
    public async Task When_Initializing()
    {
        await using var queue = new RabbitMqMessageQueue<TestMessage>(Config());

        var act = () => queue.InitializeAsync(TestContext.Current.CancellationToken);

        var thrown = await act.Should().ThrowAsync<MessageQueueException>("it should report that the broker cannot be reached");
        thrown.WithMessage("*Failed to connect to RabbitMQ*");
        thrown.Which.ToString().Should().NotContain(Secret, "it should not leak the connection password anywhere in the exception chain");
    }

    [Fact]
    public async Task When_Publishing_First()
    {
        await using var queue = new RabbitMqMessageQueue<TestMessage>(Config());

        var act = () => queue.PublishAsync(new TestMessage("1", "c"), TestContext.Current.CancellationToken);

        (await act.Should().ThrowAsync<MessageQueueException>("it should surface the connection failure on first use"))
            .Which.ToString().Should().NotContain(Secret, "it should not leak the connection password");
    }

    [Fact]
    public async Task When_Publishing_A_Batch_First()
    {
        await using var queue = new RabbitMqMessageQueue<TestMessage>(Config());

        var act = () => queue.PublishAsync([new TestMessage("1", "c")], TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<MessageQueueException>("it should surface the connection failure on a batch publish too");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task When_Publishing_A_Message_Above_MaxMessageSizeBytes(bool asBatch)
    {
        await using var queue = new RabbitMqMessageQueue<TestMessage>(Config() with { MaxMessageSizeBytes = 64 });
        var message = new TestMessage("1", "SENSITIVE-" + new string('x', 100));

        var act = () => asBatch
            ? queue.PublishAsync([message], TestContext.Current.CancellationToken)
            : queue.PublishAsync(message, TestContext.Current.CancellationToken);

        var error = (await act.Should().ThrowAsync<MessageQueueException>("it should refuse an oversized message")).Which;
        error.Message.Should().Contain("MaxMessageSizeBytes (64)", "it should check the size before it dials the broker")
            .And.NotContain("SENSITIVE", "it should not echo the content");
    }

    [Fact]
    public async Task When_A_Publish_Fails_It_Is_Not_Logged_As_Published()
    {
        var logger = new RecordingLogger();
        await using var queue = new RabbitMqMessageQueue<TestMessage>(Config(), logger: logger);

        var act = () => queue.PublishAsync(new TestMessage("1", "c"), TestContext.Current.CancellationToken);
        await act.Should().ThrowAsync<MessageQueueException>();

        logger.Entries.Should().NotContain(e => e.Message.StartsWith("Published", StringComparison.Ordinal),
            "it should log only publishes that reached the broker");
    }

    [Fact]
    public async Task When_Subscribing_First()
    {
        await using var queue = new RabbitMqMessageQueue<TestMessage>(Config());

        var act = () => queue.SubscribeAsync(_ => Task.FromResult(true), TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<MessageQueueException>("it should surface the connection failure when subscribing");
        var retry = () => queue.SubscribeAsync(_ => Task.FromResult(true), TestContext.Current.CancellationToken);
        await retry.Should().ThrowAsync<MessageQueueException>(
            "it should try to connect again on the next call instead of reporting it already has a subscriber");
    }

    [Fact]
    public async Task When_Creating_And_Connecting_In_One_Call()
    {
        var act = () => RabbitMqMessageQueue.CreateAsync<TestMessage>(Config(), cancellationToken: TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<MessageQueueException>("it should surface the connection failure from the factory");
    }

    [Fact]
    public async Task When_Resolving_From_Dependency_Injection()
    {
        var services = new ServiceCollection();
        services.AddRabbitMqProvider();
        services.AddMessageQueue<TestMessage>(new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MessageQueue:Provider"] = "RabbitMq",
            ["MessageQueue:ConnectionString"] = ConnectionString,
            ["MessageQueue:QueueName"] = "orders"
        }).Build());
        await using var provider = services.BuildServiceProvider();

        var resolve = () => provider.GetRequiredService<IMessagePublisher<TestMessage>>();

        var publisher = resolve.Should().NotThrow("it should resolve without connecting").Subject;
        var publish = () => publisher.PublishAsync(new TestMessage("1", "c"), TestContext.Current.CancellationToken);
        (await publish.Should().ThrowAsync<MessageQueueException>("it should connect, and fail, on first use"))
            .Which.ToString().Should().NotContain(Secret, "it should not leak the connection password");
    }

    [Fact]
    public async Task When_Used_After_Dispose()
    {
        var queue = new RabbitMqMessageQueue<TestMessage>(Config());
        await queue.DisposeAsync();
        await queue.DisposeAsync();

        var act = () => queue.PublishAsync(new TestMessage("1", "c"), TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<ObjectDisposedException>("it should not reconnect after it was disposed");
    }

    [Fact]
    public async Task When_A_Hosted_Service_Starts()
    {
        await using var queue = new RabbitMqMessageQueue<TestMessage>(Config());
        await using var services = new ServiceCollection().BuildServiceProvider();
        using var hosted = new MessageQueueHostedService<TestMessage>(
            queue, services.GetRequiredService<IServiceScopeFactory>(), NullLogger<MessageQueueHostedService<TestMessage>>.Instance);

        var act = () => hosted.StartAsync(TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<MessageQueueException>("it should fail host start when the broker cannot be reached");
    }

    [Fact]
    public async Task When_Connecting_Again_Right_After_A_Failure()
    {
        var logger = new RecordingLogger();
        await using var queue = new RabbitMqMessageQueue<TestMessage>(Config(), logger: logger);
        var first = () => queue.InitializeAsync(TestContext.Current.CancellationToken);
        await first.Should().ThrowAsync<MessageQueueException>("it should report the failed connection");

        var again = () => queue.InitializeAsync(TestContext.Current.CancellationToken);

        (await again.Should().ThrowAsync<MessageQueueException>("it should still report the broker as unreachable"))
            .WithMessage("*retry*");
        logger.Entries.Count(e => e.Level == Microsoft.Extensions.Logging.LogLevel.Error).Should().Be(1,
            "it should back off instead of dialling the broker again immediately");
    }
    [Fact]
    public async Task When_A_Publish_Is_Refused_For_Its_Size()
    {
        var telemetry = new ESE.MessageQueue.Tests.Telemetry.TestTelemetryProvider();
        await using var queue = new RabbitMqMessageQueue<TestMessage>(Config() with { MaxMessageSizeBytes = 16 }, telemetry);

        var single = () => queue.PublishAsync(new TestMessage("1", new string('x', 100)), TestContext.Current.CancellationToken);
        var batch = () => queue.PublishAsync([new TestMessage("1", new string('x', 100))], TestContext.Current.CancellationToken);
        (await single.Should().ThrowAsync<MessageQueueException>()).WithMessage("*for 'test-queue-*'; it was not published");
        (await batch.Should().ThrowAsync<MessageQueueException>()).WithMessage("*index 0 of the batch*; no message of the batch was published");

        telemetry.Published.Should().BeEmpty("it should record no publish metric for a message it refused before sending");
        telemetry.Activities.Should().BeEmpty("it should start no publish span for a message it refused before sending");
    }

    [Fact]
    public async Task When_Reading_Its_Shutdown_Budget()
    {
        await using var queue = new RabbitMqMessageQueue<TestMessage>(Config() with { ShutdownTimeout = TimeSpan.FromSeconds(7) });

        ((ESE.MessageQueue.Hosting.IShutdownBudget)queue).ShutdownTimeout.Should().Be(TimeSpan.FromSeconds(7),
            "it should tell the hosted service how long its drain may take");
    }
}
