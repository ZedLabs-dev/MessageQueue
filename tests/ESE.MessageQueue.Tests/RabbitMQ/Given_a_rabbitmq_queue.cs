using AwesomeAssertions;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Extensions;
using ESE.MessageQueue.HealthChecks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ESE.MessageQueue.Tests.RabbitMQ;

[Trait("Category", "Integration")]
[Collection(RabbitMqCollection.Name)]
public sealed class Given_a_rabbitmq_queue : IAsyncLifetime
{
    readonly RabbitMqBrokerFixture broker;
    readonly RabbitMqTestQueues queues;

    public Given_a_rabbitmq_queue(RabbitMqBrokerFixture broker)
    {
        this.broker = broker;
        queues = new RabbitMqTestQueues(broker);
    }

    public ValueTask InitializeAsync()
    {
        broker.SkipUnlessAvailable();
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync() => queues.DisposeAsync();

    [Fact]
    public async Task When_Config_Is_Valid()
    {
        var queue = queues.Create(queues.NewConfig("test-queue"));

        var action = () => queue.InitializeAsync(TestContext.Current.CancellationToken);

        await action.Should().NotThrowAsync("it should connect and declare its exchange and queues");
    }

    [Fact]
    public async Task When_Initializing_Twice()
    {
        var queue = queues.Create(queues.NewConfig("test-queue"));
        await queue.InitializeAsync(TestContext.Current.CancellationToken);

        var action = () => queue.InitializeAsync(TestContext.Current.CancellationToken);

        await action.Should().NotThrowAsync("it should treat a second initialization as a no-op");
    }

    [Fact]
    public async Task When_Publishing_Single_Message()
    {
        var queue = queues.Create(queues.NewConfig("test-queue"));
        var message = new TestMessage("test-id", "test-content");

        var action = () => queue.PublishAsync(message, TestContext.Current.CancellationToken);

        await action.Should().NotThrowAsync("it should connect on first use and publish the message");
    }

    [Fact]
    public async Task When_Subscribing_With_Handler()
    {
        var queue = queues.Create(queues.NewConfig("test-queue"));

        var action = () => queue.SubscribeAsync(_ => Task.FromResult(true), TestContext.Current.CancellationToken);

        await action.Should().NotThrowAsync("it should subscribe without error");
    }

    [Theory]
    [InlineData(true, "quorum")]
    [InlineData(false, "classic")]
    public async Task When_Choosing_The_Queue_Type(bool useQuorumQueues, string expectedType)
    {
        var config = queues.NewConfig("test-queue") with { UseQuorumQueues = useQuorumQueues };
        var queue = queues.Create(config);
        var retried = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await queue.SubscribeAsync((_, context, _) =>
        {
            if (context.DeliveryAttempt == 1)
                return Task.FromResult(ESE.MessageQueue.Abstractions.MessageResult.Retry);
            retried.TrySetResult();
            return Task.FromResult(ESE.MessageQueue.Abstractions.MessageResult.Acknowledge);
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(new TestMessage("1", "retried once"), TestContext.Current.CancellationToken);
        await retried.Task.WaitAsync(Wait.DeliveryTimeout, TestContext.Current.CancellationToken);

        (await broker.GetQueueTypeAsync(config.QueueName)).Should().Be(expectedType, "it should declare the main queue with the configured type");
        (await broker.GetQueueTypeAsync(config.EffectiveDeadLetterQueueName)).Should()
            .Be(expectedType, "it should declare the dead-letter queue with the same type");
        var retryQueues = await broker.ListQueuesAsync($"{config.QueueName}.retry.");
        retryQueues.Should().ContainSingle("it should declare one retry queue for the one backoff delay used");
        (await broker.GetQueueTypeAsync(retryQueues[0])).Should().Be(expectedType, "it should keep retries as durable as the main queue");
    }

    [Fact]
    public async Task When_Checking_Health_Through_A_Registered_Queue()
    {
        var config = queues.NewConfig("health-test");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRabbitMqProvider();
        services.AddMessageQueue<TestMessage>(new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MessageQueue:Provider"] = "RabbitMq",
            ["MessageQueue:ConnectionString"] = config.ConnectionString,
            ["MessageQueue:RequireTls"] = "false",
            ["MessageQueue:QueueName"] = config.QueueName,
            ["MessageQueue:UseQuorumQueues"] = "false"
        }).Build());
        services.AddHealthChecks().AddRabbitMqHealthCheck<TestMessage>();
        await using var provider = services.BuildServiceProvider();
        queues.Track(config);

        var report = await provider.GetRequiredService<HealthCheckService>().CheckHealthAsync(TestContext.Current.CancellationToken);

        report.Entries["rabbitmq"].Status.Should().Be(HealthStatus.Healthy, "it should connect the registered queue and report it healthy");
        report.Entries["rabbitmq"].Data.Should().BeEmpty("it should not publish broker details in the result");
        var publish = () => provider.GetRequiredService<IMessagePublisher<TestMessage>>()
            .PublishAsync(new TestMessage("1", "after probe"), TestContext.Current.CancellationToken);
        await publish.Should().NotThrowAsync("it should share the connection the probe opened with the queue");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_The_Consumer_Is_Cancelled_By_The_Broker()
    {
        var config = queues.NewConfig("consumer-cancelled-test");
        var queue = queues.Create(config);
        var check = new RabbitMqHealthCheck(queue);
        var context = new HealthCheckContext { Registration = new HealthCheckRegistration("rabbitmq", check, null, null) };
        await queue.SubscribeAsync(_ => Task.FromResult(true), TestContext.Current.CancellationToken);
        (await check.CheckHealthAsync(context, TestContext.Current.CancellationToken)).Status.Should()
            .Be(HealthStatus.Healthy, "it should report a running consumer as healthy");

        await broker.DeleteQueuesAndExchangesAsync([config.QueueName], []);
        await Task.Delay(500, TestContext.Current.CancellationToken);
        var result = await check.CheckHealthAsync(context, TestContext.Current.CancellationToken);

        result.Status.Should().Be(HealthStatus.Unhealthy, "it should notice that the broker cancelled the subscription's consumer");
    }
}
