using AwesomeAssertions;

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

    public ValueTask DisposeAsync()
    {
        queues.Dispose();
        return ValueTask.CompletedTask;
    }

    [Fact]
    public void When_Config_Is_Valid()
    {
        var config = queues.NewConfig("test-queue");

        var action = () => queues.Create(config);

        action.Should().NotThrow("it should create instance with valid configuration");
    }

    [Fact]
    public async Task When_Publishing_Single_Message()
    {
        var queue = queues.Create(queues.NewConfig("test-queue"));
        var message = new TestMessage("test-id", "test-content");

        var action = () => queue.PublishAsync(message, TestContext.Current.CancellationToken);

        await action.Should().NotThrowAsync("it should publish message without error");
    }

    [Fact]
    public async Task When_Subscribing_With_Handler()
    {
        var queue = queues.Create(queues.NewConfig("test-queue"));

        var action = () => queue.SubscribeAsync(_ => Task.FromResult(true), TestContext.Current.CancellationToken);

        await action.Should().NotThrowAsync("it should subscribe without error");
    }

    [Fact]
    public async Task When_Using_Quorum_Queues()
    {
        var quorumConfig = queues.NewConfig("test-queue") with { UseQuorumQueues = true };

        var action = () => queues.Create(quorumConfig);

        action.Should().NotThrow("it should create instance with quorum queue configuration");
        (await broker.GetQueueTypeAsync(quorumConfig.QueueName)).Should().Be("quorum", "it should declare a quorum queue");
        (await broker.GetQueueTypeAsync($"{quorumConfig.QueueName}.deadletter")).Should()
            .Be("quorum", "it should declare the dead-letter queue as a quorum queue too");
    }

    [Fact]
    public async Task When_Using_Classic_Queues()
    {
        var classicConfig = queues.NewConfig("test-queue") with { UseQuorumQueues = false };

        var action = () => queues.Create(classicConfig);

        action.Should().NotThrow("it should create instance with classic queue configuration");
        (await broker.GetQueueTypeAsync(classicConfig.QueueName)).Should().Be("classic", "it should declare a classic queue");
        (await broker.GetQueueTypeAsync($"{classicConfig.QueueName}.deadletter")).Should()
            .Be("classic", "it should declare the dead-letter queue as a classic queue too");
    }
}
