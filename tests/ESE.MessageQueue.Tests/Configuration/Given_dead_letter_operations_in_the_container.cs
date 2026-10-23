using AwesomeAssertions;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Configuration;
using ESE.MessageQueue.Extensions;
using ESE.MessageQueue.InMemory;
using ESE.MessageQueue.Providers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace ESE.MessageQueue.Tests.Configuration;

// ADR 0023: IDeadLetterQueue<T> resolves next to IMessageQueue<T>, as the same queue, whenever the provider's queue
// implements it; a queue without dead-letter operations registers nothing, so GetService returns null.
[Trait("Category", "Unit")]
public sealed class Given_dead_letter_operations_in_the_container
{
    public sealed record PlainConfig : MessageQueueConfig
    {
        public override string Provider => "plain";
    }

    sealed class PlainValidator : MessageQueueConfigValidator<PlainConfig>
    {
        protected override void ValidateProvider(PlainConfig options, ICollection<string> errors)
        {
        }
    }

    sealed class PlainQueue : IMessageQueue<TestMessage>
    {
        public MessageQueueCapabilities Capabilities { get; } = new(Durable: false, DeadLetter: false, DelayedRedelivery: false);
        public Task PublishAsync(TestMessage message, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task PublishAsync(TestMessage message, PublishOptions options, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task PublishAsync(IEnumerable<TestMessage> messages, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task SubscribeAsync(Func<TestMessage, MessageContext, CancellationToken, Task<MessageResult>> handler, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task SubscribeAsync(Func<TestMessage, Task<bool>> handler, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task UnsubscribeAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
    }

    sealed class PlainProvider : IMessageQueueProvider
    {
        public string Name => "plain";

        public void AddQueue<T>(MessageQueueRegistration<T> registration) where T : class =>
            ((MessageQueueRegistration<TestMessage>)(object)registration).AddQueue<PlainConfig, PlainValidator, PlainQueue>(_ => new PlainQueue());
    }

    static IConfiguration Settings(string provider) => new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
    {
        ["MessageQueue:Provider"] = provider,
        ["MessageQueue:QueueName"] = "orders"
    }).Build();

    [Fact]
    public async Task When_The_Queue_Is_Registered_From_Configuration()
    {
        await using var provider = new ServiceCollection().AddMessageQueue<TestMessage>(Settings("InMemory")).BuildServiceProvider();

        provider.GetRequiredService<IDeadLetterQueue<TestMessage>>().Should().BeSameAs(provider.GetRequiredService<IMessageQueue<TestMessage>>(),
            "it should resolve the dead-letter operations as the registered queue");
    }

    [Fact]
    public async Task When_The_Queue_Is_Registered_In_Code()
    {
        await using var provider = new ServiceCollection().AddInMemoryMessageQueue<TestMessage>().BuildServiceProvider();

        provider.GetRequiredService<IDeadLetterQueue<TestMessage>>().Should().BeSameAs(provider.GetRequiredService<IMessageQueue<TestMessage>>(),
            "it should resolve the dead-letter operations for a code-first queue");
    }

    [Fact]
    public async Task When_The_Queue_Is_Registered_As_An_Instance()
    {
        await using var queue = new InMemoryMessageQueue<TestMessage>();
        await using var provider = new ServiceCollection().AddInMemoryMessageQueue(queue).BuildServiceProvider();

        provider.GetRequiredService<IDeadLetterQueue<TestMessage>>().Should().BeSameAs(queue,
            "it should resolve the dead-letter operations for a caller-owned queue");
    }

    [Fact]
    public async Task When_The_Queue_Has_No_Dead_Letter_Operations()
    {
        await using var provider = new ServiceCollection().AddMessageQueueProvider(new PlainProvider())
            .AddMessageQueue<TestMessage>(Settings("plain")).BuildServiceProvider();

        provider.GetRequiredService<IMessageQueue<TestMessage>>().Should().BeOfType<PlainQueue>("it should register the provider's queue");
        provider.GetService<IDeadLetterQueue<TestMessage>>().Should().BeNull(
            "it should not register dead-letter operations a queue does not implement");
    }
}
