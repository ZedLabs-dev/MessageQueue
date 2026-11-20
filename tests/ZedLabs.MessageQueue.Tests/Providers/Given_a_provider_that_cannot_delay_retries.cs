using AwesomeAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using ZedLabs.MessageQueue.Abstractions;
using ZedLabs.MessageQueue.Configuration;
using ZedLabs.MessageQueue.Exceptions;
using ZedLabs.MessageQueue.Extensions;
using ZedLabs.MessageQueue.Providers;

namespace ZedLabs.MessageQueue.Tests.Providers;

// ADR 0020: a queue refused by the RequireDelayedRedelivery guard was already created, and the container does not own
// it, so the guard disposes it.
[Trait("Category", "Unit")]
public class Given_a_provider_that_cannot_delay_retries
{
    public sealed record NoDelayConfig : MessageQueueConfig
    {
        public override string Provider => "nodelay";
    }

    sealed class NoDelayValidator : MessageQueueConfigValidator<NoDelayConfig>
    {
        protected override void ValidateProvider(NoDelayConfig options, ICollection<string> errors)
        {
        }
    }

    sealed class NoDelayQueue : IMessageQueue<TestMessage>, IAsyncDisposable
    {
        public bool Disposed { get; private set; }
        public MessageQueueCapabilities Capabilities { get; } = new(Durable: false, DeadLetter: false, DelayedRedelivery: false);
        public Task PublishAsync(TestMessage message, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task PublishAsync(TestMessage message, PublishOptions options, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task PublishAsync(IEnumerable<TestMessage> messages, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task SubscribeAsync(Func<TestMessage, MessageContext, CancellationToken, Task<MessageResult>> handler, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task SubscribeAsync(Func<TestMessage, Task<bool>> handler, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task UnsubscribeAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public ValueTask DisposeAsync()
        {
            Disposed = true;
            return ValueTask.CompletedTask;
        }
    }

    sealed class NoDelayProvider : IMessageQueueProvider
    {
        public List<NoDelayQueue> Created { get; } = [];
        public string Name => "nodelay";

        public void AddQueue<T>(MessageQueueRegistration<T> registration) where T : class =>
            ((MessageQueueRegistration<TestMessage>)(object)registration).AddQueue<NoDelayConfig, NoDelayValidator, NoDelayQueue>(_ =>
            {
                var queue = new NoDelayQueue();
                Created.Add(queue);
                return queue;
            });
    }

    [Fact]
    public async Task When_The_Guard_Refuses_The_Queue()
    {
        var provider = new NoDelayProvider();
        var services = new ServiceCollection().AddMessageQueueProvider(provider);
        services.AddMessageQueue<TestMessage>(new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MessageQueue:Provider"] = "nodelay",
            ["MessageQueue:QueueName"] = "orders",
            ["MessageQueue:RequireDelayedRedelivery"] = "true"
        }).Build());
        await using var container = services.BuildServiceProvider();

        var act = () => container.GetRequiredService<IMessageQueue<TestMessage>>();

        act.Should().Throw<ConfigurationException>("it should refuse a queue that cannot honor the required backoff");
        provider.Created.Should().ContainSingle("it should have created the queue once").Which.Disposed
            .Should().BeTrue("it should dispose the queue it refused, which the container does not own");
    }
}
