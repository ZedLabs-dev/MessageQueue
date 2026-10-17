using ESE.MessageQueue.Configuration;
using ESE.MessageQueue.Providers;

namespace ESE.MessageQueue.InMemory;

// The built-in provider: always selectable as "InMemory", no registration needed.
sealed class InMemoryProvider : IMessageQueueProvider
{
    public static readonly InMemoryProvider Instance = new();

    public string Name => InMemoryConfig.ProviderName;

    public void AddQueue<T>(MessageQueueRegistration<T> registration) where T : class =>
        registration.AddQueue<InMemoryConfig, InMemoryConfigValidator, InMemoryMessageQueue<T>>(Create<T>);

    public static InMemoryMessageQueue<T> Create<T>(QueueFactoryContext<InMemoryConfig> context) where T : class =>
        new(context.Config, context.Telemetry, context.Logger);
}
