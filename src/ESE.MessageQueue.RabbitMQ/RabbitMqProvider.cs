using ESE.MessageQueue.Providers;

namespace ESE.MessageQueue.RabbitMQ;

// Selectable as Provider = "RabbitMq" once AddRabbitMqProvider() has registered it.
sealed class RabbitMqProvider : IMessageQueueProvider
{
    public string Name => RabbitMqConfig.ProviderName;

    public void AddQueue<T>(MessageQueueRegistration<T> registration) where T : class =>
        registration.AddQueue<RabbitMqConfig, RabbitMqConfigValidator, RabbitMqMessageQueue<T>>(
            context => new RabbitMqMessageQueue<T>(context.Config, context.Telemetry, context.Logger));
}
