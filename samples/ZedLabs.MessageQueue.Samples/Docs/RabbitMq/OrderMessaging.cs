#region snippet:rabbitmq-registration
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using ZedLabs.MessageQueue.Extensions;
using ZedLabs.MessageQueue.HealthChecks;
using ZedLabs.MessageQueue.Hosting;
#endregion

namespace ZedLabs.MessageQueue.Samples.Docs.RabbitMq;

#region snippet:rabbitmq-registration
public static class OrderMessaging
{
    public static IServiceCollection AddOrderMessaging(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddRabbitMqProvider();                                        // first: makes Provider = "RabbitMq" selectable
        services.AddMessageQueue<OrderPlaced>(configuration);                  // binds and validates "MessageQueue"
        services.AddMessageProcessor<OrderPlaced, OrderPlacedProcessor>();     // scoped: one per message
        services.AddHostedService<MessageQueueHostedService<OrderPlaced>>();   // consumes while the host runs
        services.AddSingleton<IOrderLedger, InMemoryOrderLedger>();            // your own services
        services.AddSingleton<OrderService>();
        services.AddSingleton<OrderDeadLetters>();                             // operator tool (dead-letter peek and replay)

        // Reuses the queue's connection: Unhealthy when it is closed or the consumer was cancelled by the broker.
        services.AddHealthChecks().AddRabbitMqHealthCheck<OrderPlaced>();
        return services;
    }
}
#endregion
