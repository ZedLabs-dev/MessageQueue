#region snippet:inmemory-registration
using ESE.MessageQueue.Extensions;
using ESE.MessageQueue.HealthChecks;
using ESE.MessageQueue.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
#endregion

namespace ESE.MessageQueue.Samples.Docs.InMemory;

#region snippet:inmemory-registration
public static class OrderMessaging
{
    public static IServiceCollection AddOrderMessaging(this IServiceCollection services, IConfiguration configuration)
    {
        // InMemory is built in: no provider to register. The section's Provider setting selects it.
        services.AddMessageQueue<OrderPlaced>(configuration);                  // binds and validates "MessageQueue"
        services.AddMessageProcessor<OrderPlaced, OrderPlacedProcessor>();     // scoped: one per message
        services.AddHostedService<MessageQueueHostedService<OrderPlaced>>();   // consumes while the host runs
        services.AddSingleton<IOrderLedger, InMemoryOrderLedger>();            // your own services
        services.AddSingleton<OrderService>();
        services.AddSingleton<OrderDeadLetters>();                             // operator tool (dead-letter peek and replay)

        // The in-memory queue has no connection to probe; report the host as able to publish.
        services.AddHealthChecks().AddMessageQueueHealthCheck("orders", _ => Task.FromResult(true));
        return services;
    }
}
#endregion
