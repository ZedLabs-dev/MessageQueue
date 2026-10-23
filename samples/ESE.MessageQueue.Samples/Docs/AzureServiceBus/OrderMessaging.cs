#region snippet:azureservicebus-registration
using Azure.Core;
using Azure.Identity;
using ESE.MessageQueue.Extensions;
using ESE.MessageQueue.HealthChecks;
using ESE.MessageQueue.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
#endregion

namespace ESE.MessageQueue.Samples.Docs.AzureServiceBus;

#region snippet:azureservicebus-registration
public static class OrderMessaging
{
    public static IServiceCollection AddOrderMessaging(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddAzureServiceBusProvider();                                 // first: makes Provider = "AzureServiceBus" selectable
        services.AddMessageQueue<OrderPlaced>(configuration);                  // binds and validates "MessageQueue"
        services.AddMessageProcessor<OrderPlaced, OrderPlacedProcessor>();     // scoped: one per message
        services.AddHostedService<MessageQueueHostedService<OrderPlaced>>();   // consumes while the host runs
        services.AddSingleton<IOrderLedger, InMemoryOrderLedger>();            // your own services
        services.AddSingleton<OrderService>();
        services.AddSingleton<OrderDeadLetters>();                             // operator tool (dead-letter peek and replay)

        // Reuses the queue's client: Unhealthy while its processor is not running, else a peek (Listen rights only).
        services.AddHealthChecks().AddAzureServiceBusHealthCheck<OrderPlaced>();
        return services;
    }

    // Optional, with FullyQualifiedNamespace: pin the identity instead of DefaultAzureCredential's search order.
    public static IServiceCollection AddOrderMessagingIdentity(this IServiceCollection services, string managedIdentityClientId)
    {
        services.AddSingleton<TokenCredential>(
            new ManagedIdentityCredential(ManagedIdentityId.FromUserAssignedClientId(managedIdentityClientId)));
        return services;
    }
}
#endregion
