using ESE.MessageQueue.AzureServiceBus;
using Microsoft.Extensions.DependencyInjection;

namespace ESE.MessageQueue.Extensions;

/// <summary>Registers the Azure Service Bus provider.</summary>
public static class AzureServiceBusServiceCollectionExtensions
{
    /// <summary>
    /// Makes <c>Provider = "AzureServiceBus"</c> selectable in <c>AddMessageQueue&lt;T&gt;</c>. Call it before
    /// <c>AddMessageQueue&lt;T&gt;</c>; calling it more than once is a no-op. A <c>TokenCredential</c> registered in the
    /// container is used with <c>FullyQualifiedNamespace</c>; without one, <c>DefaultAzureCredential</c>.
    /// </summary>
    public static IServiceCollection AddAzureServiceBusProvider(this IServiceCollection services) =>
        services.AddMessageQueueProvider(new AzureServiceBusProvider());
}
