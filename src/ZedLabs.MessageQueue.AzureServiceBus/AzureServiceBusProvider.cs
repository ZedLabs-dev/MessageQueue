using Azure.Core;
using Microsoft.Extensions.DependencyInjection;
using ZedLabs.MessageQueue.Providers;

namespace ZedLabs.MessageQueue.AzureServiceBus;

// Selectable as Provider = "AzureServiceBus" once AddAzureServiceBusProvider() has registered it.
sealed class AzureServiceBusProvider : IMessageQueueProvider
{
    public string Name => AzureServiceBusConfig.ProviderName;

    public void AddQueue<T>(MessageQueueRegistration<T> registration) where T : class =>
        registration.AddQueue<AzureServiceBusConfig, AzureServiceBusConfigValidator, AzureServiceBusMessageQueue<T>>(Create<T>);

    // A TokenCredential registered in the container is used with FullyQualifiedNamespace (and ignored with a
    // ConnectionString); without one the queue uses DefaultAzureCredential. (The transport factory is an internal test seam.)
    static AzureServiceBusMessageQueue<T> Create<T>(QueueFactoryContext<AzureServiceBusConfig> context) where T : class
    {
        var config = context.Config;
        var credential = string.IsNullOrWhiteSpace(config.FullyQualifiedNamespace) ? null : context.Services.GetService<TokenCredential>();
        return new(config, credential, context.Telemetry, context.Logger,
            context.Services.GetService<IServiceBusTransportFactory>() ?? ServiceBusTransportFactory.Instance);
    }
}
