using AgentReview.MessageQueue.Abstractions;
using AgentReview.MessageQueue.AzureServiceBus;
using AgentReview.MessageQueue.Configuration;
using AgentReview.MessageQueue.Processing;
using AgentReview.MessageQueue.RabbitMQ;

namespace AgentReview.MessageQueue.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddMessageQueue<T>(
        this IServiceCollection services,
        IConfiguration configuration,
        string configurationSection = "MessageQueue") where T : class
    {
        var providerType = configuration.GetValue<MessageQueueProvider>($"{configurationSection}:Provider");

        return providerType switch
        {
            MessageQueueProvider.RabbitMq => services.AddSingleton<IMessageQueue<T>>(_ =>
                new RabbitMqMessageQueue<T>(CreateRabbitMqConfig(configuration, configurationSection))),
            MessageQueueProvider.AzureServiceBus => services.AddSingleton<IMessageQueue<T>>(_ =>
                new AzureServiceBusMessageQueue<T>(CreateAzureServiceBusConfig(configuration, configurationSection))),
            _ => throw new NotSupportedException($"Provider '{providerType}' is not supported")
        };
    }

    static RabbitMqConfig CreateRabbitMqConfig(IConfiguration configuration, string section)
    {
        var config = new RabbitMqConfig();
        configuration.GetSection(section).Bind(config);
        return config;
    }

    static AzureServiceBusConfig CreateAzureServiceBusConfig(IConfiguration configuration, string section)
    {
        var config = new AzureServiceBusConfig();
        configuration.GetSection(section).Bind(config);
        return config;
    }

    public static IServiceCollection AddMessageProcessor<T, TProcessor>(
        this IServiceCollection services) 
        where T : class 
        where TProcessor : class, IMessageProcessor<T>
    {
        return services.AddScoped<IMessageProcessor<T>, TProcessor>();
    }
}
