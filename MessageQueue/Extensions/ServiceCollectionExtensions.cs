using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.AzureServiceBus;
using ESE.MessageQueue.Configuration;
using ESE.MessageQueue.Exceptions;
using ESE.MessageQueue.Processing;
using ESE.MessageQueue.RabbitMQ;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace ESE.MessageQueue.Extensions;

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
            MessageQueueProvider.RabbitMq => AddRabbitMqQueue<T>(services, configuration, configurationSection),
            MessageQueueProvider.AzureServiceBus => AddAzureServiceBusQueue<T>(services, configuration, configurationSection),
            _ => throw new NotSupportedException($"Provider '{providerType}' is not supported")
        };
    }

    static IServiceCollection AddRabbitMqQueue<T>(
        IServiceCollection services,
        IConfiguration configuration,
        string section) where T : class
    {
        var config = new RabbitMqConfig();
        configuration.GetSection(section).Bind(config);
        ValidateBaseConfig(config);
        ValidateRabbitMqConfig(config);
        return services.AddSingleton<IMessageQueue<T>>(_ => new RabbitMqMessageQueue<T>(config));
    }

    static IServiceCollection AddAzureServiceBusQueue<T>(
        IServiceCollection services,
        IConfiguration configuration,
        string section) where T : class
    {
        var config = new AzureServiceBusConfig();
        configuration.GetSection(section).Bind(config);
        ValidateBaseConfig(config);
        ValidateAzureServiceBusConfig(config);
        return services.AddSingleton<IMessageQueue<T>>(_ => new AzureServiceBusMessageQueue<T>(config));
    }

    static void ValidateBaseConfig(MessageQueueConfig config)
    {
        if (string.IsNullOrWhiteSpace(config.ConnectionString))
            throw new ConfigurationException("ConnectionString is required");

        if (string.IsNullOrWhiteSpace(config.QueueName))
            throw new ConfigurationException("QueueName is required");

        if (config.RetryAttempts < 0)
            throw new ConfigurationException("RetryAttempts must be >= 0");
    }

    static void ValidateRabbitMqConfig(RabbitMqConfig config)
    {
        if (config.PrefetchCount < 1)
            throw new ConfigurationException("PrefetchCount must be >= 1");
    }

    static void ValidateAzureServiceBusConfig(AzureServiceBusConfig config)
    {
        if (config.MaxConcurrentCalls < 1)
            throw new ConfigurationException("MaxConcurrentCalls must be >= 1");

        if (config.MaxBatchSize < 1)
            throw new ConfigurationException("MaxBatchSize must be >= 1");
    }

    public static IServiceCollection AddMessageProcessor<T, TProcessor>(
        this IServiceCollection services)
        where T : class
        where TProcessor : class, IMessageProcessor<T>
    {
        return services.AddScoped<IMessageProcessor<T>, TProcessor>();
    }
}
