using Azure.Core;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.AzureServiceBus;
using ESE.MessageQueue.Configuration;
using ESE.MessageQueue.Exceptions;
using ESE.MessageQueue.InMemory;
using ESE.MessageQueue.Processing;
using ESE.MessageQueue.RabbitMQ;
using ESE.MessageQueue.Telemetry;
using ESE.MessageQueue.Telemetry.OpenTelemetry;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace ESE.MessageQueue.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddMessageQueue<T>(
        this IServiceCollection services,
        IConfiguration configuration,
        string configurationSection = MessageQueueConfig.SectionName) where T : class
    {
        var section = configuration.GetSection(configurationSection);
        var provider = ReadProvider(section, configurationSection);

        return provider switch
        {
            MessageQueueProvider.RabbitMq => AddQueue<T, RabbitMqConfig, RabbitMqConfigValidator, RabbitMqMessageQueue<T>>(
                services, builder => BindStrictly(builder, section), (_, config, telemetry, logger) => new RabbitMqMessageQueue<T>(config, telemetry, logger)),
            MessageQueueProvider.AzureServiceBus => AddQueue<T, AzureServiceBusConfig, AzureServiceBusConfigValidator, AzureServiceBusMessageQueue<T>>(
                services, builder => BindStrictly(builder, section), CreateAzureServiceBusQueue<T>),
            MessageQueueProvider.InMemory => AddQueue<T, InMemoryConfig, InMemoryConfigValidator, InMemoryMessageQueue<T>>(
                services, builder => BindStrictly(builder, section), (_, config, telemetry, logger) => new InMemoryMessageQueue<T>(config, telemetry, logger)),
            _ => throw new ConfigurationException($"Provider '{provider}' is not supported")
        };
    }

    // A TokenCredential registered in the container is used with FullyQualifiedNamespace (and ignored with a
    // ConnectionString); without one the queue uses DefaultAzureCredential. (The transport factory is an internal test seam.)
    static AzureServiceBusMessageQueue<T> CreateAzureServiceBusQueue<T>(
        IServiceProvider services, AzureServiceBusConfig config, ITelemetryProvider telemetry, ILogger logger) where T : class =>
        new(config, string.IsNullOrWhiteSpace(config.FullyQualifiedNamespace) ? null : services.GetService<TokenCredential>(), telemetry, logger,
            services.GetService<IServiceBusTransportFactory>() ?? ServiceBusTransportFactory.Instance);

    // Unknown keys (typos, or settings removed such as RetryAttempts) fail at startup instead of being ignored,
    // and enum settings must be spelled as names.
    static void BindStrictly<TConfig>(OptionsBuilder<TConfig> builder, IConfigurationSection section) where TConfig : MessageQueueConfig
    {
        builder.Bind(section, options => options.ErrorOnUnknownConfiguration = true);
        builder.Services.AddSingleton<IValidateOptions<TConfig>>(new EnumNameValidator<TConfig>(builder.Name, section));
    }

    // The provider decides which config type and queue to register, so it is checked here, at registration.
    // A missing or unknown value fails: there is no default broker.
    static MessageQueueProvider ReadProvider(IConfigurationSection section, string sectionName)
    {
        var value = section["Provider"];
        var supported = string.Join(", ", Enum.GetNames<MessageQueueProvider>());

        if (string.IsNullOrWhiteSpace(value))
            throw new ConfigurationException($"{sectionName}:Provider is required. Supported providers: {supported}");

        if (!ConfigValidation.IsEnumName<MessageQueueProvider>(value))
            throw new ConfigurationException($"{sectionName}:Provider '{value}' is not supported. Supported providers: {supported}");

        return Enum.Parse<MessageQueueProvider>(value.Trim(), ignoreCase: true);
    }

    public static IServiceCollection AddInMemoryMessageQueue<T>(
        this IServiceCollection services,
        Action<InMemoryConfig>? configure = null) where T : class
    {
        return AddQueue<T, InMemoryConfig, InMemoryConfigValidator, InMemoryMessageQueue<T>>(
            services,
            builder => builder.Configure(config =>
            {
                config.QueueName = typeof(T).Name;
                configure?.Invoke(config);
            }),
            (_, config, telemetry, logger) => new InMemoryMessageQueue<T>(config, telemetry, logger));
    }

    public static IServiceCollection AddInMemoryMessageQueue<T>(
        this IServiceCollection services,
        InMemoryMessageQueue<T> queue) where T : class
    {
        ThrowIfRegistered<T>(services);

        // Instance registrations: the caller created the queue, so the container must not dispose it.
        services.AddSingleton(queue);
        services.AddSingleton<IMessageQueue<T>>(queue);
        services.AddSingleton<IMessagePublisher<T>>(queue);
        services.AddSingleton<IMessageSubscriber<T>>(queue);
        return services;
    }

    public static IServiceCollection AddMessageProcessor<T, TProcessor>(
        this IServiceCollection services)
        where T : class
        where TProcessor : class, IMessageProcessor<T>
    {
        return services.AddScoped<IMessageProcessor<T>, TProcessor>();
    }

    public static IServiceCollection AddMessageQueueTelemetry(
        this IServiceCollection services,
        string sourceName = "ESE.MessageQueue",
        bool recordExceptionDetails = false)
    {
        // A factory registration, so the container disposes the provider's ActivitySource and Meter.
        return services.AddSingleton<ITelemetryProvider>(_ => new OpenTelemetryProvider(sourceName, recordExceptionDetails));
    }

    public static IServiceCollection AddMessageQueueTelemetry<TProvider>(
        this IServiceCollection services)
        where TProvider : class, ITelemetryProvider
    {
        return services.AddSingleton<ITelemetryProvider, TProvider>();
    }

    // Options are named per message type, so queues for different T can use different sections and providers.
    internal static string OptionsName<T>() => typeof(T).FullName ?? typeof(T).Name;

    static IServiceCollection AddQueue<T, TConfig, TValidator, TQueue>(
        IServiceCollection services,
        Action<OptionsBuilder<TConfig>> configure,
        Func<IServiceProvider, TConfig, ITelemetryProvider, ILogger, TQueue> create)
        where T : class
        where TConfig : MessageQueueConfig
        where TValidator : class, IValidateOptions<TConfig>
        where TQueue : class, IMessageQueue<T>
    {
        ThrowIfRegistered<T>(services);

        var name = OptionsName<T>();
        var builder = services.AddOptions<TConfig>(name);
        configure(builder);
        builder.ValidateDataAnnotations().ValidateOnStart();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<TConfig>, TValidator>());

        services.AddSingleton(sp => create(
            sp,
            sp.GetRequiredService<IOptionsMonitor<TConfig>>().Get(name),
            sp.GetService<ITelemetryProvider>() ?? NullTelemetryProvider.Instance,
            sp.GetService<ILoggerFactory>()?.CreateLogger<TQueue>() ?? (ILogger)NullLogger.Instance));

        return AddQueueAliases<T, TQueue>(services);
    }

    // One queue per message type: a second registration would silently replace the first.
    static void ThrowIfRegistered<T>(IServiceCollection services) where T : class
    {
        if (services.Any(d => d.ServiceType == typeof(IMessageQueue<T>)))
            throw new ConfigurationException($"A message queue for {typeof(T).FullName} is already registered");
    }

    static IServiceCollection AddQueueAliases<T, TQueue>(IServiceCollection services)
        where T : class
        where TQueue : class, IMessageQueue<T>
    {
        services.AddSingleton<IMessageQueue<T>>(sp => sp.GetRequiredService<TQueue>());
        services.AddSingleton<IMessagePublisher<T>>(sp => sp.GetRequiredService<IMessageQueue<T>>());
        services.AddSingleton<IMessageSubscriber<T>>(sp => sp.GetRequiredService<IMessageQueue<T>>());
        return services;
    }
}
