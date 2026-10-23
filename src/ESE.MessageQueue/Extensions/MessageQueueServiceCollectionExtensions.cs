using System.Text.Json.Serialization.Metadata;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Configuration;
using ESE.MessageQueue.Exceptions;
using ESE.MessageQueue.InMemory;
using ESE.MessageQueue.Processing;
using ESE.MessageQueue.Providers;
using ESE.MessageQueue.Telemetry;
using ESE.MessageQueue.Telemetry.OpenTelemetry;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace ESE.MessageQueue.Extensions;

/// <summary>
/// Registers message queues, processors, providers and telemetry. All methods are for service registration at
/// startup and are not thread-safe, like <see cref="IServiceCollection"/> itself.
/// </summary>
public static class MessageQueueServiceCollectionExtensions
{
    /// <summary>
    /// Registers the queue for <typeparamref name="T"/> from <paramref name="configuration"/>: the section's
    /// <c>Provider</c> setting selects a registered provider by name (the in-memory provider, <c>InMemory</c>, is always
    /// registered; register others, such as <c>AddRabbitMqProvider()</c>, before this call). The section is bound
    /// strictly and validated when the host starts. One queue per message type.
    /// </summary>
    /// <exception cref="ConfigurationException">
    /// <c>Provider</c> is missing or names no registered provider (the message lists the registered ones), or a queue for
    /// <typeparamref name="T"/> is already registered.
    /// </exception>
    public static IServiceCollection AddMessageQueue<T>(
        this IServiceCollection services,
        IConfiguration configuration,
        string configurationSection = MessageQueueConfig.SectionName) where T : class
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        var section = configuration.GetSection(configurationSection);
        var provider = FindProvider(services, section["Provider"], configurationSection);
        QueueRegistration.ThrowIfRegistered<T>(services);

        var registration = new MessageQueueRegistration<T>(services, section);
        provider.AddQueue(registration);
        if (!registration.Registered)
            throw new ConfigurationException($"Provider '{provider.Name}' registered no queue for {typeof(T).FullName}");
        return services;
    }

    /// <summary>
    /// Supplies System.Text.Json metadata for the queue of <typeparamref name="T"/>, typically a source-generated
    /// <c>JsonSerializerContext</c> with <c>[JsonSerializable(typeof(T))]</c>, so that trimmed or native AOT applications
    /// serialize without reflection (ADR 0019). It is set as the queue's <c>JsonTypeInfoResolver</c> whichever provider
    /// the configuration selects, before or after <c>AddMessageQueue&lt;T&gt;</c>. The library's JSON rules still apply
    /// (camelCase names, a depth limit of 64, strict UTF-8). Startup validation fails when the resolver has no metadata
    /// for <typeparamref name="T"/> or the queue's <c>SerializationFormat</c> is not JSON.
    /// </summary>
    /// <exception cref="ConfigurationException">Metadata for <typeparamref name="T"/> is already registered, or the queue
    /// for <typeparamref name="T"/> is a caller-created instance (set its config's <c>JsonTypeInfoResolver</c> instead).</exception>
    public static IServiceCollection AddMessageQueueJsonMetadata<T>(this IServiceCollection services, IJsonTypeInfoResolver jsonTypeInfoResolver)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(jsonTypeInfoResolver);
        if (services.Any(d => d.ServiceType == typeof(QueueJsonMetadata<T>)))
            throw new ConfigurationException($"JSON metadata for {typeof(T).FullName} is already registered");
        if (services.Any(d => d.ServiceType == typeof(IMessageQueue<T>) && d.ImplementationInstance is not null))
            throw new ConfigurationException(InstanceWithMetadata<T>());

        services.AddSingleton(new QueueJsonMetadata<T>(jsonTypeInfoResolver));
        return services;
    }

    /// <summary>
    /// Makes <paramref name="provider"/> selectable by its <see cref="IMessageQueueProvider.Name"/> in later
    /// <c>AddMessageQueue&lt;T&gt;</c> calls. Registering the same provider type again is a no-op.
    /// </summary>
    /// <exception cref="ConfigurationException">
    /// Another provider type is already registered under that name, or the name is the built-in <c>InMemory</c>.
    /// </exception>
    public static IServiceCollection AddMessageQueueProvider(this IServiceCollection services, IMessageQueueProvider provider)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentException.ThrowIfNullOrWhiteSpace(provider.Name, nameof(provider));

        var existing = RegisteredProviders(services).Prepend(InMemoryProvider.Instance).FirstOrDefault(p => SameName(p.Name, provider.Name));
        if (existing is not null)
        {
            if (existing.GetType() == provider.GetType())
                return services;
            throw new ConfigurationException(
                $"A message queue provider named '{existing.Name}' is already registered ({existing.GetType().FullName})");
        }

        services.AddSingleton(provider);
        return services;
    }

    /// <summary>
    /// Registers an in-memory queue for <typeparamref name="T"/> without configuration: queue name
    /// <c>typeof(T).Name</c>, then <paramref name="configure"/>. Single-process and non-durable (ADR 0003).
    /// </summary>
    public static IServiceCollection AddInMemoryMessageQueue<T>(
        this IServiceCollection services,
        Action<InMemoryConfig>? configure = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(services);

        QueueRegistration.Add<T, InMemoryConfig, InMemoryConfigValidator, InMemoryMessageQueue<T>>(
            services,
            builder => builder.Configure(config =>
            {
                config.QueueName = typeof(T).Name;
                configure?.Invoke(config);
            }),
            InMemoryProvider.Create<T>);
        return services;
    }

    /// <summary>
    /// Registers a queue the caller created and owns: the container exposes it but does not dispose it.
    /// </summary>
    /// <exception cref="ConfigurationException">A queue or JSON metadata (<c>AddMessageQueueJsonMetadata&lt;T&gt;</c>, which
    /// cannot reach an instance) is already registered for <typeparamref name="T"/>.</exception>
    public static IServiceCollection AddInMemoryMessageQueue<T>(
        this IServiceCollection services,
        InMemoryMessageQueue<T> queue) where T : class
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(queue);
        QueueRegistration.ThrowIfRegistered<T>(services);
        if (services.Any(d => d.ServiceType == typeof(QueueJsonMetadata<T>)))
            throw new ConfigurationException(InstanceWithMetadata<T>());

        // Instance registrations: the caller created the queue, so the container must not dispose it.
        services.AddSingleton(queue);
        services.AddSingleton<IMessageQueue<T>>(queue);
        services.AddSingleton<IMessagePublisher<T>>(queue);
        services.AddSingleton<IMessageSubscriber<T>>(queue);
        return services;
    }

    /// <summary>
    /// Registers <typeparamref name="TProcessor"/> as the scoped <see cref="IMessageProcessor{T}"/> that
    /// <see cref="Hosting.MessageQueueHostedService{T}"/> resolves in a new scope per message.
    /// </summary>
    public static IServiceCollection AddMessageProcessor<T, TProcessor>(
        this IServiceCollection services)
        where T : class
        where TProcessor : class, IMessageProcessor<T>
    {
        return services.AddScoped<IMessageProcessor<T>, TProcessor>();
    }

    /// <summary>
    /// Registers <see cref="OpenTelemetryProvider"/>, which reports spans and metrics through an
    /// <c>ActivitySource</c> and <c>Meter</c> named <paramref name="sourceName"/>; the host's OpenTelemetry SDK (or any
    /// listener) subscribes to that name. Exception messages and stack traces are recorded on spans only when
    /// <paramref name="recordExceptionDetails"/> is true. The container disposes the provider.
    /// </summary>
    public static IServiceCollection AddMessageQueueTelemetry(
        this IServiceCollection services,
        string sourceName = "ESE.MessageQueue",
        bool recordExceptionDetails = false)
    {
        // A factory registration, so the container disposes the provider's ActivitySource and Meter.
        return services.AddSingleton<ITelemetryProvider>(_ => new OpenTelemetryProvider(sourceName, recordExceptionDetails));
    }

    /// <summary>Registers a custom <see cref="ITelemetryProvider"/> as a singleton.</summary>
    public static IServiceCollection AddMessageQueueTelemetry<TProvider>(
        this IServiceCollection services)
        where TProvider : class, ITelemetryProvider
    {
        return services.AddSingleton<ITelemetryProvider, TProvider>();
    }

    internal static string OptionsName<T>() => OptionsNames.For<T>();

    // A caller-created queue already has its config, so registered metadata could not reach it.
    static string InstanceWithMetadata<T>() =>
        $"The queue for {typeof(T).FullName} is a registered instance, which AddMessageQueueJsonMetadata<T> cannot reach: " +
        "set JsonTypeInfoResolver on the config the instance is created with instead";

    // The provider decides which config type and queue to register, so it is resolved here, at registration.
    // A missing or unregistered value fails: there is no default broker.
    static IMessageQueueProvider FindProvider(IServiceCollection services, string? name, string sectionName)
    {
        var providers = RegisteredProviders(services).Prepend(InMemoryProvider.Instance).ToList();
        var registered = string.Join(", ", providers.Select(p => p.Name));

        if (string.IsNullOrWhiteSpace(name))
            throw new ConfigurationException($"{sectionName}:Provider is required. Registered providers: {registered}");

        return providers.FirstOrDefault(p => SameName(p.Name, name))
            ?? throw new ConfigurationException(
                $"{sectionName}:Provider '{name}' is not registered. Registered providers: {registered}. " +
                "Add the provider's package and register it (for example services.AddRabbitMqProvider()) before AddMessageQueue<T>.");
    }

    // Providers are needed at registration, before a container exists, so only instances count. A provider registered
    // as a type or a factory would be silently invisible: that is an error.
    static IEnumerable<IMessageQueueProvider> RegisteredProviders(IServiceCollection services)
    {
        var descriptors = services.Where(d => d.ServiceType == typeof(IMessageQueueProvider)).ToList();
        if (descriptors.Find(d => d.IsKeyedService || d.ImplementationInstance is null) is { } invisible)
            throw new ConfigurationException(
                $"Message queue providers must be registered with services.AddMessageQueueProvider(instance); found " +
                $"{(invisible.IsKeyedService ? "a keyed" : "a type or factory")} registration of {nameof(IMessageQueueProvider)}" +
                (invisible.ImplementationType is { } type ? $" ({type.FullName})" : string.Empty));
        return descriptors.Select(d => (IMessageQueueProvider)d.ImplementationInstance!);
    }

    static bool SameName(string a, string b) => string.Equals(a.Trim(), b.Trim(), StringComparison.OrdinalIgnoreCase);
}
