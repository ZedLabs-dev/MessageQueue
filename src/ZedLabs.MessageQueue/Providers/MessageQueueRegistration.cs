using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using ZedLabs.MessageQueue.Abstractions;
using ZedLabs.MessageQueue.Configuration;
using ZedLabs.MessageQueue.Exceptions;
using ZedLabs.MessageQueue.Telemetry;

namespace ZedLabs.MessageQueue.Providers;

/// <summary>
/// What a provider gets when <c>AddMessageQueue&lt;T&gt;</c> selects it: the configuration section of the queue and the
/// service collection to register into. <see cref="AddQueue{TConfig, TValidator, TQueue}"/> does the registration the
/// same way for every provider. Valid only during the <see cref="IMessageQueueProvider.AddQueue{T}"/> call; not thread-safe.
/// </summary>
/// <typeparam name="T">The message type of the queue being registered.</typeparam>
public sealed class MessageQueueRegistration<T> where T : class
{
    internal MessageQueueRegistration(IServiceCollection services, IConfigurationSection section)
    {
        Services = services;
        Section = section;
    }

    /// <summary>The service collection the queue is registered into, for a provider's own extra services.</summary>
    public IServiceCollection Services { get; }

    /// <summary>The configuration section of this queue (the one holding <c>Provider</c>).</summary>
    public IConfigurationSection Section { get; }

    /// <summary>The name of the options instance for <typeparamref name="T"/>: resolve the queue's config with <c>IOptionsMonitor&lt;TConfig&gt;.Get(OptionsName)</c>.</summary>
    public string OptionsName { get; } = OptionsNames.For<T>();

    internal bool Registered { get; private set; }

    /// <summary>
    /// Registers the queue for <typeparamref name="T"/>. The section is bound to <typeparamref name="TConfig"/> strictly
    /// (unknown keys and enum values given as numbers fail at startup), validated with DataAnnotations, the base rules
    /// and <typeparamref name="TValidator"/> on start (<c>ValidateOnStart</c>), and the queue is registered as a
    /// singleton created by <paramref name="create"/> and exposed as <c>IMessageQueue&lt;T&gt;</c>,
    /// <c>IMessagePublisher&lt;T&gt;</c> and <c>IMessageSubscriber&lt;T&gt;</c>.
    /// </summary>
    /// <exception cref="InvalidOperationException">Called a second time for the same registration.</exception>
    public void AddQueue<TConfig, TValidator, TQueue>(Func<QueueFactoryContext<TConfig>, TQueue> create)
        where TConfig : MessageQueueConfig
        where TValidator : class, IValidateOptions<TConfig>
        where TQueue : class, IMessageQueue<T>
    {
        ArgumentNullException.ThrowIfNull(create);
        if (Registered)
            throw new InvalidOperationException($"The queue for {typeof(T).FullName} is already registered by this provider");
        Registered = true;

        QueueRegistration.Add<T, TConfig, TValidator, TQueue>(Services, builder => QueueRegistration.BindStrictly(builder, Section), create);
        Services.AddSingleton(new QueueSection<T>(Section.Path));
    }
}

/// <summary>What a provider's queue factory gets when the queue for a message type is first resolved.</summary>
/// <typeparam name="TConfig">The provider's config type, bound and validated.</typeparam>
public sealed class QueueFactoryContext<TConfig> where TConfig : MessageQueueConfig
{
    internal QueueFactoryContext(IServiceProvider services, TConfig config, ITelemetryProvider telemetry, ILogger logger)
    {
        Services = services;
        Config = config;
        Telemetry = telemetry;
        Logger = logger;
    }

    /// <summary>The application's service provider, for optional services such as a credential.</summary>
    public IServiceProvider Services { get; }

    /// <summary>The queue's validated configuration.</summary>
    public TConfig Config { get; }

    /// <summary>The registered telemetry provider, or <see cref="NullTelemetryProvider.Instance"/> when none is registered.</summary>
    public ITelemetryProvider Telemetry { get; }

    /// <summary>A logger for the queue type, or a null logger when logging is not registered.</summary>
    public ILogger Logger { get; }
}

// The registration every queue goes through, whichever provider and entry point registered it.
static class QueueRegistration
{
    public static void Add<T, TConfig, TValidator, TQueue>(
        IServiceCollection services,
        Action<OptionsBuilder<TConfig>> configure,
        Func<QueueFactoryContext<TConfig>, TQueue> create)
        where T : class
        where TConfig : MessageQueueConfig
        where TValidator : class, IValidateOptions<TConfig>
        where TQueue : class, IMessageQueue<T>
    {
        ThrowIfRegistered<T>(services);

        var name = OptionsNames.For<T>();
        var builder = services.AddOptions<TConfig>(name);
        configure(builder);
        // JSON metadata cannot come from configuration: AddMessageQueueJsonMetadata<T> supplies it in code (ADR 0019).
        builder.PostConfigure<IEnumerable<QueueJsonMetadata<T>>>((config, metadata) =>
        {
            if (metadata.LastOrDefault() is { } json)
                config.JsonTypeInfoResolver = json.Resolver;
        });
        builder.Validate(
            config => config.SerializationFormat != Serialization.SerializationFormat.Json
                || config.JsonTypeInfoResolver is null
                || MessageSerializer.HasJsonMetadata(config.JsonTypeInfoResolver, typeof(T)),
            $"JsonTypeInfoResolver has no JSON metadata for {typeof(T).FullName}: add [JsonSerializable(typeof({typeof(T).Name}))] to its JsonSerializerContext");
        builder.ValidateDataAnnotations().ValidateOnStart();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<TConfig>, TValidator>());

        services.AddSingleton(sp =>
        {
            var config = sp.GetRequiredService<IOptionsMonitor<TConfig>>().Get(name);
            var queue = create(new QueueFactoryContext<TConfig>(
                sp,
                config,
                sp.GetService<ITelemetryProvider>() ?? NullTelemetryProvider.Instance,
                sp.GetService<ILoggerFactory>()?.CreateLogger<TQueue>() ?? (ILogger)NullLogger.Instance));
            // Every provider, third-party ones included, is held to RequireDelayedRedelivery through its Capabilities
            // (ADR 0020); first-party providers already fail ValidateOnStart for it.
            if (Processing.CapabilityChecks.UnsupportedRequiredBackoff(queue.Capabilities.DelayedRedelivery, config) is { } unsupported)
            {
                Dispose(queue);
                throw new ConfigurationException($"Queue '{config.QueueName}' for {typeof(T).FullName}: {unsupported}");
            }
            return queue;
        });

        services.AddSingleton<IMessageQueue<T>>(sp => sp.GetRequiredService<TQueue>());
        services.AddSingleton<IMessagePublisher<T>>(sp => sp.GetRequiredService<IMessageQueue<T>>());
        services.AddSingleton<IMessageSubscriber<T>>(sp => sp.GetRequiredService<IMessageQueue<T>>());
        // Dead-letter operations (ADR 0023) resolve as the same queue, when it has them.
        if (typeof(IDeadLetterQueue<T>).IsAssignableFrom(typeof(TQueue)))
            services.AddSingleton(sp => (IDeadLetterQueue<T>)sp.GetRequiredService<TQueue>());
    }

    static void Dispose(object queue)
    {
        if (queue is IAsyncDisposable asyncDisposable)
            asyncDisposable.DisposeAsync().AsTask().GetAwaiter().GetResult();
        else if (queue is IDisposable disposable)
            disposable.Dispose();
    }

    // Unknown keys (typos, or settings removed such as RetryAttempts) fail at startup instead of being ignored,
    // and enum settings must be spelled as names.
    public static void BindStrictly<TConfig>(OptionsBuilder<TConfig> builder, IConfigurationSection section) where TConfig : MessageQueueConfig
    {
        builder.Bind(section, options => options.ErrorOnUnknownConfiguration = true);
        builder.Services.AddSingleton<IValidateOptions<TConfig>>(new EnumNameValidator<TConfig>(builder.Name, section));
    }

    // One queue per message type: a second registration would silently replace the first.
    public static void ThrowIfRegistered<T>(IServiceCollection services) where T : class
    {
        if (services.Any(d => d.ServiceType == typeof(IMessageQueue<T>)))
            throw new ConfigurationException($"A message queue for {typeof(T).FullName} is already registered");
    }
}

// The configuration section the queue of T was bound from, for advice that names its keys (warning 5005).
sealed class QueueSection<T>(string path) where T : class
{
    public string Path { get; } = path;
}

// The JSON metadata AddMessageQueueJsonMetadata<T> registered for the queue of T.
sealed class QueueJsonMetadata<T> where T : class
{
    public QueueJsonMetadata(System.Text.Json.Serialization.Metadata.IJsonTypeInfoResolver resolver)
    {
        Resolver = resolver;
    }

    public System.Text.Json.Serialization.Metadata.IJsonTypeInfoResolver Resolver { get; }
}

static class OptionsNames
{
    // Options are named per message type, so queues for different T can use different sections and providers.
    public static string For<T>() => typeof(T).FullName ?? typeof(T).Name;
}
