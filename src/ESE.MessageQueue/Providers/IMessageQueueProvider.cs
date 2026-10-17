namespace ESE.MessageQueue.Providers;

/// <summary>
/// A broker provider that <c>AddMessageQueue&lt;T&gt;(configuration)</c> can select by name. Register it with
/// <c>services.AddMessageQueueProvider(provider)</c> (first-party packages wrap that, e.g. <c>AddRabbitMqProvider()</c>)
/// before calling <c>AddMessageQueue&lt;T&gt;</c>, which reads the configured <c>Provider</c> name at registration.
/// </summary>
/// <remarks>
/// Implementations are stateless descriptions: the same instance serves every message type and is called only during
/// service registration, never concurrently with the queues it creates.
/// </remarks>
public interface IMessageQueueProvider
{
    /// <summary>
    /// The name the <c>Provider</c> configuration setting selects this provider by, matched case-insensitively after
    /// trimming. Names are unique among the registered providers.
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Registers the queue for <typeparamref name="T"/>: called once per <c>AddMessageQueue&lt;T&gt;</c> call whose
    /// <c>Provider</c> setting names this provider. Implementations call
    /// <see cref="MessageQueueRegistration{T}.AddQueue{TConfig, TValidator, TQueue}"/> exactly once.
    /// </summary>
    void AddQueue<T>(MessageQueueRegistration<T> registration) where T : class;
}
