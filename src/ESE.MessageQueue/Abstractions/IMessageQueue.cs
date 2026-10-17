namespace ESE.MessageQueue.Abstractions;

/// <summary>
/// A queue of <typeparamref name="T"/> messages: the provider-agnostic entry point applications code against.
/// </summary>
public interface IMessageQueue<T> : IMessagePublisher<T>, IMessageSubscriber<T> where T : class
{
    /// <summary>What this provider guarantees.</summary>
    MessageQueueCapabilities Capabilities { get; }
}
