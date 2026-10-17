namespace ESE.MessageQueue.Abstractions;

/// <summary>
/// What a provider guarantees, so applications and configuration validation do not have to assume.
/// </summary>
/// <param name="Durable">Accepted messages survive a restart of this process.</param>
/// <param name="DeadLetter">Dead-lettered messages are kept somewhere they can be inspected or replayed.</param>
/// <param name="DelayedRedelivery">A retry waits for the backoff delay without holding the message or a consumer slot.</param>
public sealed record MessageQueueCapabilities(bool Durable, bool DeadLetter, bool DelayedRedelivery);
