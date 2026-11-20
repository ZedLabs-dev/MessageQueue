namespace ZedLabs.MessageQueue.Abstractions;

/// <summary>
/// Which dead letters <see cref="IDeadLetterQueue{T}.PurgeAsync"/> deletes: those whose reason code is selected
/// (<see cref="ReasonCodes"/>) and that <see cref="Filter"/>, if set, accepts. Deleting is permanent.
/// </summary>
/// <typeparam name="T">The message type.</typeparam>
public sealed class DeadLetterPurgeOptions<T> where T : class
{
    /// <summary>The most dead letters to delete (at least 1); there is no default, so a purge is always bounded explicitly.</summary>
    public required int MaxCount { get; init; }

    /// <summary>The reason codes to delete (matched exactly); null selects every reason. Must not be empty.</summary>
    public IReadOnlyCollection<string>? ReasonCodes { get; init; }

    /// <summary>An extra condition on the snapshot; null accepts every dead letter.</summary>
    public Func<DeadLetterMessage<T>, bool>? Filter { get; init; }
}
