using ESE.MessageQueue.Abstractions;

namespace ESE.MessageQueue.Providers;

/// <summary>
/// The dead-letter seam (ADR 0023): what a provider implements so that <see cref="DeadLetterQueue{T}"/> can peek,
/// replay and purge its dead letters. The provider supplies only the primitives (read in order, replay one as a first
/// delivery, remove one, return the rest); selection, limits, dry runs, counting, telemetry and logging are shared.
/// </summary>
public interface IDeadLetterStore
{
    /// <summary>The operations this store supports; <see cref="DeadLetterQueue{T}"/> refuses the others.</summary>
    DeadLetterOperations Operations { get; }

    /// <summary>
    /// Opens one pass over the dead letters. <see cref="DeadLetterQueue{T}"/> runs one pass at a time per store.
    /// </summary>
    /// <param name="browseOnly">
    /// True for a peek or dry run: the pass must remove and alter nothing (a broker may still reorder what it read), and
    /// its entries' <see cref="IDeadLetterEntry.ReplayAsync"/>
    /// and <see cref="IDeadLetterEntry.RemoveAsync"/> are never called (a provider may then browse without locking).
    /// </param>
    /// <param name="cancellationToken">Cancels opening the pass.</param>
    Task<IDeadLetterScan> OpenScanAsync(bool browseOnly, CancellationToken cancellationToken);
}

/// <summary>
/// One pass over a store's dead letters, in the store's order. It returns each dead letter at most once, holding (locking
/// or leaving unacknowledged) the ones it returned until they are replayed, removed or the pass is disposed, so that a
/// skipped one is not returned again. It keeps as little as it can of a held dead letter (a delivery tag or lock, not
/// the body). A pass whose connection is lost ends (<see cref="NextAsync"/> returns null), and its entries' replay and
/// removal throw. <see cref="IAsyncDisposable.DisposeAsync"/> returns every entry neither replayed
/// nor removed to the store unchanged; it must not throw for a broker failure (the broker returns them when the lock
/// or channel ends) and must not count as a failed delivery where the broker counts those.
/// </summary>
public interface IDeadLetterScan : IAsyncDisposable
{
    /// <summary>
    /// The next dead letter, or null when the pass has none left: the store is empty of unseen dead letters, or the pass
    /// returned as many as the store held when it opened (where the provider can tell).
    /// </summary>
    Task<IDeadLetterEntry?> NextAsync(CancellationToken cancellationToken);
}

/// <summary>One dead letter held by an <see cref="IDeadLetterScan"/>.</summary>
public interface IDeadLetterEntry
{
    /// <summary>What the broker recorded about the dead letter; the caller bounds and sanitizes the values.</summary>
    DeadLetterMetadata Metadata { get; }

    /// <summary>The serialized body, as dead-lettered.</summary>
    ReadOnlyMemory<byte> Body { get; }

    /// <summary>
    /// Sends the message back to its queue as a fresh first delivery (attempt 1: every library-owned attempt, reason and
    /// broker header removed; message id, correlation id, application headers and trace context kept), waits for the
    /// broker to confirm it, then removes the dead letter. If the send fails or is not confirmed, the dead letter stays
    /// (and is returned when the pass ends) and this throws.
    /// </summary>
    Task ReplayAsync(CancellationToken cancellationToken);

    /// <summary>Removes the dead letter permanently.</summary>
    Task RemoveAsync(CancellationToken cancellationToken);
}
