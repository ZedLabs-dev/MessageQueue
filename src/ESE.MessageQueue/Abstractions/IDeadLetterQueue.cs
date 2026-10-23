namespace ESE.MessageQueue.Abstractions;

/// <summary>
/// Operations on the dead letters of a queue of <typeparamref name="T"/> (ADR 0023): inspect them, move them back to
/// the queue for reprocessing, or delete them. The provider's queue implements it; with dependency injection, resolve
/// <c>IDeadLetterQueue&lt;T&gt;</c> next to <c>IMessageQueue&lt;T&gt;</c>. Which operations a queue supports is in
/// <see cref="MessageQueueCapabilities.DeadLetterOperations"/>; an unsupported one throws <see cref="NotSupportedException"/>.
/// </summary>
/// <remarks>
/// Safe while the queue's subscriber runs. One operation runs at a time per queue; others wait. Each call examines the
/// dead letters in the store's order, at most <see cref="DeadLetterLimits.MaxScanCount"/> of them, holding (locking or
/// leaving unacknowledged) the ones it skips until it ends, so it never sees one twice; a replay also replays each message
/// id at most once, so a replayed message that fails and is dead-lettered again during the call is not replayed again.
/// A message without an id is keyed by its body, correlation id and headers instead. Delivery is at-least-once: a
/// replayed message is published (and confirmed) before it is removed from the dead letters, so a failure or
/// cancellation in between leaves a duplicate, never a loss. A failed pass (a lost connection, a failing broker call)
/// ends the call: what was done is counted and logged, and it throws <see cref="Exceptions.MessageQueueException"/>
/// naming only the failure's type; a throwing filter ends it the same way with the filter's own exception.
/// </remarks>
/// <typeparam name="T">The message type.</typeparam>
public interface IDeadLetterQueue<T> where T : class
{
    /// <summary>
    /// Reads up to <paramref name="maxCount"/> dead letters without removing them or changing their content. A provider
    /// may change their order: on a RabbitMQ quorum queue the dead letters a peek (or a dry run) read go back to the end
    /// of the queue.
    /// </summary>
    /// <param name="maxCount">1 to <see cref="DeadLetterLimits.MaxPeekCount"/>.</param>
    /// <param name="cancellationToken">Stops the read; nothing is changed either way.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxCount"/> is out of range.</exception>
    /// <exception cref="NotSupportedException">The queue does not support <see cref="DeadLetterOperations.Peek"/>.</exception>
    Task<IReadOnlyList<DeadLetterMessage<T>>> PeekAsync(int maxCount, CancellationToken cancellationToken = default);

    /// <summary>
    /// Moves the dead letters that <paramref name="options"/> select back to the queue, each as a fresh first delivery
    /// with the full retry budget, keeping its message id, correlation id, headers and trace context.
    /// </summary>
    /// <param name="options">Which dead letters, how many, and whether to only count them (<see cref="DeadLetterReplayOptions{T}.DryRun"/>).</param>
    /// <param name="cancellationToken">
    /// Stops after the current message: each message is then either replayed and removed or still a dead letter
    /// (possibly also replayed, so a duplicate), never lost. The call throws <see cref="OperationCanceledException"/>.
    /// </param>
    /// <exception cref="ArgumentException">The options are invalid.</exception>
    /// <exception cref="NotSupportedException">The queue does not support <see cref="DeadLetterOperations.Replay"/>.</exception>
    Task<DeadLetterReplayResult> ReplayAsync(DeadLetterReplayOptions<T> options, CancellationToken cancellationToken = default);

    /// <summary>Deletes the dead letters that <paramref name="options"/> select and returns how many were deleted.</summary>
    /// <param name="options">Which dead letters and how many; there is no parameterless purge-all.</param>
    /// <param name="cancellationToken">Stops after the current message; the ones already deleted stay deleted.</param>
    /// <exception cref="ArgumentException">The options are invalid.</exception>
    /// <exception cref="NotSupportedException">The queue does not support <see cref="DeadLetterOperations.Purge"/>.</exception>
    Task<int> PurgeAsync(DeadLetterPurgeOptions<T> options, CancellationToken cancellationToken = default);
}
