using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using ZedLabs.MessageQueue.Abstractions;
using ZedLabs.MessageQueue.Configuration;
using ZedLabs.MessageQueue.Processing;
using ZedLabs.MessageQueue.Telemetry;

namespace ZedLabs.MessageQueue.Providers;

/// <summary>
/// The shared implementation of <see cref="IDeadLetterQueue{T}"/> over a provider's <see cref="IDeadLetterStore"/>
/// (ADR 0023): argument checks, selection (reason codes, the default exclusions, the filter), limits, dry runs,
/// snapshots, counting, telemetry and logging. A provider's queue creates one and forwards its
/// <see cref="IDeadLetterQueue{T}"/> calls to it.
/// </summary>
/// <remarks>Thread-safe: operations run one at a time, in call order as far as the waits allow.</remarks>
/// <typeparam name="T">The message type.</typeparam>
#pragma warning disable CA1001 // The SemaphoreSlim never creates its wait handle (only WaitAsync is used), so there is nothing to dispose.
public sealed class DeadLetterQueue<T> : IDeadLetterQueue<T> where T : class
{
    // Replaying these would only dead-letter them again; a caller names them explicitly to replay them anyway.
    static readonly string[] NotReplayedByDefault = [DeadLetterReasons.MessageTooLarge, DeadLetterReasons.DeserializationFailed];

    readonly IDeadLetterStore store;
    readonly MessageQueueConfig config;
    readonly TelemetryTarget target;
    readonly ITelemetryProvider telemetry;
    readonly ILogger logger;
    readonly SemaphoreSlim gate = new(1, 1);

    /// <summary>Creates the dead-letter operations of one queue.</summary>
    /// <param name="store">The provider's dead letters.</param>
    /// <param name="config">The queue's config: serialization format, JSON metadata and size limit for snapshots.</param>
    /// <param name="target">What metrics are about (the queue's consume target).</param>
    /// <param name="telemetry">The telemetry provider; none when null.</param>
    /// <param name="logger">The provider's logger; a throwing logger is ignored.</param>
    public DeadLetterQueue(IDeadLetterStore store, MessageQueueConfig config, TelemetryTarget target, ITelemetryProvider? telemetry = null, ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(target);
        this.store = store;
        // A snapshot, like the dispatcher's: later changes to the config object do not change running operations.
        this.config = config with { };
        this.target = target;
        this.telemetry = telemetry ?? NullTelemetryProvider.Instance;
        this.logger = new SafeLogger(logger ?? NullLogger.Instance);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<DeadLetterMessage<T>>> PeekAsync(int maxCount, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxCount, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(maxCount, DeadLetterLimits.MaxPeekCount);
        Require(DeadLetterOperations.Peek);

        await gate.WaitAsync(cancellationToken);
        var letters = new List<DeadLetterMessage<T>>();
        IDeadLetterScan? scan = null;
        try
        {
            scan = await store.OpenScanAsync(browseOnly: true, cancellationToken);
            while (letters.Count < maxCount && await scan.NextAsync(cancellationToken) is { } entry)
                letters.Add(Snapshot(entry));
            return letters;
        }
        catch (Exception ex) when (!IsCallerCancellation(ex, cancellationToken))
        {
            throw Aborted("peek", letters.Count, ex);
        }
        finally
        {
            await ReturnHeldAsync(scan);
            gate.Release();
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// A pass that fails (the store or the broker connection fails) or a <see cref="DeadLetterReplayOptions{T}.Filter"/>
    /// that throws ends the call: what was replayed is counted and logged, the rest stay dead letters, and the call
    /// throws <see cref="Exceptions.MessageQueueException"/> (naming only the failure's type) or, for the filter, the
    /// filter's own exception unchanged.
    /// </remarks>
    public async Task<DeadLetterReplayResult> ReplayAsync(DeadLetterReplayOptions<T> options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentOutOfRangeException.ThrowIfLessThan(options.MaxCount, 1, nameof(options.MaxCount));
        ValidateReasonCodes(options.ReasonCodes);
        Require(DeadLetterOperations.Replay);

        await gate.WaitAsync(cancellationToken);
        try
        {
            return await ReplayCoreAsync(options, cancellationToken);
        }
        finally
        {
            gate.Release();
        }
    }

    async Task<DeadLetterReplayResult> ReplayCoreAsync(DeadLetterReplayOptions<T> options, CancellationToken cancellationToken)
    {
        int examined = 0, matched = 0, replayed = 0, skipped = 0, failed = 0;
        var failures = new List<DeadLetterReplayFailure>();
        // A replayed message that fails again is dead-lettered again, possibly while this pass still runs: its key (the
        // message id, or for a message without one its content) marks it, so it is not replayed twice by one call.
        var replayedKeys = new HashSet<string>(StringComparer.Ordinal);

        IDeadLetterScan? scan = null;
        try
        {
            scan = await store.OpenScanAsync(browseOnly: options.DryRun, cancellationToken);
            while (matched < options.MaxCount && examined < DeadLetterLimits.MaxScanCount)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (await scan.NextAsync(cancellationToken) is not { } entry)
                    break;
                examined++;

                var letter = Snapshot(entry);
                var key = ReplayKey(letter, entry.Body);
                if (!Selected(letter, options.ReasonCodes, NotReplayedByDefault, options.Filter) || replayedKeys.Contains(key))
                {
                    skipped++;
                    continue;
                }

                matched++;
                replayedKeys.Add(key);
                if (options.DryRun)
                    continue;

                try
                {
                    await entry.ReplayAsync(cancellationToken);
                    replayed++;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    // One message failed (a store's own timeout included); the rest go on.
                    failed++;
                    var error = BoundedText.TypeName(ex);
                    if (failures.Count < DeadLetterLimits.MaxReportedFailures)
                        failures.Add(new DeadLetterReplayFailure(letter.MessageId, error));
                    DeadLetterLog.ReplayFailed(logger, config.QueueName, letter.MessageId, error, ex);
                }
            }
        }
        catch (Exception ex) when (IsCallerCancellation(ex, cancellationToken))
        {
            DeadLetterLog.Cancelled(logger, config.QueueName, "replay", replayed);
            throw;
        }
        catch (Exception ex)
        {
            throw Aborted("replay", replayed, ex);
        }
        finally
        {
            await ReturnHeldAsync(scan);
            Count(replayed, purged: false);
        }

        DeadLetterLog.Replayed(logger, config.QueueName, replayed, failed, skipped, options.DryRun);
        return new DeadLetterReplayResult
        {
            DryRun = options.DryRun,
            Matched = matched,
            Replayed = replayed,
            Skipped = skipped,
            Failed = failed,
            Failures = failures
        };
    }

    /// <inheritdoc />
    /// <remarks>
    /// A pass that fails, or a removal that fails, ends the call: what was deleted is counted and logged, and the call
    /// throws <see cref="Exceptions.MessageQueueException"/> (naming only the failure's type); a throwing
    /// <see cref="DeadLetterPurgeOptions{T}.Filter"/> ends it the same way with the filter's own exception.
    /// </remarks>
    public async Task<int> PurgeAsync(DeadLetterPurgeOptions<T> options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentOutOfRangeException.ThrowIfLessThan(options.MaxCount, 1, nameof(options.MaxCount));
        ValidateReasonCodes(options.ReasonCodes);
        Require(DeadLetterOperations.Purge);

        await gate.WaitAsync(cancellationToken);
        var purged = 0;
        IDeadLetterScan? scan = null;
        try
        {
            scan = await store.OpenScanAsync(browseOnly: false, cancellationToken);
            var examined = 0;
            while (purged < options.MaxCount && examined < DeadLetterLimits.MaxScanCount)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (await scan.NextAsync(cancellationToken) is not { } entry)
                    break;
                examined++;

                if (!Selected(Snapshot(entry), options.ReasonCodes, [], options.Filter))
                    continue;

                await entry.RemoveAsync(cancellationToken);
                purged++;
            }
        }
        catch (Exception ex) when (IsCallerCancellation(ex, cancellationToken))
        {
            DeadLetterLog.Cancelled(logger, config.QueueName, "purge", purged);
            throw;
        }
        catch (Exception ex)
        {
            throw Aborted("purge", purged, ex);
        }
        finally
        {
            await ReturnHeldAsync(scan);
            Count(purged, purged: true);
            gate.Release();
        }

        DeadLetterLog.Purged(logger, config.QueueName, purged);
        return purged;
    }

    static bool IsCallerCancellation(Exception exception, CancellationToken cancellationToken) =>
        exception is OperationCanceledException && cancellationToken.IsCancellationRequested;

    // A failed pass: logged with how far it got. The application's own filter exception goes through unchanged; a
    // queue already disposed stays ObjectDisposedException; anything else from the store or the broker client becomes a
    // MessageQueueException naming only its type, since client text can carry endpoints or credentials.
    Exception Aborted(string operation, int done, Exception error)
    {
        var inner = error is FilterFailedException filter ? filter.InnerException! : error;
        DeadLetterLog.Aborted(logger, config.QueueName, operation, done, BoundedText.TypeName(inner), inner);
        if (error is FilterFailedException)
            return Rethrow(inner);
        if (error is Exceptions.MessageQueueException or ObjectDisposedException)
            return Rethrow(error);
        return new Exceptions.MessageQueueException(
            $"Dead-letter {operation} on queue '{config.QueueName}' failed after {done} message(s) ({inner.GetType().Name}); see the log for details");
    }

    static Exception Rethrow(Exception exception)
    {
        System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(exception).Throw();
        return exception;
    }

    void Require(DeadLetterOperations operation)
    {
        if (!store.Operations.HasFlag(operation))
            throw new NotSupportedException(
                $"Queue '{config.QueueName}' does not support the dead-letter operation {operation} (supported: {store.Operations})");
    }

    static void ValidateReasonCodes(IReadOnlyCollection<string>? reasonCodes)
    {
        if (reasonCodes is null)
            return;
        if (reasonCodes.Count == 0)
            throw new ArgumentException("ReasonCodes must not be empty: set it to null to select the default reasons", nameof(reasonCodes));
        if (reasonCodes.Any(string.IsNullOrWhiteSpace))
            throw new ArgumentException("ReasonCodes must not contain a null or blank code", nameof(reasonCodes));
    }

    // Explicit reason codes select exactly those; without them every reason except `excludedByDefault` is selected.
    static bool Selected(DeadLetterMessage<T> letter, IReadOnlyCollection<string>? reasonCodes, string[] excludedByDefault, Func<DeadLetterMessage<T>, bool>? filter)
    {
        var reasonSelected = reasonCodes is null
            ? letter.ReasonCode is null || !excludedByDefault.Contains(letter.ReasonCode, StringComparer.Ordinal)
            : letter.ReasonCode is { } code && reasonCodes.Contains(code, StringComparer.Ordinal);
        return reasonSelected && (filter is null || Invoke(filter, letter));
    }

    // The application's filter: its exception ends the call unchanged, so it is told apart from store failures.
    static bool Invoke(Func<DeadLetterMessage<T>, bool> filter, DeadLetterMessage<T> letter)
    {
        try
        {
            return filter(letter);
        }
        catch (Exception ex)
        {
            throw new FilterFailedException(ex);
        }
    }

    // What makes two dead letters "the same message" within one replay call: the message id, or for a message without
    // one its body, correlation id and headers.
    static string ReplayKey(DeadLetterMessage<T> letter, ReadOnlyMemory<byte> body)
    {
        if (letter.MessageId.Length > 0)
            return "id:" + letter.MessageId;

        using var hash = System.Security.Cryptography.IncrementalHash.CreateHash(System.Security.Cryptography.HashAlgorithmName.SHA256);
        hash.AppendData(body.Span);
        foreach (var part in letter.Headers.OrderBy(h => h.Key, StringComparer.Ordinal).Select(h => $"\0{h.Key}\0{h.Value}").Prepend($"\0{letter.CorrelationId}"))
            hash.AppendData(System.Text.Encoding.UTF8.GetBytes(part));
        return "content:" + Convert.ToHexString(hash.GetHashAndReset());
    }

    sealed class FilterFailedException(Exception inner) : Exception("The dead-letter filter threw", inner);

    // Everything here came from the broker: bounded and sanitized like consumed metadata, and a body over the size
    // limit is never deserialized (S9).
    DeadLetterMessage<T> Snapshot(IDeadLetterEntry entry)
    {
        var metadata = entry.Metadata;
        var body = entry.Body;
        var headers = new Dictionary<string, string>();
        foreach (var (name, value) in metadata.Headers)
        {
            if (value is not null)
                DeliveryText.TryAddHeader(headers, name, value);
        }

        return new DeadLetterMessage<T>
        {
            MessageId = DeliveryText.MessageId(metadata.MessageId),
            CorrelationId = DeliveryText.CorrelationId(metadata.CorrelationId),
            ReasonCode = BoundedText.Truncate(metadata.ReasonCode),
            ReasonDetail = BoundedText.Truncate(metadata.ReasonDetail),
            EnqueuedAt = metadata.EnqueuedAt,
            DeadLetteredAt = metadata.DeadLetteredAt,
            DeliveryAttempt = metadata.DeliveryAttempt is >= 1 ? metadata.DeliveryAttempt : null,
            Headers = headers,
            Message = body.Length <= config.MaxMessageSizeBytes ? TryDeserialize(body) : null,
            BodyLength = body.Length
        };
    }

    T? TryDeserialize(ReadOnlyMemory<byte> body)
    {
        try
        {
            return MessageSerializer.Deserialize<T>(body, config.SerializationFormat, config.JsonTypeInfoResolver);
        }
        catch (Exception)
        {
            return null;
        }
    }

    // Ends the pass, which hands back what it still holds. Never throws: if the store cannot hand them back, the broker
    // does when the lock or channel ends.
    async Task ReturnHeldAsync(IDeadLetterScan? scan)
    {
        if (scan is null)
            return;
        try
        {
            await scan.DisposeAsync();
        }
        catch (Exception ex)
        {
            DeadLetterLog.ReturnFailed(logger, config.QueueName, ex);
        }
    }

    void Count(int count, bool purged)
    {
        if (count == 0)
            return;
        try
        {
            if (purged)
                telemetry.RecordDeadLettersPurged(target, count);
            else
                telemetry.RecordDeadLettersReplayed(target, count);
        }
        catch (Exception ex)
        {
            DispatcherLog.TelemetryFailed(logger, config.QueueName, ex);
        }
    }
}
#pragma warning restore CA1001

static partial class DeadLetterLog
{
    [LoggerMessage(EventId = 1201, Level = LogLevel.Information,
        Message = "Dead-letter replay on queue {QueueName}: {Replayed} replayed, {Failed} failed, {Skipped} skipped (dry run: {DryRun})")]
    public static partial void Replayed(ILogger logger, string queueName, int replayed, int failed, int skipped, bool dryRun);

    [LoggerMessage(EventId = 1202, Level = LogLevel.Warning,
        Message = "Dead-letter purge on queue {QueueName}: {Purged} dead letter(s) deleted")]
    public static partial void Purged(ILogger logger, string queueName, int purged);

    [LoggerMessage(EventId = 1203, Level = LogLevel.Warning,
        Message = "Replaying dead letter {MessageId} on queue {QueueName} failed ({ExceptionType}); it stays in the dead-letter queue")]
    public static partial void ReplayFailed(ILogger logger, string queueName, string messageId, string exceptionType, Exception exception);

    [LoggerMessage(EventId = 1204, Level = LogLevel.Warning,
        Message = "Dead-letter {Operation} on queue {QueueName} was cancelled after {Done} message(s); the rest stay in the dead-letter queue")]
    public static partial void Cancelled(ILogger logger, string queueName, string operation, int done);

    [LoggerMessage(EventId = 1206, Level = LogLevel.Warning,
        Message = "Dead-letter {Operation} on queue {QueueName} aborted after {Done} message(s) ({ExceptionType}); the rest stay in the dead-letter queue")]
    public static partial void Aborted(ILogger logger, string queueName, string operation, int done, string exceptionType, Exception exception);

    [LoggerMessage(EventId = 1205, Level = LogLevel.Warning,
        Message = "Queue {QueueName}: handing back the dead letters a pass held failed; the broker returns them when their lock or channel ends")]
    public static partial void ReturnFailed(ILogger logger, string queueName, Exception exception);
}
