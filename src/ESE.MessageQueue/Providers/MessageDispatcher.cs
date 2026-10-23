using System.Diagnostics;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Configuration;
using ESE.MessageQueue.Processing;
using ESE.MessageQueue.Retry;
using ESE.MessageQueue.Serialization;
using ESE.MessageQueue.Telemetry;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace ESE.MessageQueue.Providers;

/// <summary>
/// The shared consumer pipeline (ADR 0004). A provider hands each delivery to <see cref="DispatchAsync"/> with an
/// <see cref="IDeliveryActions"/> for that delivery; the dispatcher decides, the transport only executes:
/// size limit, deserialization, the handler, then the retry policy, and finally complete, reschedule or dead-letter
/// (or drop, when dead-lettering is disabled). It also records the consume span and metrics.
/// </summary>
/// <remarks>
/// Create one per subscription. <see cref="DispatchAsync"/> is thread-safe: call it concurrently for different
/// deliveries, up to the provider's concurrency limit (<c>MaxConcurrentCalls</c>).
/// </remarks>
/// <typeparam name="T">The message type.</typeparam>
public sealed class MessageDispatcher<T> where T : class
{
    const string RetryRequested = "Retry";

    readonly MessageQueueConfig config;
    readonly TelemetryTarget target;
    readonly Func<T, MessageContext, CancellationToken, Task<MessageResult>> handler;
    readonly ITelemetryProvider telemetry;
    readonly ILogger logger;
    readonly RetryExecutor retry;

    /// <summary>Creates the pipeline for one subscription.</summary>
    /// <param name="config">The queue's config: size limit, serialization format, retry policy, dead-lettering.</param>
    /// <param name="target">What consume spans and metrics are about (<c>messaging.system</c>, destination, message type).</param>
    /// <param name="handler">The subscriber's handler.</param>
    /// <param name="telemetry">The telemetry provider; none when null.</param>
    /// <param name="logger">The provider's logger; a throwing logger is ignored.</param>
    public MessageDispatcher(
        MessageQueueConfig config,
        TelemetryTarget target,
        Func<T, MessageContext, CancellationToken, Task<MessageResult>> handler,
        ITelemetryProvider? telemetry = null,
        ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(target);
        ArgumentNullException.ThrowIfNull(handler);
        // A snapshot: a provider (or the app) changing its config object later does not change running subscriptions.
        this.config = config with { };
        this.target = target;
        this.handler = handler;
        this.telemetry = telemetry ?? NullTelemetryProvider.Instance;
        this.logger = new SafeLogger(logger ?? NullLogger.Instance);
        retry = new RetryExecutor(config.Retry);
    }

    /// <summary>
    /// Runs one delivery through the pipeline and settles it through <paramref name="actions"/>. Telemetry, logging
    /// and the retry policy's callbacks can fail without affecting the outcome; only a failing
    /// <see cref="IDeliveryActions"/> call propagates.
    /// </summary>
    /// <param name="body">The message body as received; it is not copied.</param>
    /// <param name="context">The delivery's metadata, with the 1-based <c>DeliveryAttempt</c> and, through
    /// <see cref="MessageContextExtensions.WithTraceHeaders"/>, the trace headers it arrived with.</param>
    /// <param name="actions">How to settle this delivery.</param>
    /// <param name="stopping">Cancelled when the subscription stops; the handler gets it.</param>
    /// <exception cref="OperationCanceledException"><paramref name="stopping"/> cancelled the handler; the delivery was not settled.</exception>
    public async Task<DispatchOutcome> DispatchAsync(
        ReadOnlyMemory<byte> body,
        MessageContext context,
        IDeliveryActions actions,
        CancellationToken stopping)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(actions);
        // The ids are logged, tagged on spans and shown to the handler: no control characters, at most the publish-side
        // id length, whatever the provider passed in.
        var messageId = DeliveryText.MessageId(context.MessageId);
        var correlationId = DeliveryText.CorrelationId(context.CorrelationId);
        if (!ReferenceEquals(messageId, context.MessageId) || !ReferenceEquals(correlationId, context.CorrelationId))
            context = context with { MessageId = messageId, CorrelationId = correlationId };

        // A consume span continues the publisher's trace, never the span that happened to be current on the consumer
        // thread (the subscriber's, captured when the transport started its loop). This async method's change to
        // Activity.Current does not leak to the caller.
        Activity.Current = null;
        var started = Stopwatch.GetTimestamp();
        var outcome = new DeliveryOutcome();
        var activity = StartActivity(context, ExtractParent(context));
        try
        {
            return await DispatchCoreAsync(body, context, actions, activity, outcome, stopping);
        }
        finally
        {
            if (outcome.Counted)
            {
                // The span reports the same outcome as the metrics: error.type and Error status unless acknowledged.
                if (outcome.ErrorType is { } errorType && activity is not null)
                    Guard((activity, errorType), static s => s.activity.SetErrorType(s.errorType));
                Guard((telemetry, target, Stopwatch.GetElapsedTime(started), outcome.ErrorType),
                    static s => s.telemetry.RecordMessageConsumed(s.target, s.Item3, s.ErrorType));
            }
            if (activity is not null)
                Guard(activity, static a => a.Dispose());
        }
    }

    async Task<DispatchOutcome> DispatchCoreAsync(
        ReadOnlyMemory<byte> body,
        MessageContext context,
        IDeliveryActions actions,
        TelemetryActivity? activity,
        DeliveryOutcome outcome,
        CancellationToken stopping)
    {
        if (body.Length > config.MaxMessageSizeBytes)
        {
            outcome.Fail(DeadLetterReasons.MessageTooLarge);
            return await DeadLetterAsync(actions, activity, context.MessageId, new DeadLetterReason(DeadLetterReasons.MessageTooLarge));
        }

        T? message;
        try
        {
            message = MessageSerializer.Deserialize<T>(body, config.SerializationFormat, config.JsonTypeInfoResolver);
        }
        catch (Exception ex)
        {
            SetError(activity, ex);
            outcome.Fail(DeadLetterReasons.DeserializationFailed);
            return await DeadLetterAsync(actions, activity, context.MessageId, new DeadLetterReason(DeadLetterReasons.DeserializationFailed, TypeName(ex)));
        }

        if (message is null)
        {
            outcome.Fail(DeadLetterReasons.DeserializationFailed);
            return await DeadLetterAsync(actions, activity, context.MessageId, new DeadLetterReason(DeadLetterReasons.DeserializationFailed));
        }

        MessageResult result;
        Exception? failure = null;
        try
        {
            result = await handler(message, context, stopping);
        }
        catch (OperationCanceledException) when (stopping.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            failure = ex;
            result = MessageResult.Retry;
            SetError(activity, ex);
            DispatcherLog.HandlerFailed(logger, config.QueueName, context.MessageId, context.DeliveryAttempt, TypeName(ex), ex);
        }

        switch (result)
        {
            case MessageResult.Acknowledge:
                outcome.Succeed();
                await actions.CompleteAsync(CancellationToken.None);
                return DispatchOutcome.Completed;

            case MessageResult.DeadLetter:
                outcome.Fail(DeadLetterReasons.Rejected);
                return await DeadLetterAsync(actions, activity, context.MessageId, new DeadLetterReason(DeadLetterReasons.Rejected));

            default:
                outcome.Fail(failure is null ? RetryRequested : TypeName(failure));
                return await RetryOrDeadLetterAsync(actions, activity, context, failure, stopping);
        }
    }

    async Task<DispatchOutcome> RetryOrDeadLetterAsync(
        IDeliveryActions actions,
        TelemetryActivity? activity,
        MessageContext context,
        Exception? failure,
        CancellationToken stopping)
    {
        var attempt = context.DeliveryAttempt;

        if (!retry.ShouldRetry(attempt, failure))
        {
            var code = attempt > config.Retry.MaxRetries ? DeadLetterReasons.MaxRetriesExceeded : DeadLetterReasons.NonRetryableError;
            return await DeadLetterAsync(actions, activity, context.MessageId, new DeadLetterReason(code, failure is null ? null : TypeName(failure)));
        }

        var delay = DelayBeforeRetry(attempt);
        await actions.RescheduleAsync(delay, attempt + 1, stopping);
        if (activity is not null)
        {
            Guard((activity, attempt, delay), static s =>
            {
                s.activity.SetTag(TelemetryNames.RetryNextAttempt, s.attempt + 1);
                s.activity.SetTag(TelemetryNames.RetryDelay, (long)s.delay.TotalMilliseconds);
            });
        }
        Guard((telemetry, target, attempt), static s => s.telemetry.RecordMessageRetried(s.target, s.attempt + 1));
        DispatcherLog.Rescheduled(logger, config.QueueName, context.MessageId, attempt + 1, delay);
        return DispatchOutcome.Rescheduled;
    }

    /// <summary>
    /// Settles a delivery the transport could not read (its metadata or body): dead-letters it as
    /// <see cref="DeadLetterReasons.DeliveryFailed"/> with the exception type, or drops it with a warning when
    /// dead-lettering is disabled. Only a failing <see cref="IDeliveryActions"/> call propagates.
    /// </summary>
    public Task<DispatchOutcome> RejectUnreadableAsync(string messageId, IDeliveryActions actions, Exception error)
    {
        ArgumentNullException.ThrowIfNull(actions);
        ArgumentNullException.ThrowIfNull(error);
        messageId = DeliveryText.MessageId(messageId);
        var detail = TypeName(error);
        DispatcherLog.DeliveryUnreadable(logger, config.QueueName, messageId, detail, error);
        Guard((telemetry, target), static s => s.telemetry.RecordMessageConsumed(s.target, TimeSpan.Zero, DeadLetterReasons.DeliveryFailed));
        return DeadLetterAsync(actions, null, messageId, new DeadLetterReason(DeadLetterReasons.DeliveryFailed, detail));
    }

    TimeSpan DelayBeforeRetry(int attempt)
    {
        try
        {
            return retry.CalculateDelay(attempt);
        }
        catch (Exception ex)
        {
            // Only a user-supplied CustomDelayCalculator can throw here.
            var fallback = config.Retry.InitialDelay < config.Retry.MaxDelay ? config.Retry.InitialDelay : config.Retry.MaxDelay;
            DispatcherLog.DelayCalculatorFailed(logger, config.QueueName, fallback, ex);
            return fallback < TimeSpan.Zero ? TimeSpan.Zero : fallback;
        }
    }

    async Task<DispatchOutcome> DeadLetterAsync(IDeliveryActions actions, TelemetryActivity? activity, string messageId, DeadLetterReason reason)
    {
        if (activity is not null)
            Guard((activity, reason.Code), static s => s.activity.SetTag(TelemetryNames.DeadLetterReason, s.Code));
        if (!config.EnableDeadLetterQueue)
        {
            DispatcherLog.Dropped(logger, config.QueueName, messageId, reason.Code, reason.Detail);
            await actions.CompleteAsync(CancellationToken.None);
            return DispatchOutcome.Dropped;
        }

        await actions.DeadLetterAsync(reason, CancellationToken.None);
        Guard((telemetry, target, reason.Code), static s => s.telemetry.RecordMessageDeadLettered(s.target, s.Code));
        DispatcherLog.DeadLettered(logger, config.QueueName, messageId, reason.Code, reason.Detail);
        return DispatchOutcome.DeadLettered;
    }

    // The publisher's trace context, read from the headers the transport found on the message. Malformed or missing
    // headers mean no parent: the consume span then starts a new trace.
    TraceContext? ExtractParent(MessageContext context)
    {
        if (context.TraceParent is null || telemetry is NullTelemetryProvider)
            return null;

        try
        {
            return telemetry.ExtractTraceContext(new TraceHeaderCarrier(context.TraceParent, context.TraceState));
        }
        catch (Exception ex)
        {
            DispatcherLog.TelemetryFailed(logger, config.QueueName, ex);
            return null;
        }
    }

    TelemetryActivity? StartActivity(MessageContext context, TraceContext? parent)
    {
        try
        {
            return telemetry.StartConsumeActivity(target, context, parent);
        }
        catch (Exception ex)
        {
            DispatcherLog.TelemetryFailed(logger, config.QueueName, ex);
            return null;
        }
    }

    void SetError(TelemetryActivity? activity, Exception exception)
    {
        if (activity is not null)
            Guard((activity, exception), static s => s.activity.SetError(s.exception));
    }

    // Telemetry must never change what happens to a message.
    // Static lambdas with explicit state: no closure is allocated per message.
    void Guard<TState>(TState state, Action<TState> telemetryCall)
    {
        try
        {
            telemetryCall(state);
        }
        catch (Exception ex)
        {
            DispatcherLog.TelemetryFailed(logger, config.QueueName, ex);
        }
    }

    static string TypeName(Exception exception) => BoundedText.TypeName(exception);

    // The two trace headers as a read-only dictionary, without allocating a hash table per message.
    sealed class TraceHeaderCarrier : IReadOnlyDictionary<string, string>
    {
        readonly string traceParent;
        readonly string? traceState;

        public TraceHeaderCarrier(string traceParent, string? traceState)
        {
            this.traceParent = traceParent;
            this.traceState = traceState;
        }

        public int Count => traceState is null ? 1 : 2;

        public string this[string key] => TryGetValue(key, out var value) ? value : throw new KeyNotFoundException(key);

        public IEnumerable<string> Keys => this.Select(pair => pair.Key);

        public IEnumerable<string> Values => this.Select(pair => pair.Value);

        public bool ContainsKey(string key) => TryGetValue(key, out _);

        public bool TryGetValue(string key, [System.Diagnostics.CodeAnalysis.MaybeNullWhen(false)] out string value)
        {
            value = key switch
            {
                TraceHeaders.TraceParent => traceParent,
                TraceHeaders.TraceState => traceState,
                _ => null
            };
            return value is not null;
        }

        public IEnumerator<KeyValuePair<string, string>> GetEnumerator()
        {
            yield return new(TraceHeaders.TraceParent, traceParent);
            if (traceState is not null)
                yield return new(TraceHeaders.TraceState, traceState);
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    // What the consumed-messages metric reports: null (acknowledged), a dead-letter reason code, "Retry" (the handler
    // asked for one) or the type of the exception the handler threw. Unset when the delivery was cancelled by shutdown.
    sealed class DeliveryOutcome
    {
        public bool Counted { get; private set; }
        public string? ErrorType { get; private set; }

        public void Succeed() => Counted = true;

        public void Fail(string errorType)
        {
            Counted = true;
            ErrorType = errorType;
        }
    }
}

static partial class DispatcherLog
{
    [LoggerMessage(EventId = 1001, Level = LogLevel.Warning,
        Message = "Handler for queue {QueueName} failed on message {MessageId} (attempt {DeliveryAttempt}) with {ExceptionType}")]
    public static partial void HandlerFailed(
        ILogger logger, string queueName, string messageId, int deliveryAttempt, string exceptionType, Exception exception);

    [LoggerMessage(EventId = 1002, Level = LogLevel.Debug,
        Message = "Message {MessageId} on queue {QueueName} rescheduled as attempt {NextAttempt} after {Delay}")]
    public static partial void Rescheduled(ILogger logger, string queueName, string messageId, int nextAttempt, TimeSpan delay);

    [LoggerMessage(EventId = 1003, Level = LogLevel.Warning,
        Message = "Message {MessageId} on queue {QueueName} dead-lettered: {Reason} {Detail}")]
    public static partial void DeadLettered(ILogger logger, string queueName, string messageId, string reason, string? detail);

    [LoggerMessage(EventId = 1004, Level = LogLevel.Warning,
        Message = "Message {MessageId} on queue {QueueName} dropped because dead-lettering is disabled: {Reason} {Detail}")]
    public static partial void Dropped(ILogger logger, string queueName, string messageId, string reason, string? detail);

    [LoggerMessage(EventId = 1005, Level = LogLevel.Warning,
        Message = "Telemetry for queue {QueueName} failed; the message is processed regardless")]
    public static partial void TelemetryFailed(ILogger logger, string queueName, Exception exception);

    [LoggerMessage(EventId = 1006, Level = LogLevel.Warning,
        Message = "Retry delay calculator for queue {QueueName} failed; retrying after {FallbackDelay} instead")]
    public static partial void DelayCalculatorFailed(ILogger logger, string queueName, TimeSpan fallbackDelay, Exception exception);

    [LoggerMessage(EventId = 1007, Level = LogLevel.Error,
        Message = "Message {MessageId} on queue {QueueName} could not be read ({ExceptionType}); dead-lettering it as DeliveryFailed")]
    public static partial void DeliveryUnreadable(ILogger logger, string queueName, string messageId, string exceptionType, Exception exception);

    [LoggerMessage(EventId = 1009, Level = LogLevel.Warning,
        Message = "Publish telemetry for queue {QueueName} failed; the message is published regardless")]
    public static partial void PublishTelemetryFailed(ILogger logger, string queueName, Exception exception);
}
