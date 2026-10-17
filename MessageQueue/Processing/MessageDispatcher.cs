using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Configuration;
using ESE.MessageQueue.Retry;
using ESE.MessageQueue.Serialization;
using ESE.MessageQueue.Telemetry;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace ESE.MessageQueue.Processing;

// The shared consumer pipeline (ADR 0004). Every provider hands each delivery to DispatchAsync with an
// IDeliveryActions for that delivery; the dispatcher decides, the transport only executes.
//   size limit -> deserialize -> handler -> retry policy -> complete / reschedule / dead-letter (or drop).
sealed class MessageDispatcher<T> where T : class
{
    static readonly string MessageType = typeof(T).Name;

    readonly MessageQueueConfig config;
    readonly Func<T, MessageContext, CancellationToken, Task<MessageResult>> handler;
    readonly ITelemetryProvider telemetry;
    readonly ILogger logger;
    readonly RetryExecutor retry;

    public MessageDispatcher(
        MessageQueueConfig config,
        Func<T, MessageContext, CancellationToken, Task<MessageResult>> handler,
        ITelemetryProvider? telemetry = null,
        ILogger? logger = null)
    {
        this.config = config;
        this.handler = handler;
        this.telemetry = telemetry ?? NullTelemetryProvider.Instance;
        this.logger = new SafeLogger(logger ?? NullLogger.Instance);
        retry = new RetryExecutor(config.Retry);
    }

    // Throws OperationCanceledException, without settling the delivery, when `stopping` cancels the handler.
    // Telemetry, logging and the policy's callbacks can fail without affecting the outcome; only a failing
    // transport action (IDeliveryActions) propagates.
    public async Task<DispatchOutcome> DispatchAsync(
        ReadOnlyMemory<byte> body,
        MessageContext context,
        IDeliveryActions actions,
        CancellationToken stopping)
    {
        var activity = StartActivity(context);
        try
        {
            return await DispatchCoreAsync(body, context, actions, activity, stopping);
        }
        finally
        {
            Guard(() => activity?.Dispose());
        }
    }

    async Task<DispatchOutcome> DispatchCoreAsync(
        ReadOnlyMemory<byte> body,
        MessageContext context,
        IDeliveryActions actions,
        TelemetryActivity? activity,
        CancellationToken stopping)
    {
        if (body.Length > config.MaxMessageSizeBytes)
        {
            RecordConsumed(success: false);
            return await DeadLetterAsync(actions, context.MessageId, new DeadLetterReason(DeadLetterReasons.MessageTooLarge));
        }

        T? message;
        try
        {
            message = MessageSerializer.Deserialize<T>(body, config.SerializationFormat);
        }
        catch (Exception ex)
        {
            SetError(activity, ex);
            RecordConsumed(success: false);
            return await DeadLetterAsync(actions, context.MessageId, new DeadLetterReason(DeadLetterReasons.DeserializationFailed, TypeName(ex)));
        }

        if (message is null)
        {
            RecordConsumed(success: false);
            return await DeadLetterAsync(actions, context.MessageId, new DeadLetterReason(DeadLetterReasons.DeserializationFailed));
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

        RecordConsumed(success: result == MessageResult.Acknowledge);

        switch (result)
        {
            case MessageResult.Acknowledge:
                await actions.CompleteAsync(CancellationToken.None);
                return DispatchOutcome.Completed;

            case MessageResult.DeadLetter:
                return await DeadLetterAsync(actions, context.MessageId, new DeadLetterReason(DeadLetterReasons.Rejected));

            default:
                return await RetryOrDeadLetterAsync(actions, context, failure, stopping);
        }
    }

    async Task<DispatchOutcome> RetryOrDeadLetterAsync(
        IDeliveryActions actions,
        MessageContext context,
        Exception? failure,
        CancellationToken stopping)
    {
        var attempt = context.DeliveryAttempt;

        if (!retry.ShouldRetry(attempt, failure))
        {
            var code = attempt > config.Retry.MaxRetries ? DeadLetterReasons.MaxRetriesExceeded : DeadLetterReasons.NonRetryableError;
            return await DeadLetterAsync(actions, context.MessageId, new DeadLetterReason(code, failure is null ? null : TypeName(failure)));
        }

        var delay = DelayBeforeRetry(attempt);
        await actions.RescheduleAsync(delay, attempt + 1, stopping);
        Guard(() => telemetry.RecordMessageRetried(config.QueueName, MessageType, attempt + 1));
        DispatcherLog.Rescheduled(logger, config.QueueName, context.MessageId, attempt + 1, delay);
        return DispatchOutcome.Rescheduled;
    }

    // A delivery the transport could not even read (its metadata or body): dead-letter it as DeliveryFailed with the
    // exception type, or drop it with a warning when dead-lettering is disabled. Only a failing action propagates.
    public Task<DispatchOutcome> RejectUnreadableAsync(string messageId, IDeliveryActions actions, Exception error)
    {
        var detail = TypeName(error);
        DispatcherLog.DeliveryUnreadable(logger, config.QueueName, messageId, detail, error);
        RecordConsumed(success: false);
        return DeadLetterAsync(actions, messageId, new DeadLetterReason(DeadLetterReasons.DeliveryFailed, detail));
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

    async Task<DispatchOutcome> DeadLetterAsync(IDeliveryActions actions, string messageId, DeadLetterReason reason)
    {
        if (!config.EnableDeadLetterQueue)
        {
            DispatcherLog.Dropped(logger, config.QueueName, messageId, reason.Code, reason.Detail);
            await actions.CompleteAsync(CancellationToken.None);
            return DispatchOutcome.Dropped;
        }

        await actions.DeadLetterAsync(reason, CancellationToken.None);
        Guard(() => telemetry.RecordMessageDeadLettered(config.QueueName, MessageType, reason.Code));
        DispatcherLog.DeadLettered(logger, config.QueueName, messageId, reason.Code, reason.Detail);
        return DispatchOutcome.DeadLettered;
    }

    TelemetryActivity? StartActivity(MessageContext context)
    {
        try
        {
            return telemetry.StartConsumeActivity(config.QueueName, MessageType, context.MessageId);
        }
        catch (Exception ex)
        {
            DispatcherLog.TelemetryFailed(logger, config.QueueName, ex);
            return null;
        }
    }

    void RecordConsumed(bool success) => Guard(() => telemetry.RecordMessageConsumed(config.QueueName, MessageType, success));

    void SetError(TelemetryActivity? activity, Exception exception) => Guard(() => activity?.SetError(exception));

    // Telemetry must never change what happens to a message.
    void Guard(Action telemetryCall)
    {
        try
        {
            telemetryCall();
        }
        catch (Exception ex)
        {
            DispatcherLog.TelemetryFailed(logger, config.QueueName, ex);
        }
    }

    static string TypeName(Exception exception) => exception.GetType().FullName ?? exception.GetType().Name;
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
}
