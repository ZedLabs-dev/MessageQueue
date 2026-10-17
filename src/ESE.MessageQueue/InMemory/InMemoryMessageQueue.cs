using System.Threading.Channels;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Configuration;
using ESE.MessageQueue.Exceptions;
using ESE.MessageQueue.Processing;
using ESE.MessageQueue.Providers;
using ESE.MessageQueue.Serialization;
using ESE.MessageQueue.Telemetry;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace ESE.MessageQueue.InMemory;

/// <summary>
/// An in-process queue that behaves like a broker: messages are serialized on publish, delivered on a background loop
/// with at most <c>MaxConcurrentCalls</c> handlers at a time, and run through the same retry, backoff and dead-letter
/// pipeline as the broker providers.
/// <para>
/// Single-process and non-durable (ADR 0003): queued messages, scheduled retries and dead letters live in memory and
/// are lost when the process stops. Use it for local development, tests, and deployments that accept that.
/// </para>
/// </summary>
public sealed class InMemoryMessageQueue<T> : IMessageQueue<T>, IDisposable, IAsyncDisposable where T : class
{
    static readonly MessageQueueCapabilities InMemoryCapabilities = new(Durable: false, DeadLetter: true, DelayedRedelivery: true);

    readonly InMemoryConfig config;
    readonly ITelemetryProvider telemetry;
    readonly ILogger logger;
    readonly TelemetryTarget publishTarget;
    readonly Channel<Envelope> queue = Channel.CreateUnbounded<Envelope>(new UnboundedChannelOptions { SingleReader = true });
    readonly CancellationTokenSource lifetime = new();
    readonly CancellationToken stopping;
    readonly object gate = new();
    readonly List<T> published = [];
    readonly List<T> processed = [];
    readonly List<InMemoryDeadLetter<T>> deadLetters = [];
    TaskCompletionSource idle = NewCompletedSignal();
    Subscription? subscription;
    int pending;
    int disposed;

    /// <summary>
    /// Creates the queue. <paramref name="config"/> defaults to a queue named <c>typeof(T).Name</c> and is validated
    /// like startup validation; the queue starts delivering on <c>SubscribeAsync</c>.
    /// </summary>
    public InMemoryMessageQueue(InMemoryConfig? config = null, ITelemetryProvider? telemetry = null, ILogger? logger = null)
    {
        this.config = config ?? new InMemoryConfig { QueueName = typeof(T).Name };
        ConfigValidation.ThrowIfInvalid(this.config, new InMemoryConfigValidator());
        this.telemetry = telemetry ?? NullTelemetryProvider.Instance;
        this.logger = logger ?? NullLogger.Instance;
        publishTarget = new TelemetryTarget(MessagingSystems.InMemory, this.config.QueueName, typeof(T).Name);
        stopping = lifetime.Token;
    }

    /// <summary>
    /// Not durable; dead letters are kept in <see cref="DeadLetters"/>; retries wait for their backoff without holding a
    /// concurrency slot.
    /// </summary>
    public MessageQueueCapabilities Capabilities => InMemoryCapabilities;

    /// <summary>Every message published, in publish order.</summary>
    public IReadOnlyList<T> PublishedMessages
    {
        get
        {
            lock (gate)
            {
                return [.. published];
            }
        }
    }

    /// <summary>Every message a handler acknowledged.</summary>
    public IReadOnlyList<T> ProcessedMessages
    {
        get
        {
            lock (gate)
            {
                return [.. processed];
            }
        }
    }

    /// <summary>Every message that was dead-lettered, with the reason.</summary>
    public IReadOnlyList<InMemoryDeadLetter<T>> DeadLetters
    {
        get
        {
            lock (gate)
            {
                return [.. deadLetters];
            }
        }
    }

    /// <summary>Messages published but not yet acknowledged, dead-lettered or dropped (queued, in a handler, or awaiting a retry).</summary>
    public int PendingCount => Volatile.Read(ref pending);

    /// <summary>True while a handler is subscribed.</summary>
    public bool IsSubscribed => Volatile.Read(ref subscription) is not null;

    /// <inheritdoc />
    public Task PublishAsync(T message, CancellationToken cancellationToken = default) =>
        PublishAsync(message, new PublishOptions(), cancellationToken);

    /// <inheritdoc />
    public Task PublishAsync(T message, PublishOptions options, CancellationToken cancellationToken = default)
    {
        PublishValidation.Validate(message, options);
        ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed) == 1, this);

        var body = Serialize(message);
        var messageId = options.MessageId ?? Guid.CreateVersion7().ToString();
        using var trace = PublishTelemetry.Start(telemetry, publishTarget, 1, logger, messageId);
        Enqueue(message, body, messageId, options, trace);
        return Task.CompletedTask;
    }

    // One producer span for the whole call (ADR 0013); every message carries it as its parent.
    /// <inheritdoc />
    public Task PublishAsync(IEnumerable<T> messages, CancellationToken cancellationToken = default)
    {
        PublishValidation.Validate(messages);
        ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed) == 1, this);
        var options = new PublishOptions();
        var batch = messages.Select(message =>
        {
            PublishValidation.Validate(message, options);
            return (Message: message, Body: Serialize(message));
        }).ToList();
        if (batch.Count == 0)
            return Task.CompletedTask;

        using var trace = PublishTelemetry.Start(telemetry, publishTarget, batch.Count, logger);
        foreach (var (message, body) in batch)
            Enqueue(message, body, Guid.CreateVersion7().ToString(), options, trace);
        return Task.CompletedTask;
    }

    byte[] Serialize(T message)
    {
        try
        {
            return MessageSerializer.Serialize(message, config.SerializationFormat);
        }
        catch (Exception ex)
        {
            throw new MessageQueueException($"Failed to serialize message of type {typeof(T).Name}", ex);
        }
    }

    void Enqueue(T message, byte[] body, string messageId, PublishOptions options, PublishTelemetry trace)
    {
        var context = new MessageContext
        {
            MessageId = messageId,
            CorrelationId = options.CorrelationId,
            Timestamp = DateTimeOffset.UtcNow,
            DeliveryAttempt = 1,
            TraceParent = trace.TraceHeaders.GetValueOrDefault(TraceHeaders.TraceParent),
            TraceState = trace.TraceHeaders.GetValueOrDefault(TraceHeaders.TraceState)
        };
        if (options.Headers is not null)
        {
            context = context with { Headers = options.Headers };
        }

        lock (gate)
        {
            published.Add(message);
            if (pending++ == 0)
                idle = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        queue.Writer.TryWrite(new Envelope(body, context));
    }

    /// <inheritdoc />
    public Task SubscribeAsync(Func<T, Task<bool>> handler, CancellationToken cancellationToken = default) =>
        SubscribeAsync(MessageHandlers.FromPredicate(handler), cancellationToken);

    /// <inheritdoc />
    public Task SubscribeAsync(
        Func<T, MessageContext, CancellationToken, Task<MessageResult>> handler,
        CancellationToken cancellationToken = default)
    {
        lock (gate)
        {
            ObjectDisposedException.ThrowIf(disposed == 1, this);
            if (subscription is not null)
                throw new InvalidOperationException("This queue already has a subscriber. Call UnsubscribeAsync first.");

            var dispatcher = new MessageDispatcher<T>(config, publishTarget, RecordAcknowledged(handler), telemetry, logger);
            var cancellation = CancellationTokenSource.CreateLinkedTokenSource(stopping);
            try
            {
                subscription = new Subscription(cancellation, Task.Run(() => DeliverLoopAsync(dispatcher, cancellation.Token), CancellationToken.None));
            }
            catch
            {
                cancellation.Dispose();
                throw;
            }
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Stops delivering. Handlers get their token cancelled, and a message whose handler is cancelled goes back on the
    /// queue. Waits for running handlers to finish, for at most <c>ShutdownTimeout</c>.
    /// </summary>
    public async Task UnsubscribeAsync(CancellationToken cancellationToken = default)
    {
        Subscription? current;
        lock (gate)
        {
            current = subscription;
            subscription = null;
        }

        if (current is null)
            return;

        try
        {
            await current.Cancellation.CancelAsync();
            await current.Loop.WaitAsync(config.ShutdownTimeout, cancellationToken);
        }
        catch (TimeoutException)
        {
            InMemoryLog.ShutdownTimedOut(logger, config.QueueName, config.ShutdownTimeout);
        }
        finally
        {
            current.Cancellation.Dispose();
        }
    }

    /// <summary>
    /// Completes when every published message has been acknowledged, dead-lettered or dropped, including retries
    /// that are still waiting out their backoff. Messages published without a subscriber keep the queue busy.
    /// </summary>
    public Task WaitForIdleAsync(CancellationToken cancellationToken = default)
    {
        lock (gate)
        {
            ObjectDisposedException.ThrowIf(disposed == 1, this);
            return idle.Task.WaitAsync(cancellationToken);
        }
    }

    /// <summary>Clears the recorded published, processed and dead-lettered messages. Queued messages are kept.</summary>
    public void Clear()
    {
        lock (gate)
        {
            published.Clear();
            processed.Clear();
            deadLetters.Clear();
        }
    }

    async Task DeliverLoopAsync(MessageDispatcher<T> dispatcher, CancellationToken cancellationToken)
    {
        using var slots = new SemaphoreSlim(config.MaxConcurrentCalls, config.MaxConcurrentCalls);
        try
        {
            while (true)
            {
                // Take a handler slot before taking a message, so cancellation never strands a dequeued message.
                await slots.WaitAsync(cancellationToken);
                Envelope envelope;
                try
                {
                    envelope = await queue.Reader.ReadAsync(cancellationToken);
                }
                catch
                {
                    slots.Release();
                    throw;
                }

                _ = DeliverAsync(dispatcher, envelope, slots, cancellationToken);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (ChannelClosedException)
        {
        }
        finally
        {
            for (var i = 0; i < config.MaxConcurrentCalls; i++)
            {
                await slots.WaitAsync(CancellationToken.None);
            }
        }
    }

    async Task DeliverAsync(MessageDispatcher<T> dispatcher, Envelope envelope, SemaphoreSlim slots, CancellationToken cancellationToken)
    {
        var actions = new DeliveryActions(this, envelope);
        try
        {
            await dispatcher.DispatchAsync(envelope.Body, envelope.Context, actions, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Unsubscribed mid-handler: put the message back unchanged, like a broker redelivering an unacked message.
            Requeue(envelope);
        }
        catch (Exception ex) when (actions.Handled)
        {
            // The message was already settled or rescheduled; only a later step (telemetry, logging) failed.
            InMemoryLog.AfterDeliveryFailed(logger, config.QueueName, envelope.Context.MessageId, ex);
        }
        catch (Exception ex)
        {
            // The pipeline itself failed before settling: keep the message for inspection rather than lose it.
            InMemoryLog.DeliveryFailed(logger, config.QueueName, envelope.Context.MessageId, ex);
            if (config.EnableDeadLetterQueue)
                DeadLetter(envelope, new DeadLetterReason(DeadLetterReasons.DeliveryFailed, ex.GetType().FullName));
            else
                Settle();
        }
        finally
        {
            slots.Release();
        }
    }

    Func<T, MessageContext, CancellationToken, Task<MessageResult>> RecordAcknowledged(
        Func<T, MessageContext, CancellationToken, Task<MessageResult>> handler) =>
        async (message, context, cancellationToken) =>
        {
            var result = await handler(message, context, cancellationToken);
            if (result == MessageResult.Acknowledge)
            {
                lock (gate)
                {
                    processed.Add(message);
                }
            }
            return result;
        };

    void Settle()
    {
        lock (gate)
        {
            if (--pending == 0)
                idle.TrySetResult();
        }
    }

    async Task RedeliverAfterAsync(Envelope envelope, TimeSpan delay)
    {
        try
        {
            await Task.Delay(delay, stopping);
            Requeue(envelope);
        }
        catch (OperationCanceledException)
        {
            // Disposed while waiting: the retry is lost, as documented (non-durable).
        }
    }

    // A message that can no longer be queued (the queue was disposed) is settled, so waiters are not left hanging.
    void Requeue(Envelope envelope)
    {
        if (!queue.Writer.TryWrite(envelope))
            Settle();
    }

    void DeadLetter(Envelope envelope, DeadLetterReason reason)
    {
        var message = reason.Code == DeadLetterReasons.MessageTooLarge ? null : TryDeserialize(envelope.Body);
        lock (gate)
        {
            deadLetters.Add(new InMemoryDeadLetter<T>(message, envelope.Body, envelope.Context, reason.Code, reason.Detail));
        }
        Settle();
    }

    T? TryDeserialize(byte[] body)
    {
        try
        {
            return MessageSerializer.Deserialize<T>(body, config.SerializationFormat);
        }
        catch (Exception)
        {
            return null;
        }
    }

    /// <summary>
    /// Unsubscribes (waiting up to <c>ShutdownTimeout</c> for running handlers) and stops the queue. Idempotent;
    /// queued messages are discarded.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref disposed, 1) == 1)
            return;

        await UnsubscribeAsync(CancellationToken.None);
        Shutdown();
    }

    /// <summary>
    /// Stops the queue without waiting: running handlers are cancelled and finish in the background. Prefer
    /// <see cref="DisposeAsync"/>. Idempotent.
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) == 1)
            return;

        // No blocking wait on handlers: cancel them and let the delivery loop wind down on its own.
        Subscription? current;
        lock (gate)
        {
            current = subscription;
            subscription = null;
        }
        if (current is not null)
        {
            current.Cancellation.Cancel();
            current.Cancellation.Dispose();
        }
        Shutdown();
    }

    // Cancels pending retries, closes the queue and releases WaitForIdleAsync callers: messages still queued or
    // waiting to retry are lost (non-durable, ADR 0003).
    void Shutdown()
    {
        lifetime.Cancel();
        queue.Writer.TryComplete();
        lock (gate)
        {
            idle.TrySetException(new ObjectDisposedException(GetType().FullName));
        }
        lifetime.Dispose();
    }

    static TaskCompletionSource NewCompletedSignal()
    {
        var signal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        signal.SetResult();
        return signal;
    }

    internal sealed record Envelope(byte[] Body, MessageContext Context);

    sealed record Subscription(CancellationTokenSource Cancellation, Task Loop);

    internal sealed class DeliveryActions : IDeliveryActions
    {
        readonly InMemoryMessageQueue<T> owner;
        readonly Envelope envelope;

        public DeliveryActions(InMemoryMessageQueue<T> owner, Envelope envelope)
        {
            this.owner = owner;
            this.envelope = envelope;
        }

        // True once the message was completed, rescheduled or dead-lettered, so it is never settled twice.
        public bool Handled { get; private set; }

        public Task CompleteAsync(CancellationToken cancellationToken)
        {
            Handled = true;
            owner.Settle();
            return Task.CompletedTask;
        }

        // Real backoff: the message waits on a timer, not in a handler slot, then goes to the back of the queue.
        public Task RescheduleAsync(TimeSpan delay, int nextAttempt, CancellationToken cancellationToken)
        {
            Handled = true;
            var next = envelope with { Context = envelope.Context with { DeliveryAttempt = nextAttempt } };
            _ = owner.RedeliverAfterAsync(next, delay);
            return Task.CompletedTask;
        }

        public Task DeadLetterAsync(DeadLetterReason reason, CancellationToken cancellationToken)
        {
            Handled = true;
            owner.DeadLetter(envelope, reason);
            return Task.CompletedTask;
        }
    }
}

static partial class InMemoryLog
{
    [LoggerMessage(EventId = 4001, Level = LogLevel.Error,
        Message = "Delivery of message {MessageId} on in-memory queue {QueueName} failed before it was settled; the message was dead-lettered")]
    public static partial void DeliveryFailed(ILogger logger, string queueName, string messageId, Exception exception);

    [LoggerMessage(EventId = 4002, Level = LogLevel.Error,
        Message = "Message {MessageId} on in-memory queue {QueueName} was settled, but a step after settling failed")]
    public static partial void AfterDeliveryFailed(ILogger logger, string queueName, string messageId, Exception exception);

    [LoggerMessage(EventId = 4003, Level = LogLevel.Warning,
        Message = "In-memory queue {QueueName}: handlers did not finish within {ShutdownTimeout}; stopped waiting for them")]
    public static partial void ShutdownTimedOut(ILogger logger, string queueName, TimeSpan shutdownTimeout);
}
