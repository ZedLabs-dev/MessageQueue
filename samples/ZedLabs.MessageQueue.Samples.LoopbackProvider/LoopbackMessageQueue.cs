using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using ZedLabs.MessageQueue.Abstractions;
using ZedLabs.MessageQueue.Providers;
using ZedLabs.MessageQueue.Telemetry;

namespace ZedLabs.MessageQueue.Samples.LoopbackProvider;

/// <summary>
/// A minimal in-process transport built only on the public provider SPI: publishing validates, serializes and traces
/// through the SPI helpers, and every delivery goes through <see cref="MessageDispatcher{T}"/>, so retry, backoff,
/// dead-lettering and consume spans behave exactly as on the first-party providers. One subscriber, one delivery at
/// a time; retries wait in process. Dead-letter operations come from <see cref="DeadLetterQueue{T}"/> over the queue's
/// own <see cref="IDeadLetterStore"/>.
/// </summary>
public sealed class LoopbackMessageQueue<T> : IMessageQueue<T>, IDeadLetterQueue<T>, IAsyncDisposable where T : class
{
    readonly LoopbackConfig config;
    readonly ITelemetryProvider telemetry;
    readonly ILogger logger;
    readonly TelemetryTarget target;
    readonly Channel<Envelope> channel = Channel.CreateUnbounded<Envelope>();
    readonly List<DeadLetter> deadLetters = [];
    readonly DeadLetterQueue<T> deadLetterQueue;
    CancellationTokenSource? stopping;
    Task? loop;

    public LoopbackMessageQueue(LoopbackConfig config, ITelemetryProvider? telemetry = null, ILogger? logger = null)
    {
        new LoopbackConfigValidator().ThrowIfInvalid(config);
        this.config = config;
        this.telemetry = telemetry ?? NullTelemetryProvider.Instance;
        this.logger = logger ?? NullLogger.Instance;
        target = new TelemetryTarget("zedlabs.loopback", config.QueueName, typeof(T).Name);
        deadLetterQueue = new DeadLetterQueue<T>(new Store(this), config, target, this.telemetry, this.logger);
    }

    public MessageQueueCapabilities Capabilities { get; } =
        new(Durable: false, DeadLetter: true, DelayedRedelivery: false) { DeadLetterOperations = DeadLetterOperations.All };

    /// <summary>Dead-lettered message ids with their reason code.</summary>
    public IReadOnlyList<(string MessageId, string Reason)> DeadLetters
    {
        get
        {
            lock (deadLetters)
                return [.. deadLetters.Select(d => (d.Envelope.MessageId, d.Reason.Code))];
        }
    }

    public Task<IReadOnlyList<DeadLetterMessage<T>>> PeekAsync(int maxCount, CancellationToken cancellationToken = default) =>
        deadLetterQueue.PeekAsync(maxCount, cancellationToken);

    public Task<DeadLetterReplayResult> ReplayAsync(DeadLetterReplayOptions<T> options, CancellationToken cancellationToken = default) =>
        deadLetterQueue.ReplayAsync(options, cancellationToken);

    public Task<int> PurgeAsync(DeadLetterPurgeOptions<T> options, CancellationToken cancellationToken = default) =>
        deadLetterQueue.PurgeAsync(options, cancellationToken);

    public Task PublishAsync(T message, CancellationToken cancellationToken = default) =>
        PublishAsync(message, new PublishOptions(), cancellationToken);

    public async Task PublishAsync(T message, PublishOptions options, CancellationToken cancellationToken = default)
    {
        PublishValidation.Validate(message, options);
        var body = MessageSerializer.Serialize(message, config.SerializationFormat, config.JsonTypeInfoResolver);
        PublishValidation.ValidateBodySize(body.Length, config.MaxMessageSizeBytes, config.QueueName);
        var messageId = options.MessageId ?? Guid.CreateVersion7().ToString();
        using var trace = PublishTelemetry.Start(telemetry, target, 1, logger, messageId);
        await channel.Writer.WriteAsync(
            new Envelope(body, messageId, options.CorrelationId, options.Headers ?? new Dictionary<string, string>(), trace.TraceHeaders, 1),
            cancellationToken);
    }

    public async Task PublishAsync(IEnumerable<T> messages, CancellationToken cancellationToken = default)
    {
        PublishValidation.Validate(messages);
        foreach (var message in messages)
            await PublishAsync(message, cancellationToken);
    }

    public Task SubscribeAsync(Func<T, MessageContext, CancellationToken, Task<MessageResult>> handler, CancellationToken cancellationToken = default)
    {
        if (loop is not null)
            throw new InvalidOperationException("Already subscribed");

        var dispatcher = new MessageDispatcher<T>(config, target, handler, telemetry, logger);
        stopping = new CancellationTokenSource();
        loop = Task.Run(() => RunAsync(dispatcher, stopping.Token), CancellationToken.None);
        return Task.CompletedTask;
    }

    public Task SubscribeAsync(Func<T, Task<bool>> handler, CancellationToken cancellationToken = default) =>
        SubscribeAsync(async (message, _, _) => await handler(message) ? MessageResult.Acknowledge : MessageResult.Retry, cancellationToken);

    public async Task UnsubscribeAsync(CancellationToken cancellationToken = default)
    {
        if (stopping is null || loop is null)
            return;
        await stopping.CancelAsync();
        await loop.WaitAsync(cancellationToken);
        stopping.Dispose();
        (stopping, loop) = (null, null);
    }

    public ValueTask DisposeAsync() => new(UnsubscribeAsync());

    async Task RunAsync(MessageDispatcher<T> dispatcher, CancellationToken token)
    {
        try
        {
            await foreach (var envelope in channel.Reader.ReadAllAsync(token))
            {
                var context = new MessageContext
                {
                    MessageId = envelope.MessageId,
                    CorrelationId = envelope.CorrelationId,
                    Timestamp = DateTimeOffset.UtcNow,
                    DeliveryAttempt = envelope.Attempt,
                    Headers = envelope.Headers
                }.WithTraceHeaders(envelope.Trace.GetValueOrDefault(TraceHeaders.TraceParent), envelope.Trace.GetValueOrDefault(TraceHeaders.TraceState));

                await dispatcher.DispatchAsync(envelope.Body, context, new Actions(this, envelope), token);
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            // Unsubscribed: undelivered messages stay in the channel.
        }
    }

    sealed record Envelope(
        byte[] Body,
        string MessageId,
        string? CorrelationId,
        IReadOnlyDictionary<string, string> Headers,
        IReadOnlyDictionary<string, string> Trace,
        int Attempt);

    // The transport seam: what the dispatcher decided, executed on this "broker".
    sealed class Actions(LoopbackMessageQueue<T> queue, Envelope envelope) : IDeliveryActions
    {
        public Task CompleteAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public async Task RescheduleAsync(TimeSpan delay, int nextAttempt, CancellationToken cancellationToken)
        {
            await Task.Delay(delay, cancellationToken);
            await queue.channel.Writer.WriteAsync(envelope with { Attempt = nextAttempt }, cancellationToken);
        }

        public Task DeadLetterAsync(DeadLetterReason reason, CancellationToken cancellationToken)
        {
            lock (queue.deadLetters)
            {
                if (queue.deadLetters.Count < queue.config.MaxDeadLetters)
                    queue.deadLetters.Add(new DeadLetter(envelope, reason, DateTimeOffset.UtcNow));
            }
            return Task.CompletedTask;
        }
    }

    // Compared by reference, so replaying or purging one dead letter never touches an equal one.
    sealed class DeadLetter(Envelope envelope, DeadLetterReason reason, DateTimeOffset at)
    {
        public Envelope Envelope { get; } = envelope;
        public DeadLetterReason Reason { get; } = reason;
        public DateTimeOffset At { get; } = at;
    }

    // The dead-letter seam: a pass over a snapshot of the list; an entry acts on its own dead letter while it is there.
    sealed class Store(LoopbackMessageQueue<T> queue) : IDeadLetterStore
    {
        public DeadLetterOperations Operations => DeadLetterOperations.All;

        public Task<IDeadLetterScan> OpenScanAsync(bool browseOnly, CancellationToken cancellationToken)
        {
            lock (queue.deadLetters)
                return Task.FromResult<IDeadLetterScan>(new Scan(queue, new Queue<DeadLetter>(queue.deadLetters)));
        }
    }

    sealed class Scan(LoopbackMessageQueue<T> queue, Queue<DeadLetter> pending) : IDeadLetterScan
    {
        public Task<IDeadLetterEntry?> NextAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IDeadLetterEntry?>(pending.TryDequeue(out var next) ? new Entry(queue, next) : null);

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    sealed class Entry(LoopbackMessageQueue<T> queue, DeadLetter letter) : IDeadLetterEntry
    {
        public DeadLetterMetadata Metadata { get; } = new()
        {
            MessageId = letter.Envelope.MessageId,
            CorrelationId = letter.Envelope.CorrelationId,
            ReasonCode = letter.Reason.Code,
            ReasonDetail = letter.Reason.Detail,
            DeadLetteredAt = letter.At,
            DeliveryAttempt = letter.Envelope.Attempt,
            Headers = letter.Envelope.Headers
        };

        public ReadOnlyMemory<byte> Body => letter.Envelope.Body;

        // Queued first (as attempt 1, same id, headers and trace), then removed.
        public async Task ReplayAsync(CancellationToken cancellationToken)
        {
            await queue.channel.Writer.WriteAsync(letter.Envelope with { Attempt = 1 }, cancellationToken);
            Remove();
        }

        public Task RemoveAsync(CancellationToken cancellationToken)
        {
            Remove();
            return Task.CompletedTask;
        }

        void Remove()
        {
            lock (queue.deadLetters)
                queue.deadLetters.Remove(letter);
        }
    }
}
