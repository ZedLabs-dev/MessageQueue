using System.Collections.Concurrent;
using System.Threading.Channels;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Providers;
using ESE.MessageQueue.Telemetry;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace ESE.MessageQueue.Samples.LoopbackProvider;

/// <summary>
/// A minimal in-process transport built only on the public provider SPI: publishing validates, serializes and traces
/// through the SPI helpers, and every delivery goes through <see cref="MessageDispatcher{T}"/>, so retry, backoff,
/// dead-lettering and consume spans behave exactly as on the first-party providers. One subscriber, one delivery at
/// a time; retries wait in process.
/// </summary>
public sealed class LoopbackMessageQueue<T> : IMessageQueue<T>, IAsyncDisposable where T : class
{
    readonly LoopbackConfig config;
    readonly ITelemetryProvider telemetry;
    readonly ILogger logger;
    readonly TelemetryTarget target;
    readonly Channel<Envelope> channel = Channel.CreateUnbounded<Envelope>();
    readonly ConcurrentQueue<(Envelope Envelope, DeadLetterReason Reason)> deadLetters = new();
    CancellationTokenSource? stopping;
    Task? loop;

    public LoopbackMessageQueue(LoopbackConfig config, ITelemetryProvider? telemetry = null, ILogger? logger = null)
    {
        new LoopbackConfigValidator().ThrowIfInvalid(config);
        this.config = config;
        this.telemetry = telemetry ?? NullTelemetryProvider.Instance;
        this.logger = logger ?? NullLogger.Instance;
        target = new TelemetryTarget("ese.loopback", config.QueueName, typeof(T).Name);
    }

    public MessageQueueCapabilities Capabilities { get; } = new(Durable: false, DeadLetter: true, DelayedRedelivery: false);

    /// <summary>Dead-lettered message ids with their reason code.</summary>
    public IReadOnlyList<(string MessageId, string Reason)> DeadLetters => [.. deadLetters.Select(d => (d.Envelope.MessageId, d.Reason.Code))];

    public Task PublishAsync(T message, CancellationToken cancellationToken = default) =>
        PublishAsync(message, new PublishOptions(), cancellationToken);

    public async Task PublishAsync(T message, PublishOptions options, CancellationToken cancellationToken = default)
    {
        PublishValidation.Validate(message, options);
        var messageId = options.MessageId ?? Guid.CreateVersion7().ToString();
        using var trace = PublishTelemetry.Start(telemetry, target, 1, logger, messageId);
        var body = MessageSerializer.Serialize(message, config.SerializationFormat);
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
            if (queue.deadLetters.Count < queue.config.MaxDeadLetters)
                queue.deadLetters.Enqueue((envelope, reason));
            return Task.CompletedTask;
        }
    }
}
