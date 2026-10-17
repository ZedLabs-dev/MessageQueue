using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Configuration;
using ESE.MessageQueue.Exceptions;
using ESE.MessageQueue.Processing;
using ESE.MessageQueue.Serialization;
using ESE.MessageQueue.Telemetry;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ESE.MessageQueue.RabbitMQ;

/// <summary>
/// RabbitMQ provider (RabbitMQ.Client 7). The constructor only validates: the connection, publisher channels and
/// topology are created on first use, or up front with <see cref="InitializeAsync"/> or <see cref="RabbitMqMessageQueue.CreateAsync{T}"/>
/// (ADR 0007). Retries wait in per-delay retry queues on the broker (ADR 0006).
/// </summary>
public class RabbitMqMessageQueue<T> : IMessageQueue<T>, IRabbitMqConnectionProbe, IDisposable, IAsyncDisposable where T : class
{
    // Library-owned headers. Publishers cannot set them: PublishValidation reserves every "x-" name.
    internal const string DeliveryAttemptHeader = "x-delivery-attempt";
    internal const string DeadLetterReasonHeader = "x-ese-dead-letter-reason";
    internal const string DeadLetterDetailHeader = "x-ese-dead-letter-detail";

    // Set by the broker: quorum queues count deliveries that ended without a settlement (x-delivery-count, RabbitMQ
    // 4.x: consumer crashes and channel closes), and dead-lettering records the queue a message expired from.
    const string BrokerDeliveryCountHeader = "x-delivery-count";
    const string FirstDeathQueueHeader = "x-first-death-queue";

    static readonly MessageQueueCapabilities RabbitMqCapabilities = new(Durable: true, DeadLetter: true, DelayedRedelivery: true);
    static readonly TimeSpan MaxConnectBackoff = TimeSpan.FromSeconds(30);
    static readonly TimeSpan DrainQuietPeriod = TimeSpan.FromMilliseconds(100);

    static readonly TimeSpan CloseGrace = TimeSpan.FromSeconds(1);
    static readonly IReadOnlyDictionary<string, string> EmptyHeaders = new Dictionary<string, string>();

    readonly RabbitMqConfig config;
    readonly ITelemetryProvider telemetry;
    readonly ILogger logger;
    readonly string queueName;
    readonly string exchangeName;
    readonly string deadLetterQueueName;
    readonly string retryQueuePrefix;
    readonly TelemetryTarget publishTarget;
    readonly SemaphoreSlim connectLock = new(1, 1);
    readonly SemaphoreSlim subscriptionLock = new(1, 1);
    readonly ConcurrentDictionary<long, RetryQueue> retryQueues = new();
    readonly BackgroundWork draining = new();
    Connection? connection;
    Subscription? subscription;
    int connectFailures;
    long nextConnectAttempt;
    readonly TransportBackoff transportFailures = new();
    readonly LogThrottle failureLog = new();
    int disposed;

    public RabbitMqMessageQueue(RabbitMqConfig config, ITelemetryProvider? telemetry = null, ILogger? logger = null)
    {
        ConfigValidation.ThrowIfInvalid(config, new RabbitMqConfigValidator());
        this.config = config;
        this.telemetry = telemetry ?? NullTelemetryProvider.Instance;
        this.logger = logger ?? NullLogger.Instance;
        queueName = config.QueueName;
        exchangeName = string.IsNullOrWhiteSpace(config.ExchangeName)
            ? config.QueueName
            : config.ExchangeName;
        deadLetterQueueName = config.EffectiveDeadLetterQueueName;
        retryQueuePrefix = RetryQueues.Prefix(queueName);
        // Semantic conventions: destination "{exchange}:{routing key}" on publish and "{exchange}:{routing key}:{queue}"
        // on consume, collapsed when the routing key is the queue name, as it is here; so both sides join.
        publishTarget = new TelemetryTarget(MessagingSystems.RabbitMq, $"{exchangeName}:{queueName}", typeof(T).Name) { RoutingKey = queueName };

        if (config.MaxConcurrentCalls > config.PrefetchCount)
            RabbitMqLog.ConcurrencyAbovePrefetch(this.logger, queueName, config.MaxConcurrentCalls, config.PrefetchCount);
    }

    public MessageQueueCapabilities Capabilities => RabbitMqCapabilities;

    // Test seams: the consumer channel of the latest subscription, and a fault injected before a delivery is read.
    internal IChannel? LatestSubscriptionChannel { get; private set; }

    internal Action<BasicDeliverEventArgs>? ReadFault { get; set; }

    /// <summary>
    /// Connects and declares the exchange and queues, if that has not happened yet. Optional: the first publish or
    /// subscribe does the same. After a failure, further attempts back off (1 s, doubling, at most 30 s); calls in
    /// between fail fast without dialling.
    /// </summary>
    public async Task InitializeAsync(CancellationToken cancellationToken = default) => await ConnectAsync(cancellationToken);

    // Healthy when the connection is open and, while subscribed, the consumer channel is open and its consumer runs
    // (the broker cancels consumers, for example when their queue is deleted).
    async Task<bool> IRabbitMqConnectionProbe.IsConnectedAsync(CancellationToken cancellationToken)
    {
        var current = await ConnectAsync(cancellationToken);
        if (!current.Amqp.IsOpen)
            return false;
        return Volatile.Read(ref subscription) is not { } active || (active.Channel.IsOpen && active.Consumer.IsRunning);
    }

    async ValueTask<Connection> ConnectAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed) == 1, this);
        if (Volatile.Read(ref connection) is { } existing)
            return existing;

        await connectLock.WaitAsync(cancellationToken);
        try
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed) == 1, this);
            if (connection is { } raced)
                return raced;

            var wait = TimeSpan.FromMilliseconds(nextConnectAttempt - Environment.TickCount64);
            if (wait > TimeSpan.Zero)
                throw new MessageQueueException(
                    $"RabbitMQ for queue '{queueName}' was unreachable {connectFailures} time(s) in a row; the next connection retry is in {wait.TotalSeconds:0.#} s");

            Connection opened;
            try
            {
                opened = await OpenAsync(cancellationToken);
            }
            catch (MessageQueueException ex) when (ex is not ConfigurationException && !cancellationToken.IsCancellationRequested)
            {
                connectFailures++;
                var backoff = TimeSpan.FromSeconds(Math.Min(MaxConnectBackoff.TotalSeconds, Math.Pow(2, Math.Min(connectFailures - 1, 10))));
                nextConnectAttempt = Environment.TickCount64 + (long)backoff.TotalMilliseconds;
                throw;
            }

            connectFailures = 0;
            Volatile.Write(ref connection, opened);
            return opened;
        }
        finally
        {
            connectLock.Release();
        }
    }

    async Task<Connection> OpenAsync(CancellationToken cancellationToken)
    {
        // Invalid settings throw ConfigurationException here, before anything is dialled.
        var factory = RabbitMqConnections.CreateFactory(config);
        IConnection? amqp = null;
        PublisherPool? publishers = null;
        PublisherPool? confirmed = null;
        try
        {
            amqp = await factory.CreateConnectionAsync(cancellationToken);
            await DeclareTopologyAsync(amqp, cancellationToken);
            publishers = await PublisherPool.CreateAsync(amqp, config.PublisherChannelPoolSize, config.EnablePublisherConfirms,
                config.MaxOutstandingConfirms, config.ConfirmTimeout, cancellationToken);
            // Retry and dead-letter copies always need a confirm: the original is acked only after it.
            confirmed = config.EnablePublisherConfirms
                ? publishers
                : await PublisherPool.CreateAsync(amqp, 1, confirms: true, config.MaxOutstandingConfirms, config.ConfirmTimeout, cancellationToken);
            return new Connection(amqp, publishers, confirmed);
        }
        catch (Exception ex)
        {
            await new Connection(amqp, publishers, confirmed).DisposeAsync();
            if (ex is OperationCanceledException && cancellationToken.IsCancellationRequested)
                throw;

            // The client's message can echo endpoint details; the type is enough here, the exception goes to the log.
            RabbitMqLog.ConnectFailed(logger, queueName, ex);
            throw new MessageQueueException(
                $"Failed to connect to RabbitMQ for queue '{queueName}' ({ex.GetType().Name}). See the inner exception and the log for details.", ex);
        }
    }

    async Task DeclareTopologyAsync(IConnection amqp, CancellationToken cancellationToken)
    {
        await using var channel = await amqp.CreateChannelAsync(cancellationToken: cancellationToken);

        await channel.ExchangeDeclareAsync(exchangeName, config.ExchangeType, durable: true, autoDelete: false,
            cancellationToken: cancellationToken);

        var queueArgs = new Dictionary<string, object?>();

        if (config.UseQuorumQueues)
        {
            queueArgs.Add("x-queue-type", "quorum");
            queueArgs.Add("x-quorum-initial-group-size", config.QuorumInitialGroupSize);
        }

        if (config.EnableDeadLetterQueue)
        {
            await DeclareDeadLetterQueueAsync(channel, cancellationToken);

            // The library publishes dead letters itself; the broker uses these only for its own rejections, such as
            // the quorum delivery limit.
            queueArgs["x-dead-letter-exchange"] = "";
            queueArgs["x-dead-letter-routing-key"] = deadLetterQueueName;

            if (config.UseQuorumQueues)
            {
                queueArgs["x-overflow"] = "reject-publish";
            }
        }

        await channel.QueueDeclareAsync(queueName, durable: true, exclusive: false, autoDelete: false,
            arguments: queueArgs, cancellationToken: cancellationToken);

        await channel.QueueBindAsync(queueName, exchangeName, queueName, cancellationToken: cancellationToken);
    }

    async Task DeclareDeadLetterQueueAsync(IChannel channel, CancellationToken cancellationToken)
    {
        var deadLetterQueueArgs = new Dictionary<string, object?>();

        if (config.UseQuorumQueues)
        {
            deadLetterQueueArgs.Add("x-queue-type", "quorum");
            deadLetterQueueArgs.Add("x-quorum-initial-group-size", config.QuorumInitialGroupSize);
        }

        await channel.QueueDeclareAsync(deadLetterQueueName, durable: true, exclusive: false, autoDelete: false,
            arguments: deadLetterQueueArgs, cancellationToken: cancellationToken);
    }

    public Task PublishAsync(T message, CancellationToken cancellationToken = default) =>
        PublishAsync(message, new PublishOptions(), cancellationToken);

    public async Task PublishAsync(T message, PublishOptions options, CancellationToken cancellationToken = default)
    {
        PublishValidation.Validate(message, options);
        var body = Serialize(message);
        var messageId = options.MessageId ?? Guid.CreateVersion7().ToString();
        using var trace = PublishTelemetry.Start(telemetry, publishTarget, 1, logger, messageId);
        try
        {
            var current = await ConnectAsync(cancellationToken);
            await PublishCoreAsync(current, body, options with { MessageId = messageId }, trace.TraceHeaders, cancellationToken);
        }
        catch (Exception ex)
        {
            trace.Failed(ex);
            throw;
        }
    }

    // Publishes every message, then awaits their confirms together (§4.2). At most MaxOutstandingConfirms per
    // publisher channel are in flight; the order in which consumers see them is not guaranteed.
    public async Task PublishAsync(IEnumerable<T> messages, CancellationToken cancellationToken = default)
    {
        PublishValidation.Validate(messages);
        var options = new PublishOptions();
        var bodies = messages.Select(message =>
        {
            PublishValidation.Validate(message, options);
            return Serialize(message);
        }).ToList();
        if (bodies.Count == 0)
            return;

        // One producer span for the whole call; every message carries it as the parent of its consume span.
        using var trace = PublishTelemetry.Start(telemetry, publishTarget, bodies.Count, logger);
        try
        {
            var current = await ConnectAsync(cancellationToken);
            var window = (int)Math.Min(int.MaxValue, (long)config.MaxOutstandingConfirms * config.PublisherChannelPoolSize);
            await BatchPublisher.PublishAllAsync(bodies, window,
                (body, ct) => PublishCoreAsync(current, body, options, trace.TraceHeaders, ct), cancellationToken);
        }
        catch (Exception ex)
        {
            trace.Failed(ex);
            throw;
        }
    }

    async Task PublishCoreAsync(
        Connection current,
        byte[] body,
        PublishOptions options,
        IReadOnlyDictionary<string, string> traceHeaders,
        CancellationToken cancellationToken)
    {
        var properties = new BasicProperties
        {
            Persistent = true,
            MessageId = options.MessageId ?? Guid.CreateVersion7().ToString(),
            Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
            ContentType = MessageSerializer.GetContentType(config.SerializationFormat),
            CorrelationId = options.CorrelationId
        };
        if (options.Headers is { Count: > 0 } || traceHeaders.Count > 0)
        {
            var headers = new Dictionary<string, object?>();
            foreach (var (key, value) in (options.Headers ?? EmptyHeaders).Concat(traceHeaders))
                headers[key] = value;
            properties.Headers = headers;
        }

        try
        {
            await current.Publishers.PublishAsync(exchangeName, queueName, mandatory: false, properties, body, cancellationToken);
        }
        catch (Exception ex) when (ex is not MessageQueueException and not OperationCanceledException)
        {
            throw new MessageQueueException($"Failed to publish message to '{queueName}' ({ex.GetType().Name})", ex);
        }
    }

    byte[] Serialize(T message)
    {
        try
        {
            return MessageSerializer.Serialize(message, config.SerializationFormat);
        }
        catch (Exception ex) when (ex is not MessageQueueException)
        {
            throw new MessageQueueException($"Failed to serialize {typeof(T).Name} for '{queueName}' ({ex.GetType().Name})", ex);
        }
    }

    public Task SubscribeAsync(Func<T, Task<bool>> handler, CancellationToken cancellationToken = default) =>
        SubscribeAsync(MessageHandlers.FromPredicate(handler), cancellationToken);

    // Each subscription gets its own consumer channel: closing it on unsubscribe hands every delivery that is still
    // unacknowledged back to the broker, so a later subscriber receives it.
    public async Task SubscribeAsync(
        Func<T, MessageContext, CancellationToken, Task<MessageResult>> handler,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(handler);
        await subscriptionLock.WaitAsync(cancellationToken);
        try
        {
            if (subscription is not null)
                throw new InvalidOperationException("This queue already has a subscriber. Call UnsubscribeAsync first.");

            var current = await ConnectAsync(cancellationToken);
            IChannel? channel = null;
            try
            {
                // ConsumerDispatchConcurrency bounds the handlers running at once; BasicQos bounds unacked deliveries.
                channel = await current.Amqp.CreateChannelAsync(
                    new CreateChannelOptions(false, false, consumerDispatchConcurrency: (ushort)config.MaxConcurrentCalls),
                    cancellationToken);
                await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: (ushort)config.PrefetchCount, global: false, cancellationToken);

                var consumer = new AsyncEventingBasicConsumer(channel);
                var started = new Subscription(channel, consumer, new MessageDispatcher<T>(config, publishTarget, handler, telemetry, logger));
                consumer.ReceivedAsync += (_, delivery) => OnReceivedAsync(started, delivery);
                started.ConsumerTag = await channel.BasicConsumeAsync(queueName, autoAck: false, consumer, cancellationToken);
                Volatile.Write(ref subscription, started);
                LatestSubscriptionChannel = channel;
            }
            catch (Exception ex)
            {
                if (channel is not null)
                    await CloseQuietlyAsync(channel);
                if (ex is OperationCanceledException && cancellationToken.IsCancellationRequested)
                    throw;
                throw new MessageQueueException($"Failed to subscribe to queue '{queueName}' ({ex.GetType().Name})", ex);
            }
        }
        finally
        {
            subscriptionLock.Release();
        }
    }

    /// <summary>
    /// Stops consuming. Handlers still running get their token cancelled; their deliveries go back to the broker
    /// (nack with requeue). Returns once they finished or <c>ShutdownTimeout</c> passed, whichever is first; handlers
    /// that ignore their token can still settle their delivery afterwards, and the subscription's channel closes
    /// when the last of them finishes. Called from inside a handler, it returns without waiting for that handler.
    /// </summary>
    public async Task UnsubscribeAsync(CancellationToken cancellationToken = default)
    {
        await subscriptionLock.WaitAsync(cancellationToken);
        try
        {
            if (subscription is { } current)
            {
                Volatile.Write(ref subscription, null);
                await StopAsync(current, cancellationToken);
            }
        }
        finally
        {
            subscriptionLock.Release();
        }
    }

    async Task StopAsync(Subscription stopping, CancellationToken cancellationToken)
    {
        await Cancellation.CancelQuietlyAsync(stopping.Stopping, logger, queueName);

        if (IsRunningHandlerOf(stopping))
        {
            // Unsubscribing from inside one of this subscription's handlers: that handler cannot finish while we
            // wait for it, so the rest of the shutdown runs in the background.
            draining.Track(Task.Run(() => DrainAndCloseAsync(stopping, Timeout.InfiniteTimeSpan, CancellationToken.None)));
            return;
        }

        if (!await DrainAndCloseAsync(stopping, config.ShutdownTimeout, cancellationToken))
        {
            // Handlers ignored their token: return now, but keep the channel open so they can still settle their
            // deliveries normally (no requeue racing a late ack or retry), and close it once they are done.
            RabbitMqLog.ShutdownTimedOut(logger, queueName, config.ShutdownTimeout);
            draining.Track(Task.Run(() => DrainAndCloseAsync(stopping, Timeout.InfiniteTimeSpan, CancellationToken.None)));
        }
    }

    // Cancels the consumer, waits until no delivery is in flight (and none arrived for a short quiet period, so
    // prefetched deliveries the client still had queued are requeued too), then closes the channel.
    // Returns false, with the channel still open, if `timeout` passed first.
    async Task<bool> DrainAndCloseAsync(Subscription stopping, TimeSpan timeout, CancellationToken cancellationToken)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        if (timeout != Timeout.InfiniteTimeSpan)
            deadline.CancelAfter(timeout);

        try
        {
            if (Interlocked.Exchange(ref stopping.ConsumerCancelled, 1) == 0 && stopping.ConsumerTag is { } tag && stopping.Channel.IsOpen)
                await stopping.Channel.BasicCancelAsync(tag, noWait: false, deadline.Token);

            while (true)
            {
                var running = stopping.InFlight.Values.ToArray();
                if (running.Length > 0)
                {
                    await Task.WhenAll(running).WaitAsync(deadline.Token);
                    continue;
                }

                await Task.Delay(DrainQuietPeriod, deadline.Token);
                if (stopping.InFlight.IsEmpty)
                    break;
            }
        }
        catch (OperationCanceledException) when (deadline.IsCancellationRequested)
        {
            // Timed out, or the caller stopped waiting: the channel stays open (closing it now would block the
            // connection behind the running handlers) and the caller closes it in the background.
            return false;
        }
        catch (Exception ex)
        {
            RabbitMqLog.CancelFailed(logger, queueName, ex);
        }

        await CloseSubscriptionAsync(stopping);
        return true;
    }

    async Task CloseSubscriptionAsync(Subscription stopping)
    {
        if (Interlocked.Exchange(ref stopping.Closed, 1) == 1)
            return;

        // Closing the channel requeues whatever is still unacknowledged.
        await CloseQuietlyAsync(stopping.Channel);
    }

    static bool IsRunningHandlerOf(Subscription subscription) => HandlerScope.IsRunningHandlerOf(subscription);

    async Task CloseQuietlyAsync(IChannel channel)
    {
        try
        {
            await channel.CloseAsync();
        }
        catch (Exception ex)
        {
            RabbitMqLog.CloseFailed(logger, queueName, ex);
        }
        await channel.DisposeAsync();
    }

    async Task OnReceivedAsync(Subscription current, BasicDeliverEventArgs delivery)
    {
        var settlement = new Settlement(current.Channel, delivery.DeliveryTag, logger, queueName);
        var id = Interlocked.Increment(ref current.NextDeliveryId);
        var work = current.Stopping.IsCancellationRequested
            ? RequeueUnhandledAsync(delivery, settlement)
            : DispatchAsync(current, delivery, settlement);
        current.InFlight[id] = work;
        try
        {
            await work;
        }
        finally
        {
            current.InFlight.TryRemove(id, out _);
        }
    }

    // The body is used in place, without a copy: the client returns its pooled buffer only after this awaited
    // handler completes (RabbitMQ.Client 7 AsyncConsumerDispatcher), and every action that reads it runs inside.
    // Whatever happens, the delivery ends settled: acked, dead-lettered or requeued.
    async Task DispatchAsync(Subscription current, BasicDeliverEventArgs delivery, Settlement settlement)
    {
        var scope = HandlerScope.Enter(current);
        try
        {
            MessageContext context;
            try
            {
                ReadFault?.Invoke(delivery);
                context = ToContext(delivery, retryQueuePrefix, config.UseQuorumQueues);
            }
            catch (Exception ex)
            {
                await DeliveryFailedAsync(current, delivery, settlement, ex);
                return;
            }

            var actions = new RabbitMqDeliveryActions(this, delivery, settlement, context.DeliveryAttempt);
            await current.Dispatcher.DispatchAsync(delivery.Body, context, actions, current.Stopping.Token);
            transportFailures.Reset(FailureKey(delivery));
        }
        catch (OperationCanceledException) when (current.Stopping.IsCancellationRequested)
        {
            // Requeued below: the handler was cancelled by unsubscribe, so the broker redelivers the message.
            RabbitMqLog.DeliveryAbandoned(logger, queueName);
        }
        catch (Exception ex)
        {
            // A transport action failed (broker or channel trouble): back off, then hand the delivery back.
            await BackOffAndRequeueAsync(delivery, settlement, ex, current.Stopping.Token);
        }
        finally
        {
            scope.Exit();
            // Anything not acked, dead-lettered or already requeued goes back to the broker.
            if (!settlement.IsSettled)
                await settlement.RequeueAsync();
        }
    }

    // A delivery that could not even be read: the shared pipeline dead-letters it (DeliveryFailed), or drops it with
    // a warning when dead-lettering is disabled. If that fails too, back off and requeue.
    async Task DeliveryFailedAsync(Subscription current, BasicDeliverEventArgs delivery, Settlement settlement, Exception error)
    {
        try
        {
            var actions = new RabbitMqDeliveryActions(this, delivery, settlement, attempt: 1);
            await current.Dispatcher.RejectUnreadableAsync(delivery.BasicProperties.MessageId ?? string.Empty, actions, error);
        }
        catch (Exception ex)
        {
            await BackOffAndRequeueAsync(delivery, settlement, ex, current.Stopping.Token);
        }
    }

    // Transport failures repeat while the broker or topology is broken: a growing pause per message (TransportBackoff,
    // cut short by unsubscribe) keeps a requeued delivery from coming straight back in a hot loop, and the error log
    // is throttled to one entry per 10 s.
    async Task BackOffAndRequeueAsync(BasicDeliverEventArgs delivery, Settlement settlement, Exception error, CancellationToken stopping)
    {
        var failures = transportFailures.RecordFailure(FailureKey(delivery));
        if (failureLog.ShouldLog(string.Empty, out var suppressed))
            RabbitMqLog.DeliveryFailed(logger, queueName, failures, suppressed, error);
        await TransportBackoff.WaitAsync(failures, stopping);
        await settlement.RequeueAsync();
    }

    // Per message: the message id, which retry and requeue keep (the delivery tag does not survive a requeue).
    static string FailureKey(BasicDeliverEventArgs delivery) => delivery.BasicProperties.MessageId ?? string.Empty;

    // A delivery that reached the client after the subscription stopped was never handled: it goes back without
    // using up an attempt, as a copy through the zero-delay retry queue that keeps its attempt number. If that is
    // not possible, it is nacked with requeue (the broker then marks it redelivered).
    async Task RequeueUnhandledAsync(BasicDeliverEventArgs delivery, Settlement settlement)
    {
        try
        {
            var current = Volatile.Read(ref connection) ?? throw new ObjectDisposedException(GetType().Name);
            var attempt = ToContext(delivery, retryQueuePrefix, config.UseQuorumQueues).DeliveryAttempt;
            await PublishCopyAsync(current, delivery, attempt, bucket: 0);
            await settlement.AckAsync();
        }
        catch (Exception ex)
        {
            RabbitMqLog.UnhandledRequeueFailed(logger, queueName, ex);
            await settlement.RequeueAsync();
        }
    }

    // Maps broker metadata to the portable context. The attempt is the library's header, trusted only on messages
    // that come back from this queue's retry queues, plus redeliveries: the broker's redelivered flag adds one, and on
    // a quorum queue x-delivery-count adds every delivery that ended without a settlement (crashes).
    internal static MessageContext ToContext(BasicDeliverEventArgs delivery, string retryQueuePrefix, bool quorum)
    {
        var properties = delivery.BasicProperties;
        var headers = new Dictionary<string, string>();
        string? traceParent = null;
        string? traceState = null;
        long? libraryAttempt = null;
        long? brokerDeliveryCount = null;
        var fromRetryQueue = false;

        if (properties.Headers is not null)
        {
            foreach (var (key, value) in properties.Headers)
            {
                var isTraceParent = key.Equals(TraceHeaders.TraceParent, StringComparison.OrdinalIgnoreCase);
                if (isTraceParent || key.Equals(TraceHeaders.TraceState, StringComparison.OrdinalIgnoreCase))
                {
                    // Bounded before decoding: any publisher can set these. Names match case-insensitively.
                    var bound = isTraceParent ? TraceContext.MaxTraceParentLength : TraceContext.MaxTraceStateLength;
                    var trace = value is byte[] { Length: var length } && length > bound ? null : HeaderText(value);
                    trace = trace is { Length: > 0 } && trace.Length <= bound ? trace : null;
                    if (isTraceParent)
                        traceParent = trace;
                    else
                        traceState = trace;
                    continue;
                }

                var text = HeaderText(value);
                if (text is null)
                    continue;

                if (key == DeliveryAttemptHeader)
                    libraryAttempt = long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) && parsed > 0 ? parsed : 1;
                else if (key == BrokerDeliveryCountHeader)
                    brokerDeliveryCount = long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var count) && count > 0 ? count : 0;
                else if (key == FirstDeathQueueHeader)
                    fromRetryQueue = text.StartsWith(retryQueuePrefix, StringComparison.Ordinal);
                else if (!PublishValidation.IsReserved(key))
                    headers[key] = text;
            }
        }

        var attempt = fromRetryQueue ? libraryAttempt ?? 1 : 1;
        // x-delivery-count is the broker's only on quorum queues; on a classic queue only a publisher could set it.
        if (delivery.Redelivered)
            attempt += quorum ? Math.Max(1, brokerDeliveryCount ?? 0) : 1;

        return new MessageContext
        {
            MessageId = properties.MessageId ?? string.Empty,
            CorrelationId = properties.CorrelationId,
            Timestamp = ToTimestamp(properties.Timestamp),
            DeliveryAttempt = (int)Math.Min(attempt, int.MaxValue),
            Headers = headers,
            TraceParent = traceParent,
            TraceState = traceState
        };
    }

    // AMQP timestamps are any 64-bit number; one outside DateTimeOffset's range is left unset.
    static DateTimeOffset ToTimestamp(AmqpTimestamp timestamp) =>
        timestamp.UnixTime >= DateTimeOffset.MinValue.ToUnixTimeSeconds() && timestamp.UnixTime <= DateTimeOffset.MaxValue.ToUnixTimeSeconds()
            ? DateTimeOffset.FromUnixTimeSeconds(timestamp.UnixTime)
            : default;

    static string? HeaderText(object? value) => value switch
    {
        byte[] bytes => Encoding.UTF8.GetString(bytes),
        string s => s,
        IConvertible convertible => convertible.ToString(CultureInfo.InvariantCulture),
        _ => null
    };

    // Copies the message for a retry or dead-letter publish: application and trace headers survive, broker, client,
    // foreign "x-" and earlier library headers (x-death, x-delivery-count, sequence numbers, old reasons) do not.
    internal static BasicProperties CopyProperties(IReadOnlyBasicProperties original, int attempt)
    {
        var headers = new Dictionary<string, object?>();
        if (original.Headers is not null)
        {
            foreach (var (key, value) in original.Headers)
            {
                if (!key.StartsWith("x-", StringComparison.OrdinalIgnoreCase))
                    headers[key] = value;
            }
        }
        headers[DeliveryAttemptHeader] = attempt.ToString(CultureInfo.InvariantCulture);

        return new BasicProperties
        {
            Persistent = true,
            MessageId = original.MessageId,
            CorrelationId = original.CorrelationId,
            Timestamp = original.Timestamp,
            ContentType = original.ContentType,
            Headers = headers
        };
    }

    // Retry queues are declared lazily and cached. Classic ones expire when unused: a cache entry older than a quarter
    // of x-expires is declared again, which resets the broker's expiry timer, so a message published into the queue
    // always expires (TTL <= expires / 4) long before the queue could. Quorum ones never expire (ADR 0006).
    async Task<string> RetryQueueAsync(Connection current, long bucketMilliseconds, bool redeclare = false)
    {
        var now = Environment.TickCount64;
        var entry = retryQueues.AddOrUpdate(bucketMilliseconds,
            ms => new RetryQueue(DeclareRetryQueueAsync(current, ms), now),
            (ms, cached) => redeclare || cached.Declared.IsFaulted
                || (!config.UseQuorumQueues && RetryQueues.IsStale(cached.DeclaredAt, now, ms, config.RetryQueueExpiresOverride))
                ? new RetryQueue(DeclareRetryQueueAsync(current, ms), now)
                : cached);
        try
        {
            return await entry.Declared;
        }
        catch
        {
            retryQueues.TryRemove(new KeyValuePair<long, RetryQueue>(bucketMilliseconds, entry));
            throw;
        }
    }

    async Task<string> DeclareRetryQueueAsync(Connection current, long bucketMilliseconds)
    {
        var name = RetryQueues.Name(queueName, bucketMilliseconds);
        await using var channel = await current.Amqp.CreateChannelAsync();
        await channel.QueueDeclareAsync(name, durable: true, exclusive: false, autoDelete: false,
            arguments: RetryQueues.Arguments(config, bucketMilliseconds));
        return name;
    }

    async Task RedeclareDeadLetterQueueAsync(Connection current)
    {
        await using var channel = await current.Amqp.CreateChannelAsync();
        await DeclareDeadLetterQueueAsync(channel, CancellationToken.None);
    }

    // Retry and dead-letter copies go through the default exchange straight to their queue (other queues bound to
    // a custom exchange must not see them), mandatory so a missing queue fails instead of silently dropping, and
    // confirmed before the original is acked. Bounded by ConfirmTimeout, not by the stopping token: a retry already
    // on its way is finished rather than duplicated. A refused copy (the queue was deleted) is retried once after
    // declaring its queue again.
    async Task PublishCopyAsync(Connection current, BasicDeliverEventArgs delivery, int attempt, long? bucket, DeadLetterReason? reason = null)
    {
        // Built per publish: confirmation tracking adds its own header to the properties it is given.
        BasicProperties Properties()
        {
            var properties = CopyProperties(delivery.BasicProperties, attempt);
            if (reason is { } deadLetter)
            {
                properties.Headers![DeadLetterReasonHeader] = deadLetter.Code;
                if (deadLetter.Detail is not null)
                    properties.Headers[DeadLetterDetailHeader] = deadLetter.Detail;
            }
            return properties;
        }

        var target = bucket is { } ms ? await RetryQueueAsync(current, ms) : deadLetterQueueName;
        try
        {
            await current.Confirmed.PublishAsync(string.Empty, target, mandatory: true, Properties(), delivery.Body, CancellationToken.None);
        }
        catch (MessageQueueException error) when (IsSafeToRepublish(error))
        {
            // Only a nack or return proves the copy was not stored. After a confirm timeout it may have been, so
            // the delivery is requeued instead (at-least-once: the copy may then exist twice).
            if (bucket is { } retryBucket)
                await RetryQueueAsync(current, retryBucket, redeclare: true);
            else
                await RedeclareDeadLetterQueueAsync(current);

            await current.Confirmed.PublishAsync(string.Empty, target, mandatory: true, Properties(), delivery.Body, CancellationToken.None);
        }
    }

    // Only a nack or return proves a copy was not stored; after a confirm timeout it may have been.
    internal static bool IsSafeToRepublish(Exception error) => error is PublishRefusedException;

    // Settling a delivery uses the connection it arrived on, also while the queue is being disposed (stragglers).
    async ValueTask<Connection> DeliveryConnectionAsync() =>
        Volatile.Read(ref connection) ?? await ConnectAsync(CancellationToken.None);

    async Task RescheduleAsync(BasicDeliverEventArgs delivery, Settlement settlement, TimeSpan delay, int nextAttempt)
    {
        var current = await DeliveryConnectionAsync();
        var bucket = RetryQueues.BucketMilliseconds(config.Retry, nextAttempt - 1, delay);
        await PublishCopyAsync(current, delivery, nextAttempt, bucket);
        await settlement.AckAsync();
    }

    async Task DeadLetterAsync(BasicDeliverEventArgs delivery, Settlement settlement, DeadLetterReason reason, int attempt)
    {
        var current = await DeliveryConnectionAsync();
        await PublishCopyAsync(current, delivery, attempt, bucket: null, reason);
        await settlement.AckAsync();
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref disposed, 1) == 1)
            return;

        if (Volatile.Read(ref subscription) is { } active && IsRunningHandlerOf(active))
        {
            // Disposing from inside one of this queue's handlers: finish in the background, after that handler.
            await Cancellation.CancelQuietlyAsync(active.Stopping, logger, queueName);
            _ = Task.Run(DisposeCoreAsync);
            return;
        }

        await DisposeCoreAsync();
    }

    async Task DisposeCoreAsync()
    {
        // Runs on its own flow (possibly started from inside a handler): it is not that handler.
        HandlerScope.Clear();

        // One ShutdownTimeout for every handler, whether still subscribed or already draining. After it, the
        // connection is closed regardless (plus at most a second's grace for the close itself): the broker requeues
        // whatever was not settled, so a straggler's late ack is lost and the message is redelivered.
        using var deadline = new CancellationTokenSource(config.ShutdownTimeout);
        await subscriptionLock.WaitAsync();
        try
        {
            if (subscription is { } current)
            {
                Volatile.Write(ref subscription, null);
                await StopAsync(current, deadline.Token);
            }
        }
        finally
        {
            subscriptionLock.Release();
        }

        try
        {
            await draining.WhenAll().WaitAsync(deadline.Token);
        }
        catch (OperationCanceledException)
        {
            RabbitMqLog.ShutdownTimedOut(logger, queueName, config.ShutdownTimeout);
        }

        await connectLock.WaitAsync();
        try
        {
            if (connection is { } open)
            {
                Volatile.Write(ref connection, null);
                var closing = open.DisposeAsync().AsTask();
                try
                {
                    await closing.WaitAsync(CloseGrace);
                }
                catch (TimeoutException)
                {
                    // Closing waits for hung handlers inside the client; let it finish on its own.
                    _ = closing.ContinueWith(done => done.Exception, TaskScheduler.Default);
                    RabbitMqLog.CloseAbandoned(logger, queueName);
                }
            }
        }
        finally
        {
            connectLock.Release();
        }

        GC.SuppressFinalize(this);
    }

    // RabbitMQ.Client 7 is async-only; hosts dispose singletons through DisposeAsync. This blocking path is for
    // callers that only know IDisposable.
    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    sealed record RetryQueue(Task<string> Declared, long DeclaredAt);

    sealed class Connection : IAsyncDisposable
    {
        public Connection(IConnection? amqp, PublisherPool? publishers, PublisherPool? confirmed)
        {
            Amqp = amqp!;
            Publishers = publishers!;
            Confirmed = confirmed!;
        }

        public IConnection Amqp { get; }
        public PublisherPool Publishers { get; }
        public PublisherPool Confirmed { get; }

        public async ValueTask DisposeAsync()
        {
            if (Confirmed is not null && !ReferenceEquals(Confirmed, Publishers))
                await Confirmed.DisposeAsync();
            if (Publishers is not null)
                await Publishers.DisposeAsync();
            if (Amqp is not null)
            {
                try
                {
                    await Amqp.CloseAsync();
                }
                catch (Exception)
                {
                    // Already closed or unreachable; DisposeAsync below releases the socket either way.
                }
                await Amqp.DisposeAsync();
            }
        }
    }

    sealed class Subscription
    {
        public Subscription(IChannel channel, AsyncEventingBasicConsumer consumer, MessageDispatcher<T> dispatcher)
        {
            Channel = channel;
            Consumer = consumer;
            Dispatcher = dispatcher;
        }

        public IChannel Channel { get; }
        public AsyncEventingBasicConsumer Consumer { get; }
        public MessageDispatcher<T> Dispatcher { get; }
        public CancellationTokenSource Stopping { get; } = new();
        public ConcurrentDictionary<long, Task> InFlight { get; } = new();
        public string? ConsumerTag { get; set; }
        public long NextDeliveryId;
        public int ConsumerCancelled;
        public int Closed;
    }

    sealed class RabbitMqDeliveryActions : IDeliveryActions
    {
        readonly RabbitMqMessageQueue<T> queue;
        readonly BasicDeliverEventArgs delivery;
        readonly Settlement settlement;
        readonly int attempt;

        public RabbitMqDeliveryActions(RabbitMqMessageQueue<T> queue, BasicDeliverEventArgs delivery, Settlement settlement, int attempt)
        {
            this.queue = queue;
            this.delivery = delivery;
            this.settlement = settlement;
            this.attempt = attempt;
        }

        public Task CompleteAsync(CancellationToken cancellationToken) => settlement.AckAsync();

        // Broker-side backoff: a copy waits in the retry queue for the delay's bucket, then returns to this queue.
        public Task RescheduleAsync(TimeSpan delay, int nextAttempt, CancellationToken cancellationToken) =>
            queue.RescheduleAsync(delivery, settlement, delay, nextAttempt);

        public Task DeadLetterAsync(DeadLetterReason reason, CancellationToken cancellationToken) =>
            queue.DeadLetterAsync(delivery, settlement, reason, attempt);
    }
}

/// <summary>Factory for <see cref="RabbitMqMessageQueue{T}"/>.</summary>
public static class RabbitMqMessageQueue
{
    /// <summary>Creates the queue and connects, so connection and topology errors surface here.</summary>
    public static async Task<RabbitMqMessageQueue<T>> CreateAsync<T>(
        RabbitMqConfig config,
        ITelemetryProvider? telemetry = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default) where T : class
    {
        var queue = new RabbitMqMessageQueue<T>(config, telemetry, logger);
        try
        {
            await queue.InitializeAsync(cancellationToken);
            return queue;
        }
        catch
        {
            await queue.DisposeAsync();
            throw;
        }
    }
}

// Lets the RabbitMQ health check reuse a queue's connection instead of opening its own.
interface IRabbitMqConnectionProbe
{
    Task<bool> IsConnectedAsync(CancellationToken cancellationToken);
}

static partial class RabbitMqLog
{
    [LoggerMessage(EventId = 2001, Level = LogLevel.Error,
        Message = "Delivery from RabbitMQ queue {QueueName} could not be settled ({ConsecutiveFailures} failure(s) in a row, {SuppressedLogs} similar error(s) not logged); requeued after a backoff")]
    public static partial void DeliveryFailed(ILogger logger, string queueName, int consecutiveFailures, int suppressedLogs, Exception exception);

    [LoggerMessage(EventId = 2002, Level = LogLevel.Debug, Message = "Delivery from RabbitMQ queue {QueueName} was interrupted by shutdown and requeued for redelivery")]
    public static partial void DeliveryAbandoned(ILogger logger, string queueName);

    [LoggerMessage(EventId = 2003, Level = LogLevel.Error, Message = "Could not connect to RabbitMQ for queue {QueueName}")]
    public static partial void ConnectFailed(ILogger logger, string queueName, Exception exception);

    [LoggerMessage(EventId = 2004, Level = LogLevel.Warning,
        Message = "RabbitMQ queue {QueueName}: handlers did not finish within {ShutdownTimeout}; their channel stays open until they do")]
    public static partial void ShutdownTimedOut(ILogger logger, string queueName, TimeSpan shutdownTimeout);

    [LoggerMessage(EventId = 2005, Level = LogLevel.Warning, Message = "RabbitMQ queue {QueueName}: could not requeue a delivery; the broker requeues it when the channel closes")]
    public static partial void RequeueFailed(ILogger logger, string queueName, Exception exception);

    [LoggerMessage(EventId = 2006, Level = LogLevel.Warning, Message = "RabbitMQ queue {QueueName}: cancelling the consumer failed")]
    public static partial void CancelFailed(ILogger logger, string queueName, Exception exception);

    [LoggerMessage(EventId = 2007, Level = LogLevel.Debug, Message = "RabbitMQ queue {QueueName}: closing a channel failed")]
    public static partial void CloseFailed(ILogger logger, string queueName, Exception exception);

    [LoggerMessage(EventId = 2009, Level = LogLevel.Debug, Message = "RabbitMQ queue {QueueName}: an unhandled delivery could not be returned without using an attempt; requeued instead")]
    public static partial void UnhandledRequeueFailed(ILogger logger, string queueName, Exception exception);

    [LoggerMessage(EventId = 2011, Level = LogLevel.Warning,
        Message = "RabbitMQ queue {QueueName}: the connection did not close in time (a handler is still running); it closes in the background")]
    public static partial void CloseAbandoned(ILogger logger, string queueName);

    [LoggerMessage(EventId = 2010, Level = LogLevel.Warning,
        Message = "RabbitMQ queue {QueueName}: MaxConcurrentCalls ({MaxConcurrentCalls}) is above PrefetchCount ({PrefetchCount}), so at most PrefetchCount handlers run at once")]
    public static partial void ConcurrencyAbovePrefetch(ILogger logger, string queueName, int maxConcurrentCalls, int prefetchCount);
}
