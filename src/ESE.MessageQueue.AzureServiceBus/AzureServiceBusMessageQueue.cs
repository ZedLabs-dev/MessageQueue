using System.Collections.Concurrent;
using System.Diagnostics;
using System.Transactions;
using Azure.Core;
using Azure.Messaging.ServiceBus;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Configuration;
using ESE.MessageQueue.Exceptions;
using ESE.MessageQueue.Processing;
using ESE.MessageQueue.Providers;
using ESE.MessageQueue.Serialization;
using ESE.MessageQueue.Telemetry;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace ESE.MessageQueue.AzureServiceBus;

/// <summary>
/// Azure Service Bus provider. Consumes a queue, or a topic subscription when <c>TopicName</c> is set. Retries on a
/// queue are scheduled copies (broker-side backoff); on a subscription they wait under the message lock and are
/// abandoned (ADR 0009). Shutdown is bounded by <c>ShutdownTimeout</c> (ADR 0010). The client connects on first use.
/// </summary>
public sealed class AzureServiceBusMessageQueue<T> : IMessageQueue<T>, IDeadLetterQueue<T>, IAzureServiceBusProbe, IDisposable, IAsyncDisposable where T : class
{
    // Service Bus's default MaxDeliveryCount. The entity's real value needs Manage rights to read, so it is assumed.
    internal const int DefaultEntityMaxDeliveryCount = 10;

    static readonly TimeSpan CloseGrace = TimeSpan.FromSeconds(1);

    // Safety margin when deciding whether a subscription retry can wait under the message lock.
    static readonly TimeSpan LockMargin = TimeSpan.FromSeconds(2);

    // Above this many concurrent handlers, synchronous transaction commits can noticeably tie up thread-pool threads.
    internal const int TransactionalConcurrencyWarningThreshold = 100;

    readonly AzureServiceBusConfig config;
    readonly ITelemetryProvider telemetry;
    readonly ILogger logger;
    readonly IServiceBusTransport transport;
    readonly IServiceBusSender sender;
    readonly MessageQueueCapabilities capabilities;
    readonly DeadLetterQueue<T> deadLetterQueue;
    readonly bool subscriptionMode;
    readonly string publishEntity;
    readonly TelemetryTarget publishTarget;
    readonly TelemetryTarget consumeTarget;
    readonly SemaphoreSlim subscriptionLock = new(1, 1);
    readonly BackgroundWork draining = new();
    readonly TransportBackoff transportFailures = new();
    readonly LogThrottle errorLog = new();
    readonly object peekerLock = new();
    IServiceBusPeeker? peeker;
    Subscription? subscription;
    int disposed;
    int transactionsRefused;

    /// <summary>
    /// Creates the queue after validating <paramref name="config"/> like startup validation. With
    /// <c>FullyQualifiedNamespace</c> it authenticates with <c>DefaultAzureCredential</c>. Thread-safe once created.
    /// </summary>
    public AzureServiceBusMessageQueue(AzureServiceBusConfig config, ITelemetryProvider? telemetry = null, ILogger? logger = null)
        : this(config, null, telemetry, logger, ServiceBusTransportFactory.Instance)
    {
    }

    /// <summary>
    /// Uses <paramref name="credential"/> with <c>FullyQualifiedNamespace</c> (Entra ID). Without a credential,
    /// a namespace is reached with <c>DefaultAzureCredential</c>.
    /// </summary>
    public AzureServiceBusMessageQueue(AzureServiceBusConfig config, TokenCredential credential, ITelemetryProvider? telemetry = null, ILogger? logger = null)
        : this(config, credential, telemetry, logger, ServiceBusTransportFactory.Instance)
    {
    }

    internal AzureServiceBusMessageQueue(
        AzureServiceBusConfig config,
        TokenCredential? credential,
        ITelemetryProvider? telemetry,
        ILogger? logger,
        IServiceBusTransportFactory transportFactory)
    {
        ConfigValidation.ThrowIfInvalid(config, new AzureServiceBusConfigValidator());
        this.config = config;
        this.telemetry = telemetry ?? NullTelemetryProvider.Instance;
        this.logger = new SafeLogger(logger ?? NullLogger.Instance);
        subscriptionMode = config.UsesSubscription;
        publishEntity = subscriptionMode ? config.TopicName! : config.QueueName;
        // Publish and consume name the same entity (the topic in subscription mode), so their telemetry joins.
        publishTarget = new TelemetryTarget(MessagingSystems.ServiceBus, publishEntity, typeof(T).Name) { PublishOperation = "send" };
        consumeTarget = publishTarget with { Subscription = subscriptionMode ? config.SubscriptionName : null };
        capabilities = new MessageQueueCapabilities(Durable: true, DeadLetter: true, DelayedRedelivery: config.SupportsDelayedRedelivery)
        {
            DeadLetterOperations = ServiceBusDeadLetterStore.OperationsFor(config)
        };

        CapabilityChecks.WarnIfBackoffUnsupported(capabilities, config, this.logger);
        if (subscriptionMode && config.Retry.MaxRetries + 1 > DefaultEntityMaxDeliveryCount)
            AzureServiceBusLog.RetriesExceedMaxDeliveryCount(this.logger, config.QueueName, config.Retry.MaxRetries + 1, DefaultEntityMaxDeliveryCount);
        if (subscriptionMode && config.PrefetchCount > 0)
            AzureServiceBusLog.SubscriptionPrefetches(this.logger, config.QueueName, config.PrefetchCount);
        if (!subscriptionMode && config.TransactionalRetries && config.MaxConcurrentCalls > TransactionalConcurrencyWarningThreshold)
            AzureServiceBusLog.TransactionalConcurrency(this.logger, config.QueueName, config.MaxConcurrentCalls, TransactionalConcurrencyWarningThreshold);

        if (credential is not null && string.IsNullOrWhiteSpace(config.FullyQualifiedNamespace))
            throw new ConfigurationException(
                "A TokenCredential was supplied, but the configuration uses ConnectionString; set FullyQualifiedNamespace to use the credential");

        try
        {
            transport = transportFactory.Create(config, credential);
            sender = transport.CreateSender(publishEntity);
        }
        catch (Exception ex)
        {
            // S10: the client's own message can echo the connection string, so neither its text nor the exception
            // itself is passed on or logged; the type is enough to diagnose.
            AzureServiceBusLog.ClientCreationFailed(this.logger, config.QueueName, BoundedText.TypeName(ex));
            throw new MessageQueueException(
                $"Failed to create the Azure Service Bus client for '{config.QueueName}' ({ex.GetType().Name}). Check ConnectionString or FullyQualifiedNamespace.");
        }

        deadLetterQueue = new DeadLetterQueue<T>(
            new ServiceBusDeadLetterStore(transport, sender, config, this.logger), config, consumeTarget, this.telemetry, this.logger);
    }

    /// <summary>
    /// Durable, with the entity's dead-letter subqueue; delayed redelivery on queues (scheduled copies), not on
    /// subscriptions (retries wait under the message lock).
    /// </summary>
    public MessageQueueCapabilities Capabilities => capabilities;

    string ConsumeEntity => subscriptionMode ? config.TopicName! : config.QueueName;

    string? ConsumeSubscription => subscriptionMode ? config.SubscriptionName : null;

    /// <inheritdoc />
    public Task PublishAsync(T message, CancellationToken cancellationToken = default) =>
        PublishAsync(message, new PublishOptions(), cancellationToken);

    /// <inheritdoc />
    public async Task PublishAsync(T message, PublishOptions options, CancellationToken cancellationToken = default)
    {
        PublishValidation.Validate(message, options);
        ThrowIfDisposed();
        var serviceBusMessage = CreateMessage(message, options);
        using var trace = PublishTelemetry.Start(telemetry, publishTarget, 1, logger, serviceBusMessage.MessageId);
        AddTraceHeaders(serviceBusMessage, trace);
        try
        {
            await SendAsync(token => sender.SendAsync(serviceBusMessage, token), cancellationToken);
        }
        catch (Exception ex)
        {
            trace.Failed(ex);
            throw;
        }
    }

    /// <summary>
    /// Publishes in batches built with <c>CreateMessageBatchAsync</c>, each within the entity's byte limit and at most
    /// <c>MaxBatchSize</c> messages. Batches are sent one after another and are not atomic together: if one fails,
    /// the batches before it stay sent.
    /// </summary>
    public async Task PublishAsync(IEnumerable<T> messages, CancellationToken cancellationToken = default)
    {
        PublishValidation.Validate(messages);
        ThrowIfDisposed();
        var options = new PublishOptions();
        var pending = messages.Select(message =>
        {
            PublishValidation.Validate(message, options);
            return CreateMessage(message, options);
        }).ToList();
        if (pending.Count == 0)
            return;

        // One producer span for the whole call; every message carries it as the parent of its consume span.
        using var trace = PublishTelemetry.Start(telemetry, publishTarget, pending.Count, logger);
        foreach (var each in pending)
            AddTraceHeaders(each, trace);
        try
        {
            await PublishPendingAsync(pending, cancellationToken);
        }
        catch (Exception ex)
        {
            trace.Failed(ex);
            throw;
        }
    }

    async Task PublishPendingAsync(List<ServiceBusMessage> pending, CancellationToken cancellationToken)
    {
        if (!config.EnableBatchedOperations)
        {
            foreach (var single in pending)
                await SendAsync(token => sender.SendAsync(single, token), cancellationToken);
            return;
        }

        var next = 0;
        while (next < pending.Count)
        {
            await SendAsync(async token =>
            {
                using var batch = await sender.CreateBatchAsync(token);
                while (next < pending.Count && batch.Count < config.MaxBatchSize && batch.TryAddMessage(pending[next]))
                    next++;

                if (batch.Count == 0)
                    throw new MessageQueueException(
                        $"The message at index {next} is too large for an Azure Service Bus batch on '{publishEntity}' (at most {batch.MaxSizeInBytes} bytes); the messages before it were sent");

                await sender.SendBatchAsync(batch, token);
            }, cancellationToken);
        }
    }

    // One send, bounded by SendTimeout. Client failures name only their type (S10: client text can name the namespace).
    async Task SendAsync(Func<CancellationToken, Task> send, CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(config.SendTimeout);
        try
        {
            await send(timeout.Token);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new MessageQueueException($"Sending to '{publishEntity}' timed out after {config.SendTimeout}");
        }
        catch (Exception ex) when (ex is not MessageQueueException and not OperationCanceledException)
        {
            throw new MessageQueueException($"Failed to publish to '{publishEntity}' ({ex.GetType().Name})", ex);
        }
    }

    static void AddTraceHeaders(ServiceBusMessage message, PublishTelemetry trace)
    {
        foreach (var (key, value) in trace.TraceHeaders)
            message.ApplicationProperties[key] = value;
    }

    ServiceBusMessage CreateMessage(T message, PublishOptions options)
    {
        byte[] body;
        try
        {
            body = MessageSerializer.Serialize(message, config.SerializationFormat, config.JsonTypeInfoResolver);
        }
        catch (Exception ex) when (ex is not MessageQueueException)
        {
            throw new MessageQueueException($"Failed to serialize {typeof(T).Name} for '{publishEntity}' ({ex.GetType().Name})", ex);
        }

        var serviceBusMessage = new ServiceBusMessage(body)
        {
            MessageId = options.MessageId ?? Guid.CreateVersion7().ToString(),
            CorrelationId = options.CorrelationId,
            ContentType = MessageSerializer.GetContentType(config.SerializationFormat),
            TimeToLive = config.MessageTimeToLive,
            Subject = typeof(T).Name
        };

        if (options.Headers is not null)
        {
            foreach (var (key, value) in options.Headers)
                serviceBusMessage.ApplicationProperties[key] = value;
        }

        return serviceBusMessage;
    }

    /// <summary>
    /// Browses the entity's dead-letter subqueue (the queue's, or the subscription's) without locking it (ADR 0023).
    /// Needs Listen rights.
    /// </summary>
    /// <inheritdoc />
    public Task<IReadOnlyList<DeadLetterMessage<T>>> PeekAsync(int maxCount, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        return deadLetterQueue.PeekAsync(maxCount, cancellationToken);
    }

    /// <summary>
    /// Queue mode only: sends each selected dead letter back to the queue as a copy (a new broker message id, the
    /// original id in <see cref="MessageContext.MessageId"/>, attempt 1), then completes the dead letter, in one transaction
    /// while <c>TransactionalRetries</c> is on and the entity accepts it, else send first (ADR 0023). Needs Send and Listen.
    /// </summary>
    /// <inheritdoc />
    /// <exception cref="NotSupportedException">The queue consumes a topic subscription: a copy sent to the topic would
    /// reach every subscription.</exception>
    public Task<DeadLetterReplayResult> ReplayAsync(DeadLetterReplayOptions<T> options, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (subscriptionMode)
            throw new NotSupportedException(
                $"Replaying the dead letters of subscription '{config.SubscriptionName}' is not supported: a copy sent to topic '{config.TopicName}' " +
                "would reach every subscription. Peek or purge them, or resubmit them with a tool that targets this subscription only.");
        return deadLetterQueue.ReplayAsync(options, cancellationToken);
    }

    /// <summary>Completes (deletes) the selected messages of the entity's dead-letter subqueue (ADR 0023). Needs Listen rights.</summary>
    /// <inheritdoc />
    public Task<int> PurgeAsync(DeadLetterPurgeOptions<T> options, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        return deadLetterQueue.PurgeAsync(options, cancellationToken);
    }

    /// <inheritdoc />
    public Task SubscribeAsync(Func<T, Task<bool>> handler, CancellationToken cancellationToken = default) =>
        SubscribeAsync(MessageHandlers.FromPredicate(handler), cancellationToken);

    // Each subscription gets its own processor: the SDK cannot replace a processor's handlers, and disposing it on
    // unsubscribe makes a later SubscribeAsync start from a clean state (B6).
    /// <inheritdoc />
    public async Task SubscribeAsync(
        Func<T, MessageContext, CancellationToken, Task<MessageResult>> handler,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(handler);
        ThrowIfDisposed();
        await subscriptionLock.WaitAsync(cancellationToken);
        try
        {
            // Dispose may have run while this call waited for the lock.
            ThrowIfDisposed();
            if (subscription is not null)
                throw new InvalidOperationException("This queue already has a subscriber. Call UnsubscribeAsync first.");

            IServiceBusProcessor? processor = null;
            try
            {
                processor = transport.CreateProcessor(ConsumeEntity, ConsumeSubscription, new ServiceBusProcessorOptions
                {
                    AutoCompleteMessages = false,
                    MaxConcurrentCalls = config.MaxConcurrentCalls,
                    PrefetchCount = config.PrefetchCount,
                    ReceiveMode = ServiceBusReceiveMode.PeekLock,
                    MaxAutoLockRenewalDuration = config.MaxAutoLockRenewalDuration
                });
                var started = new Subscription(processor, new MessageDispatcher<T>(config, consumeTarget, handler, telemetry, logger));
                await processor.StartAsync(delivery => OnMessageAsync(started, delivery), OnErrorAsync, cancellationToken);
                Volatile.Write(ref subscription, started);
            }
            catch (Exception ex)
            {
                if (processor is not null)
                    await DisposeQuietlyAsync(processor);
                if (ex is OperationCanceledException && cancellationToken.IsCancellationRequested)
                    throw;
                throw new MessageQueueException($"Failed to subscribe to '{ConsumeEntity}' ({ex.GetType().Name})", ex);
            }
        }
        finally
        {
            subscriptionLock.Release();
        }
    }

    /// <summary>
    /// Stops consuming. Running handlers get their token cancelled and their messages are abandoned for redelivery
    /// (without using up an attempt). Returns once they finished or <c>ShutdownTimeout</c> passed, whichever is first;
    /// handlers that ignore their token can still settle their message afterwards, and the processor is disposed when
    /// the last of them finishes. Called from inside a handler, it returns without waiting for that handler.
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
        await Cancellation.CancelQuietlyAsync(stopping.Stopping, logger, config.QueueName);

        if (HandlerScope.IsRunningHandlerOf(stopping))
        {
            // From inside one of this subscription's handlers: StopProcessingAsync waits for that handler, so the
            // rest of the shutdown runs in the background.
            draining.Track(Task.Run(() => DrainAndCloseAsync(stopping), CancellationToken.None));
            return;
        }

        var drain = DrainAndCloseAsync(stopping);
        try
        {
            await drain.WaitAsync(config.ShutdownTimeout, cancellationToken);
        }
        catch (Exception ex) when (ex is TimeoutException || (ex is OperationCanceledException && cancellationToken.IsCancellationRequested))
        {
            // Handlers ignored their token (or the caller stopped waiting): let them finish and settle normally.
            AzureServiceBusLog.ShutdownTimedOut(logger, config.QueueName, config.ShutdownTimeout);
            draining.Track(drain);
        }
    }

    // Stops the processor (which waits for its running handlers), waits for this subscription's own in-flight work,
    // then disposes the processor. Never throws.
    async Task DrainAndCloseAsync(Subscription stopping)
    {
        try
        {
            await stopping.Processor.StopAsync(CancellationToken.None);
        }
        catch (Exception ex)
        {
            AzureServiceBusLog.StopFailed(logger, config.QueueName, ex);
        }

        while (stopping.InFlight.Values.ToArray() is { Length: > 0 } running)
            await Task.WhenAll(running);

        await DisposeQuietlyAsync(stopping.Processor);
    }

    async Task DisposeQuietlyAsync(IAsyncDisposable resource)
    {
        try
        {
            await resource.DisposeAsync();
        }
        catch (Exception ex)
        {
            AzureServiceBusLog.CloseFailed(logger, config.QueueName, ex);
        }
    }

    async Task OnMessageAsync(Subscription current, IServiceBusDelivery delivery)
    {
        var id = Interlocked.Increment(ref current.NextDeliveryId);
        var work = current.Stopping.IsCancellationRequested
            ? ReturnUnhandledAsync(delivery)
            : HandleAsync(current, delivery);
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

    // Whatever happens, the message ends settled exactly once (completed, dead-lettered, rescheduled or abandoned),
    // or is left to its lock expiry when the lock is gone. Nothing escapes to the processor.
    async Task HandleAsync(Subscription current, IServiceBusDelivery delivery)
    {
        var scope = HandlerScope.Enter(current);
        var received = Stopwatch.GetTimestamp();
        var message = delivery.Message;
        var settlement = new ServiceBusSettlement(delivery, config.SendTimeout);
        using var stopping = CancellationTokenSource.CreateLinkedTokenSource(current.Stopping.Token, delivery.CancellationToken);
        MessageContext? context = null;
        try
        {
            ReadOnlyMemory<byte> body;
            try
            {
                context = ServiceBusMessages.ToContext(message);
                body = message.Body.ToMemory();
            }
            catch (Exception ex)
            {
                await RejectUnreadableAsync(current, delivery, settlement, ex, stopping.Token);
                return;
            }

            var actions = new DeliveryActions(this, delivery, settlement, context, received);
            await current.Dispatcher.DispatchAsync(body, context, actions, stopping.Token);
            transportFailures.Reset(SafeMessageId(message));
        }
        catch (OperationCanceledException) when (stopping.IsCancellationRequested && !settlement.IsSettled)
        {
            // Cancelled by shutdown (or the processor): hand it back without counting this as a failed attempt.
            AzureServiceBusLog.DeliveryAbandoned(logger, config.QueueName);
            await HandBackAsync(settlement, context is null ? null : ServiceBusMessages.AttemptProperties(message, context.DeliveryAttempt));
        }
        catch (Exception ex) when (ServiceBusSettlement.IsLockLost(ex))
        {
            LogLockLost(ex);
        }
        catch (Exception ex)
        {
            await BackOffAndHandBackAsync(settlement, message, ex, stopping.Token);
        }
        finally
        {
            scope.Exit();
            // Safety net: anything still unsettled goes back to the broker.
            if (!settlement.IsSettled)
                await HandBackAsync(settlement, null);
        }
    }

    // A message that could not be read: the shared pipeline dead-letters it as DeliveryFailed (or drops it when
    // dead-lettering is disabled). If that fails too, back off and hand it back.
    async Task RejectUnreadableAsync(Subscription current, IServiceBusDelivery delivery, ServiceBusSettlement settlement, Exception error, CancellationToken stopping)
    {
        try
        {
            var actions = new DeliveryActions(this, delivery, settlement, context: null, Stopwatch.GetTimestamp());
            await current.Dispatcher.RejectUnreadableAsync(SafeMessageId(delivery.Message), actions, error);
        }
        catch (Exception ex) when (ServiceBusSettlement.IsLockLost(ex))
        {
            LogLockLost(ex);
        }
        catch (Exception ex)
        {
            await BackOffAndHandBackAsync(settlement, delivery.Message, ex, stopping);
        }
    }

    static string SafeMessageId(ServiceBusReceivedMessage message)
    {
        try
        {
            return message.MessageId ?? string.Empty;
        }
        catch (Exception)
        {
            return string.Empty;
        }
    }

    // A settlement or retry send failed (broker trouble, missing rights): a growing pause per message keeps it from
    // coming straight back in a hot loop; then it is abandoned (DeliveryCount rises, so MaxDeliveryCount ends a
    // persistent failure in the broker's dead-letter queue). The error log is throttled.
    async Task BackOffAndHandBackAsync(ServiceBusSettlement settlement, ServiceBusReceivedMessage message, Exception error, CancellationToken stopping)
    {
        var key = SafeMessageId(message);
        var failures = transportFailures.RecordFailure(key);
        if (errorLog.ShouldLog("delivery", out var suppressed))
            AzureServiceBusLog.DeliveryFailed(logger, config.QueueName, failures, suppressed, error);
        await TransportBackoff.WaitAsync(failures, stopping);
        await HandBackAsync(settlement, null);
    }

    // The last attempt to settle: abandon, best effort. If it fails, the lock expires and the broker redelivers.
    async Task HandBackAsync(ServiceBusSettlement settlement, IDictionary<string, object>? properties)
    {
        try
        {
            await settlement.SettleAsync((delivery, token) => delivery.AbandonAsync(properties, token), final: true);
        }
        catch (Exception ex) when (ServiceBusSettlement.IsLockLost(ex))
        {
            LogLockLost(ex);
        }
        catch (Exception ex)
        {
            AzureServiceBusLog.AbandonFailed(logger, config.QueueName, ex);
        }
    }

    void LogLockLost(Exception error)
    {
        if (errorLog.ShouldLog("lock", out var suppressed))
            AzureServiceBusLog.LockLost(logger, config.QueueName, suppressed, error);
    }

    // A message that reached the handler callback after shutdown began was never handled: it goes back without
    // using up an attempt.
    async Task ReturnUnhandledAsync(IServiceBusDelivery delivery)
    {
        var settlement = new ServiceBusSettlement(delivery, config.SendTimeout);
        IDictionary<string, object>? properties = null;
        try
        {
            properties = ServiceBusMessages.AttemptProperties(delivery.Message, ServiceBusMessages.ToContext(delivery.Message).DeliveryAttempt);
        }
        catch (Exception)
        {
            // Unreadable: abandon it as it is.
        }
        await HandBackAsync(settlement, properties);
    }

    Task OnErrorAsync(ProcessErrorEventArgs args)
    {
        var error = args.Exception;
        var reason = error is ServiceBusException serviceBus ? serviceBus.Reason.ToString() : string.Empty;
        // B6: every error is counted, logged or throttled; the type is bounded (a reason code or an exception type).
        CountTransportError(reason.Length > 0 ? reason : error is null ? "unknown" : BoundedText.TypeName(error));
        var key = $"{args.ErrorSource}|{args.EntityPath}|{error?.GetType().FullName}|{reason}";
        if (!errorLog.ShouldLog(key, out var suppressed))
            return Task.CompletedTask;

        var source = args.ErrorSource.ToString();
        var type = error?.GetType().Name ?? "unknown";
        if (error is not null && ServiceBusSettlement.IsLockLost(error))
            AzureServiceBusLog.ProcessorWarning(logger, args.EntityPath ?? ConsumeEntity, source, type, suppressed, error);
        else
            AzureServiceBusLog.ProcessorError(logger, args.EntityPath ?? ConsumeEntity, source, type, suppressed, error);
        return Task.CompletedTask;
    }

    void CountTransportError(string errorType)
    {
        try
        {
            telemetry.RecordTransportError(consumeTarget, errorType);
        }
        catch (Exception ex)
        {
            AzureServiceBusLog.TelemetryFailed(logger, config.QueueName, ex);
        }
    }

    // Queue: a copy scheduled for now + delay with the next attempt, then the original completed, in one transaction
    // when TransactionalRetries is on. Without it the copy is sent first, so the worst case is a duplicate, never a loss.
    // Entities that refuse the transaction (partitioned entities, the Basic tier, the emulator) switch this queue to
    // send-then-complete for good, with one warning: the refused transaction rolled back, so nothing was applied.
    // Bounded by SendTimeout, not by the stopping token: a retry already decided is finished rather than duplicated.
    async Task RescheduleOnQueueAsync(IServiceBusDelivery delivery, ServiceBusSettlement settlement, MessageContext context, TimeSpan delay, int nextAttempt)
    {
        var copy = ServiceBusMessages.RetryCopy(delivery.Message, context, nextAttempt);
        if (delay > TimeSpan.Zero)
            copy.ScheduledEnqueueTime = DateTimeOffset.UtcNow + delay;

        await settlement.SettleAsync(async (original, token) =>
        {
            if (config.TransactionalRetries && Volatile.Read(ref transactionsRefused) == 0)
            {
                try
                {
                    await SendAndCompleteInTransactionAsync(copy, original, token);
                    return;
                }
                catch (Exception ex) when (IsTransactionRefusal(ex))
                {
                    if (Interlocked.Exchange(ref transactionsRefused, 1) == 0)
                        AzureServiceBusLog.TransactionsRefused(logger, config.QueueName, ex.GetType().Name, ex);
                }
            }

            await sender.SendAsync(copy, token);
            await original.CompleteAsync(token);
        });
    }

    // The commit runs synchronously when the scope is disposed (System.Transactions), on the calling thread (ADR 0009).
    async Task SendAndCompleteInTransactionAsync(ServiceBusMessage copy, IServiceBusDelivery original, CancellationToken token)
    {
        using var transaction = new TransactionScope(TransactionScopeOption.RequiresNew,
            new TransactionOptions { Timeout = config.SendTimeout }, TransactionScopeAsyncFlowOption.Enabled);
        await sender.SendAsync(copy, token);
        await original.CompleteAsync(token);
        transaction.Complete();
    }

    static bool IsTransactionRefusal(Exception exception) => ServiceBusSettlement.IsTransactionRefusal(exception);

    // Subscription: a copy sent to the topic would reach every subscription, so the retry waits under the message
    // lock (auto-renewed by the processor) when the delay fits the remaining renewal budget, then the message is
    // abandoned with the next attempt recorded. Longer delays are abandoned at once (logged). Shutdown cuts the wait short.
    async Task RescheduleOnSubscriptionAsync(ServiceBusReceivedMessage message, ServiceBusSettlement settlement, long received,
        TimeSpan delay, int nextAttempt, CancellationToken stopping)
    {
        if (delay > TimeSpan.Zero)
        {
            var budget = LockBudget(message, received);
            if (delay <= budget)
            {
                try
                {
                    await Task.Delay(delay, stopping);
                }
                catch (OperationCanceledException)
                {
                    // Shutting down: abandon now.
                }
            }
            else if (errorLog.ShouldLog("lock-budget", out var suppressed))
            {
                AzureServiceBusLog.DelayExceedsLock(logger, config.QueueName, delay, budget < TimeSpan.Zero ? TimeSpan.Zero : budget, suppressed);
            }
        }

        var properties = ServiceBusMessages.AttemptProperties(message, nextAttempt);
        await settlement.SettleAsync((delivery, token) => delivery.AbandonAsync(properties, token));
    }

    // How long the message can still be held: until the processor stops renewing its lock (MaxAutoLockRenewalDuration
    // after it was received) or until its current lock expires, whichever is later, minus a safety margin.
    TimeSpan LockBudget(ServiceBusReceivedMessage message, long received)
    {
        var renewal = config.MaxAutoLockRenewalDuration - Stopwatch.GetElapsedTime(received);
        var locked = message.LockedUntil - DateTimeOffset.UtcNow;
        return (renewal > locked ? renewal : locked) - LockMargin;
    }

    // Lets the health check reuse this queue's client: unhealthy while a subscription's processor is not running,
    // and a peek (Listen rights only) on the consumed entity through one cached receiver.
    async Task<bool> IAzureServiceBusProbe.IsHealthyAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        if (Volatile.Read(ref subscription) is { } active && !active.Processor.IsProcessing)
            return false;
        await Peeker().PeekAsync(cancellationToken);
        return true;
    }

    IServiceBusPeeker Peeker()
    {
        lock (peekerLock)
            return peeker ??= transport.CreatePeeker(ConsumeEntity, ConsumeSubscription);
    }

    void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed) == 1, this);

    /// <summary>
    /// Unsubscribes and closes the client, waiting at most <c>ShutdownTimeout</c> for running handlers plus one second
    /// for the close. Idempotent; called from inside a handler, it finishes in the background.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref disposed, 1) == 1)
            return;

        if (Volatile.Read(ref subscription) is { } active && HandlerScope.IsRunningHandlerOf(active))
        {
            // Disposing from inside one of this queue's handlers: finish in the background, after that handler.
            await Cancellation.CancelQuietlyAsync(active.Stopping, logger, config.QueueName);
            _ = Task.Run(DisposeCoreAsync);
            return;
        }

        await DisposeCoreAsync();
    }

    async Task DisposeCoreAsync()
    {
        // Runs on its own flow (possibly started from inside a handler): it is not that handler.
        HandlerScope.Clear();

        // One ShutdownTimeout for every handler, whether still subscribed or already draining. After it the client
        // is closed regardless (plus at most a second's grace): a straggler's settlement then fails, its lock expires
        // and the broker redelivers the message.
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
            AzureServiceBusLog.ShutdownTimedOut(logger, config.QueueName, config.ShutdownTimeout);
        }

        var closing = CloseAsync();
        try
        {
            await closing.WaitAsync(CloseGrace);
        }
        catch (TimeoutException)
        {
            _ = closing.ContinueWith(done => done.Exception, TaskScheduler.Default);
            AzureServiceBusLog.CloseAbandoned(logger, config.QueueName);
        }
    }

    async Task CloseAsync()
    {
        IServiceBusPeeker? cached;
        lock (peekerLock)
            cached = peeker;
        if (cached is not null)
            await DisposeQuietlyAsync(cached);
        await DisposeQuietlyAsync(sender);
        await DisposeQuietlyAsync(transport);
    }

    /// <summary>
    /// Blocks on <see cref="DisposeAsync"/> on a thread-pool thread (so it cannot deadlock on a synchronization
    /// context), at most about <c>ShutdownTimeout</c> plus one second. Prefer <see cref="DisposeAsync"/>; this exists
    /// for callers that only know <see cref="IDisposable"/>, such as a synchronously disposed service provider.
    /// </summary>
    public void Dispose() => Task.Run(() => DisposeAsync().AsTask()).GetAwaiter().GetResult();

    sealed class Subscription
    {
        public Subscription(IServiceBusProcessor processor, MessageDispatcher<T> dispatcher)
        {
            Processor = processor;
            Dispatcher = dispatcher;
        }

        public IServiceBusProcessor Processor { get; }
        public MessageDispatcher<T> Dispatcher { get; }
        public CancellationTokenSource Stopping { get; } = new();
        public ConcurrentDictionary<long, Task> InFlight { get; } = new();
        public long NextDeliveryId;
    }

    sealed class DeliveryActions : IDeliveryActions
    {
        readonly AzureServiceBusMessageQueue<T> queue;
        readonly IServiceBusDelivery delivery;
        readonly ServiceBusSettlement settlement;
        readonly MessageContext? context;
        readonly long received;

        public DeliveryActions(AzureServiceBusMessageQueue<T> queue, IServiceBusDelivery delivery, ServiceBusSettlement settlement,
            MessageContext? context, long received)
        {
            this.queue = queue;
            this.delivery = delivery;
            this.settlement = settlement;
            this.context = context;
            this.received = received;
        }

        public Task CompleteAsync(CancellationToken cancellationToken) =>
            settlement.SettleAsync((d, token) => d.CompleteAsync(token));

        public Task RescheduleAsync(TimeSpan delay, int nextAttempt, CancellationToken cancellationToken) =>
            queue.subscriptionMode
                ? queue.RescheduleOnSubscriptionAsync(delivery.Message, settlement, received, delay, nextAttempt, cancellationToken)
                : queue.RescheduleOnQueueAsync(delivery, settlement, context!, delay, nextAttempt);

        public Task DeadLetterAsync(DeadLetterReason reason, CancellationToken cancellationToken) =>
            settlement.SettleAsync((d, token) =>
                d.DeadLetterAsync(ServiceBusMessages.DeadLetterProperties(reason, context?.DeliveryAttempt), reason.Code, reason.Detail ?? string.Empty, token));
    }
}

// Lets the Azure Service Bus health check reuse a queue's client instead of creating its own.
interface IAzureServiceBusProbe
{
    Task<bool> IsHealthyAsync(CancellationToken cancellationToken);
}

static partial class AzureServiceBusLog
{
    [LoggerMessage(EventId = 3001, Level = LogLevel.Error,
        Message = "Message from Azure Service Bus queue {QueueName} could not be settled ({ConsecutiveFailures} failure(s) in a row, {SuppressedLogs} similar error(s) not logged); abandoned after a backoff")]
    public static partial void DeliveryFailed(ILogger logger, string queueName, int consecutiveFailures, int suppressedLogs, Exception exception);

    [LoggerMessage(EventId = 3002, Level = LogLevel.Debug, Message = "Message from Azure Service Bus queue {QueueName} was interrupted by shutdown and abandoned for redelivery")]
    public static partial void DeliveryAbandoned(ILogger logger, string queueName);

    [LoggerMessage(EventId = 3003, Level = LogLevel.Error,
        Message = "Azure Service Bus client for queue {QueueName} could not be created ({ExceptionType}); the exception is not logged because it can contain the connection string")]
    public static partial void ClientCreationFailed(ILogger logger, string queueName, string exceptionType);

    [LoggerMessage(EventId = 3004, Level = LogLevel.Warning,
        Message = "Azure Service Bus queue {QueueName}: handlers did not finish within {ShutdownTimeout}; the processor is disposed once they do")]
    public static partial void ShutdownTimedOut(ILogger logger, string queueName, TimeSpan shutdownTimeout);

    [LoggerMessage(EventId = 3005, Level = LogLevel.Warning, Message = "Azure Service Bus queue {QueueName}: abandoning a message failed; it is redelivered when its lock expires")]
    public static partial void AbandonFailed(ILogger logger, string queueName, Exception exception);

    [LoggerMessage(EventId = 3006, Level = LogLevel.Warning, Message = "Azure Service Bus queue {QueueName}: stopping the processor failed")]
    public static partial void StopFailed(ILogger logger, string queueName, Exception exception);

    [LoggerMessage(EventId = 3007, Level = LogLevel.Debug, Message = "Azure Service Bus queue {QueueName}: closing a client resource failed")]
    public static partial void CloseFailed(ILogger logger, string queueName, Exception exception);

    [LoggerMessage(EventId = 3008, Level = LogLevel.Warning,
        Message = "Azure Service Bus queue {QueueName}: a message lock was lost ({SuppressedLogs} similar warning(s) not logged); the broker redelivers the message")]
    public static partial void LockLost(ILogger logger, string queueName, int suppressedLogs, Exception exception);

    [LoggerMessage(EventId = 3009, Level = LogLevel.Error,
        Message = "Azure Service Bus processor error on {EntityPath} from {ErrorSource} ({ExceptionType}; {SuppressedLogs} similar error(s) not logged)")]
    public static partial void ProcessorError(ILogger logger, string entityPath, string errorSource, string exceptionType, int suppressedLogs, Exception? exception);

    [LoggerMessage(EventId = 3010, Level = LogLevel.Warning,
        Message = "Azure Service Bus processor warning on {EntityPath} from {ErrorSource} ({ExceptionType}; {SuppressedLogs} similar warning(s) not logged): the message is redelivered")]
    public static partial void ProcessorWarning(ILogger logger, string entityPath, string errorSource, string exceptionType, int suppressedLogs, Exception? exception);

    [LoggerMessage(EventId = 3011, Level = LogLevel.Warning,
        Message = "Azure Service Bus queue {QueueName}: retry delay {Delay} exceeds the remaining lock budget {LockBudget} of a subscription message, so it is abandoned for immediate redelivery ({SuppressedLogs} similar warning(s) not logged)")]
    public static partial void DelayExceedsLock(ILogger logger, string queueName, TimeSpan delay, TimeSpan lockBudget, int suppressedLogs);

    [LoggerMessage(EventId = 3012, Level = LogLevel.Warning,
        Message = "Azure Service Bus queue {QueueName}: the retry policy allows {Deliveries} deliveries, more than Service Bus's default MaxDeliveryCount ({MaxDeliveryCount}); make sure the subscription's MaxDeliveryCount is higher, or the broker dead-letters first")]
    public static partial void RetriesExceedMaxDeliveryCount(ILogger logger, string queueName, int deliveries, int maxDeliveryCount);

    [LoggerMessage(EventId = 3014, Level = LogLevel.Warning,
        Message = "Azure Service Bus queue {QueueName}: the entity refused a transaction ({ExceptionType}); retries now send the copy, then complete the original (at-least-once: a crash in between can duplicate). Partitioned entities, the Basic tier and the emulator do this; set TransactionalRetries = false to skip the attempt")]
    public static partial void TransactionsRefused(ILogger logger, string queueName, string exceptionType, Exception exception);

    [LoggerMessage(EventId = 3015, Level = LogLevel.Warning,
        Message = "Azure Service Bus queue {QueueName}: subscription with PrefetchCount {PrefetchCount}: prefetched messages' locks run down while retries wait under the lock in handler slots, so they can expire and be redelivered; consider PrefetchCount = 0")]
    public static partial void SubscriptionPrefetches(ILogger logger, string queueName, int prefetchCount);

    [LoggerMessage(EventId = 3016, Level = LogLevel.Warning,
        Message = "Azure Service Bus queue {QueueName}: TransactionalRetries with MaxConcurrentCalls {MaxConcurrentCalls} (above {Threshold}): each retry's transaction commit blocks a thread-pool thread for a broker round trip")]
    public static partial void TransactionalConcurrency(ILogger logger, string queueName, int maxConcurrentCalls, int threshold);

    [LoggerMessage(EventId = 3013, Level = LogLevel.Warning,
        Message = "Azure Service Bus queue {QueueName}: the client did not close in time (a handler is still running); it closes in the background")]
    public static partial void CloseAbandoned(ILogger logger, string queueName);

    [LoggerMessage(EventId = 3018, Level = LogLevel.Warning,
        Message = "Azure Service Bus queue {QueueName}: handing back a dead letter a pass held failed; it is returned when its lock expires")]
    public static partial void DeadLetterAbandonFailed(ILogger logger, string queueName, Exception exception);

    [LoggerMessage(EventId = 3019, Level = LogLevel.Warning,
        Message = "Azure Service Bus queue {QueueName}: the entity refused a transaction for a dead-letter replay ({ExceptionType}); replays now send the copy, then complete the dead letter (at-least-once: a failure in between leaves a duplicate)")]
    public static partial void ReplayTransactionsRefused(ILogger logger, string queueName, string exceptionType, Exception exception);

    [LoggerMessage(EventId = 3017, Level = LogLevel.Warning,
        Message = "Azure Service Bus queue {QueueName}: telemetry failed while counting a processor error; the error is handled regardless")]
    public static partial void TelemetryFailed(ILogger logger, string queueName, Exception exception);
}
