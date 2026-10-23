using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Transactions;
using Azure.Core;
using Azure.Messaging.ServiceBus;
using ESE.MessageQueue.AzureServiceBus;
using ESE.MessageQueue.Providers;
using ESE.MessageQueue.Serialization;
using ESE.MessageQueue.Telemetry;
using Microsoft.Extensions.Logging;

namespace ESE.MessageQueue.Tests;

// An in-process stand-in for the Service Bus SDK behind the provider's seams. It mirrors the SDK where the specs depend
// on it: sends and settlements complete asynchronously and enlist in the ambient transaction (applied on commit,
// discarded on rollback), StopAsync cancels the deliveries' tokens and waits for running handlers (as
// StopProcessingAsync does), a processor's handlers can be registered once, a delivery can be settled once, and
// batches enforce a byte limit. Each entity has a dead-letter subqueue: deliveries dead-lettered through a processor land
// in it (with the SDK's DeadLetterReason/DeadLetterErrorDescription properties and a new sequence number), a receiver
// peeks it with its own cursor, receives under a lock that only that receiver can settle once (each receive is one
// delivery: DeliveryCount + 1), an abandon or an expired lock makes the message receivable again, a lock stays held
// after the receiver is disposed until it expires (ExpireLocks), and pages can be capped (MaxPageSize) as the broker
// may return fewer messages than asked for.
sealed class FakeServiceBus : IServiceBusTransportFactory, IServiceBusTransport
{
    public ConcurrentQueue<string> Operations { get; } = new();
    public List<(AzureServiceBusConfig Config, TokenCredential? Credential)> Created { get; } = [];
    public List<FakeServiceBusSender> Senders { get; } = [];
    public List<FakeServiceBusProcessor> Processors { get; } = [];
    public List<FakeServiceBusPeeker> Peekers { get; } = [];
    public List<FakeServiceBusDeadLetterReceiver> DeadLetterReceivers { get; } = [];
    public ConcurrentDictionary<string, FakeDeadLetterSubqueue> DeadLetterQueues { get; } = new();
    public Exception? CreateFailure { get; set; }
    public long BatchLimitBytes { get; set; } = 1024;
    public bool Disposed => DisposeCount > 0;
    public int DisposeCount;

    public FakeServiceBusSender Sender => Senders.Single();
    public FakeServiceBusProcessor Processor => Processors[^1];

    public AzureServiceBusMessageQueue<TestMessage> CreateQueue(AzureServiceBusConfig config, ILogger? logger = null,
        TokenCredential? credential = null, ITelemetryProvider? telemetry = null) =>
        new(config, credential, telemetry, logger, this);

    public IServiceBusTransport Create(AzureServiceBusConfig config, TokenCredential? credential)
    {
        Created.Add((config, credential));
        if (CreateFailure is { } failure)
            throw failure;
        return this;
    }

    public IServiceBusSender CreateSender(string entityPath)
    {
        var sender = new FakeServiceBusSender(this, entityPath);
        Senders.Add(sender);
        return sender;
    }

    public IServiceBusProcessor CreateProcessor(string entityPath, string? subscriptionName, ServiceBusProcessorOptions options)
    {
        var processor = new FakeServiceBusProcessor(this, entityPath, subscriptionName, options);
        Processors.Add(processor);
        return processor;
    }

    public IServiceBusPeeker CreatePeeker(string entityPath, string? subscriptionName)
    {
        var peeker = new FakeServiceBusPeeker(entityPath, subscriptionName);
        Peekers.Add(peeker);
        return peeker;
    }

    public FakeDeadLetterSubqueue DeadLetterQueue(string entityPath, string? subscriptionName = null) =>
        DeadLetterQueues.GetOrAdd(subscriptionName is null ? entityPath : $"{entityPath}/Subscriptions/{subscriptionName}", _ => new FakeDeadLetterSubqueue(this));

    public IServiceBusDeadLetterReceiver CreateDeadLetterReceiver(string entityPath, string? subscriptionName)
    {
        var receiver = new FakeServiceBusDeadLetterReceiver(this, DeadLetterQueue(entityPath, subscriptionName), entityPath, subscriptionName);
        lock (DeadLetterReceivers)
            DeadLetterReceivers.Add(receiver);
        return receiver;
    }

    public ValueTask DisposeAsync()
    {
        Interlocked.Increment(ref DisposeCount);
        return ValueTask.CompletedTask;
    }

    internal void Record(string operation, string? messageId) =>
        Operations.Enqueue($"{operation}:{messageId}{(Transaction.Current is null ? "" : ":tx")}");

    // Like the SDK's AMQP transaction enlistment: inside an ambient transaction the effect happens on commit only.
    internal static void Apply(Action effect, Action? rollback = null)
    {
        if (Transaction.Current is { } transaction)
            transaction.EnlistVolatile(new TransactionalEffect(effect, rollback), EnlistmentOptions.None);
        else
            effect();
    }

    sealed class TransactionalEffect : IEnlistmentNotification
    {
        readonly Action effect;
        readonly Action? rollback;

        public TransactionalEffect(Action effect, Action? rollback)
        {
            this.effect = effect;
            this.rollback = rollback;
        }

        public void Prepare(PreparingEnlistment preparingEnlistment) => preparingEnlistment.Prepared();

        public void Commit(Enlistment enlistment)
        {
            effect();
            enlistment.Done();
        }

        public void Rollback(Enlistment enlistment)
        {
            rollback?.Invoke();
            enlistment.Done();
        }

        public void InDoubt(Enlistment enlistment) => enlistment.Done();
    }

    public static ServiceBusReceivedMessage Message(
        string messageId = "m-1",
        TestMessage? body = null,
        int deliveryCount = 1,
        IDictionary<string, object>? properties = null,
        DateTimeOffset lockedUntil = default,
        DateTimeOffset enqueuedTime = default,
        string? correlationId = null) =>
        ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromBytes(MessageSerializer.Serialize(body ?? new TestMessage(messageId, "content"), SerializationFormat.Json)),
            messageId: messageId,
            correlationId: correlationId,
            properties: properties ?? new Dictionary<string, object>(),
            deliveryCount: deliveryCount,
            lockedUntil: lockedUntil == default ? DateTimeOffset.UtcNow.AddMinutes(1) : lockedUntil,
            enqueuedTime: enqueuedTime == default ? DateTimeOffset.UtcNow : enqueuedTime);
}

sealed class FakeServiceBusSender : IServiceBusSender
{
    readonly FakeServiceBus bus;
    readonly ConditionalWeakTable<ServiceBusMessageBatch, List<ServiceBusMessage>> batchContents = new();

    public FakeServiceBusSender(FakeServiceBus bus, string entityPath)
    {
        this.bus = bus;
        EntityPath = entityPath;
    }

    public string EntityPath { get; }
    public ConcurrentQueue<ServiceBusMessage> Sent { get; } = new();
    public List<List<ServiceBusMessage>> Batches { get; } = [];
    public Func<ServiceBusMessage, Exception?>? SendFailure { get; set; }
    public TimeSpan SendDelay { get; set; }
    public bool Disposed { get; private set; }

    public async Task SendAsync(ServiceBusMessage message, CancellationToken cancellationToken)
    {
        await Task.Yield();
        if (SendDelay > TimeSpan.Zero)
            await Task.Delay(SendDelay, cancellationToken);
        bus.Record("send", message.MessageId);
        if (SendFailure?.Invoke(message) is { } failure)
            throw failure;
        FakeServiceBus.Apply(() => Sent.Enqueue(message));
    }

    // A batch holds as many messages as fit in BatchLimitBytes of body.
    public ValueTask<ServiceBusMessageBatch> CreateBatchAsync(CancellationToken cancellationToken)
    {
        var store = new List<ServiceBusMessage>();
        long size = 0;
        var batch = ServiceBusModelFactory.ServiceBusMessageBatch(bus.BatchLimitBytes, store,
            new CreateMessageBatchOptions { MaxSizeInBytes = bus.BatchLimitBytes },
            message =>
            {
                var bytes = message.Body.ToMemory().Length;
                if (size + bytes > bus.BatchLimitBytes)
                    return false;
                size += bytes;
                return true;
            });
        batchContents.Add(batch, store);
        return ValueTask.FromResult(batch);
    }

    public async Task SendBatchAsync(ServiceBusMessageBatch batch, CancellationToken cancellationToken)
    {
        await Task.Yield();
        if (SendDelay > TimeSpan.Zero)
            await Task.Delay(SendDelay, cancellationToken);
        var messages = batchContents.TryGetValue(batch, out var store) ? store.ToList() : [];
        bus.Record("batch", messages.Count.ToString(System.Globalization.CultureInfo.InvariantCulture));
        if (messages.Select(m => SendFailure?.Invoke(m)).FirstOrDefault(e => e is not null) is { } failure)
            throw failure;
        lock (Batches)
            Batches.Add(messages);
    }

    public ValueTask DisposeAsync()
    {
        Disposed = true;
        return ValueTask.CompletedTask;
    }
}

sealed class FakeServiceBusProcessor : IServiceBusProcessor
{
    readonly FakeServiceBus bus;
    readonly CancellationTokenSource stopping = new();
    readonly ConcurrentDictionary<FakeServiceBusDelivery, Task> running = new();
    Func<IServiceBusDelivery, Task>? onMessage;
    Func<ProcessErrorEventArgs, Task>? onError;

    public FakeServiceBusProcessor(FakeServiceBus bus, string entityPath, string? subscriptionName, ServiceBusProcessorOptions options)
    {
        this.bus = bus;
        EntityPath = entityPath;
        SubscriptionName = subscriptionName;
        Options = options;
    }

    public string EntityPath { get; }
    public string? SubscriptionName { get; }
    public ServiceBusProcessorOptions Options { get; }
    public bool IsProcessing { get; set; }
    public bool Disposed { get; private set; }
    public int StopCalls;

    public Task StartAsync(Func<IServiceBusDelivery, Task> onMessage, Func<ProcessErrorEventArgs, Task> onError, CancellationToken cancellationToken)
    {
        if (this.onMessage is not null)
            throw new InvalidOperationException("The handler has already been assigned to this processor.");
        this.onMessage = onMessage;
        this.onError = onError;
        IsProcessing = true;
        return Task.CompletedTask;
    }

    // Hands a message to the provider the way the processor does: on a pool thread, awaited until the handler returns.
    public FakeServiceBusDelivery Deliver(ServiceBusReceivedMessage message, Func<string, Exception?>? fail = null)
    {
        var delivery = new FakeServiceBusDelivery(bus, message, stopping.Token) { Fail = fail, DeadLetterQueue = bus.DeadLetterQueue(EntityPath, SubscriptionName) };
        delivery.Handled = Run(delivery);
        return delivery;
    }

    async Task Run(FakeServiceBusDelivery delivery)
    {
        var handler = Task.Run(() => onMessage!(delivery));
        running[delivery] = handler;
        try
        {
            await handler;
        }
        catch (Exception ex)
        {
            delivery.HandlerException = ex;
        }
        finally
        {
            running.TryRemove(delivery, out _);
        }
    }

    public Task RaiseErrorAsync(Exception exception, ServiceBusErrorSource source = ServiceBusErrorSource.Receive) =>
        onError!(new ProcessErrorEventArgs(exception, source, "test.servicebus.windows.net", EntityPath, "processor-1", CancellationToken.None));

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref StopCalls);
        IsProcessing = false;
        await stopping.CancelAsync();
        await Task.WhenAll(running.Values).WaitAsync(cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        if (IsProcessing)
            await StopAsync(CancellationToken.None);
        Disposed = true;
    }
}

sealed class FakeServiceBusDelivery : IServiceBusDelivery
{
    readonly FakeServiceBus bus;
    readonly TaskCompletionSource settled = new(TaskCreationOptions.RunContinuationsAsynchronously);
    int settlements;

    public FakeServiceBusDelivery(FakeServiceBus bus, ServiceBusReceivedMessage message, CancellationToken cancellationToken)
    {
        this.bus = bus;
        Message = message;
        CancellationToken = cancellationToken;
    }

    public ServiceBusReceivedMessage Message { get; }
    public CancellationToken CancellationToken { get; }
    public Task Handled { get; set; } = Task.CompletedTask;
    public Exception? HandlerException { get; set; }
    public Func<string, Exception?>? Fail { get; set; }
    public FakeDeadLetterSubqueue? DeadLetterQueue { get; set; }
    public ConcurrentQueue<string> Attempts { get; } = new();
    public string? Settlement { get; private set; }
    public IDictionary<string, object>? Properties { get; private set; }
    public string? DeadLetterReason { get; private set; }
    public string? DeadLetterDescription { get; private set; }
    public Task Settled => settled.Task;

    public Task CompleteAsync(CancellationToken cancellationToken) => Settle("complete", null, cancellationToken);

    public Task AbandonAsync(IDictionary<string, object>? propertiesToModify, CancellationToken cancellationToken) =>
        Settle("abandon", propertiesToModify, cancellationToken);

    public Task DeadLetterAsync(IDictionary<string, object> propertiesToModify, string reason, string description, CancellationToken cancellationToken)
    {
        DeadLetterReason = reason;
        DeadLetterDescription = description;
        return Settle("deadletter", propertiesToModify, cancellationToken);
    }

    async Task Settle(string action, IDictionary<string, object>? properties, CancellationToken cancellationToken)
    {
        await Task.Yield();
        Attempts.Enqueue(action);
        bus.Record(action, Message.MessageId);
        if (Fail?.Invoke(action) is { } failure)
            throw failure;
        // The SDK refuses a second settlement of the same lock.
        if (Interlocked.Increment(ref settlements) > 1)
            throw new InvalidOperationException($"The message was already settled ({Settlement}); {action} is not allowed.");
        var snapshot = properties is null ? null : new Dictionary<string, object>(properties);
        FakeServiceBus.Apply(() =>
        {
            Settlement = action;
            Properties = snapshot;
            if (action == "deadletter")
                DeadLetterQueue?.AddDeadLettered(Message, snapshot!, DeadLetterReason!, DeadLetterDescription!);
            settled.TrySetResult();
        }, rollback: () => Interlocked.Decrement(ref settlements));
    }
}

sealed class FakeServiceBusPeeker : IServiceBusPeeker
{
    public FakeServiceBusPeeker(string entityPath, string? subscriptionName)
    {
        EntityPath = entityPath;
        SubscriptionName = subscriptionName;
    }

    public string EntityPath { get; }
    public string? SubscriptionName { get; }
    public int Peeks;
    public Exception? Failure { get; set; }
    public bool Disposed { get; private set; }

    public Task PeekAsync(CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref Peeks);
        return Failure is null ? Task.CompletedTask : Task.FromException(Failure);
    }

    public ValueTask DisposeAsync()
    {
        Disposed = true;
        return ValueTask.CompletedTask;
    }
}

// One entity's dead-letter subqueue, in sequence-number order.
sealed class FakeDeadLetterSubqueue
{
    // The SDK's names for the dead-letter reason and description application properties.
    public const string ReasonProperty = "DeadLetterReason";
    public const string DescriptionProperty = "DeadLetterErrorDescription";

    readonly FakeServiceBus bus;
    readonly object gate = new();
    readonly List<Slot> slots = [];
    long nextSequenceNumber = 1;

    public FakeDeadLetterSubqueue(FakeServiceBus bus)
    {
        this.bus = bus;
    }

    sealed class Slot(ServiceBusReceivedMessage message)
    {
        public ServiceBusReceivedMessage Message { get; set; } = message;
        public object? LockedBy { get; set; }
        public bool Settling { get; set; }
    }

    public IReadOnlyList<ServiceBusReceivedMessage> Messages
    {
        get
        {
            lock (gate)
                return [.. slots.Select(s => s.Message)];
        }
    }

    // Runs before each receive, for example to let locks expire in the middle of a pass.
    public Action? BeforeReceive { get; set; }

    // The most messages one peek or receive returns, whatever was asked for.
    public int MaxPageSize { get; set; } = int.MaxValue;

    // A failure for an abandon of the given message, before the lock is released.
    public Func<ServiceBusReceivedMessage, Exception?>? AbandonFailure { get; set; }

    public int LockedCount
    {
        get
        {
            lock (gate)
                return slots.Count(s => s.LockedBy is not null);
        }
    }

    // A message as the broker stores it in the subqueue: a new sequence number, delivery count 0 until received.
    public ServiceBusReceivedMessage Add(
        string messageId = "m-1",
        TestMessage? body = null,
        IDictionary<string, object>? properties = null,
        string? correlationId = null,
        DateTimeOffset enqueuedTime = default,
        byte[]? rawBody = null)
    {
        lock (gate)
        {
            var message = Create(
                BinaryData.FromBytes(rawBody ?? MessageSerializer.Serialize(body ?? new TestMessage(messageId, "content"), SerializationFormat.Json)),
                messageId, correlationId, properties ?? new Dictionary<string, object>(), deliveryCount: 0,
                nextSequenceNumber++, enqueuedTime == default ? DateTimeOffset.UtcNow : enqueuedTime);
            slots.Add(new Slot(message));
            return message;
        }
    }

    // What DeadLetterMessageAsync does: the message moves here with the given properties plus the SDK's reason and
    // description properties.
    public void AddDeadLettered(ServiceBusReceivedMessage original, IDictionary<string, object> propertiesToModify, string reason, string description)
    {
        var properties = new Dictionary<string, object>(original.ApplicationProperties);
        foreach (var (key, value) in propertiesToModify)
            properties[key] = value;
        // The broker's own properties replace any of the same name, whatever its case.
        foreach (var key in properties.Keys.Where(k => k.Equals(ReasonProperty, StringComparison.OrdinalIgnoreCase)
            || k.Equals(DescriptionProperty, StringComparison.OrdinalIgnoreCase)).ToList())
            properties.Remove(key);
        properties[ReasonProperty] = reason;
        properties[DescriptionProperty] = description;
        Add(original.MessageId, properties: properties, correlationId: original.CorrelationId, enqueuedTime: original.EnqueuedTime,
            rawBody: original.Body.ToArray());
    }

    // Every lock runs out: the messages are receivable again (the next receive counts the delivery).
    public void ExpireLocks()
    {
        lock (gate)
        {
            foreach (var slot in slots.Where(s => s.LockedBy is not null && !s.Settling))
                slot.LockedBy = null;
        }
    }

    internal IReadOnlyList<ServiceBusReceivedMessage> Peek(long afterSequenceNumber, int maxMessages)
    {
        lock (gate)
            return [.. slots.Where(s => s.Message.SequenceNumber > afterSequenceNumber).Take(Math.Min(maxMessages, MaxPageSize)).Select(s => s.Message)];
    }

    internal IReadOnlyList<ServiceBusReceivedMessage> Receive(object receiver, int maxMessages)
    {
        lock (gate)
        {
            var taken = slots.Where(s => s.LockedBy is null).Take(Math.Min(maxMessages, MaxPageSize)).ToList();
            foreach (var slot in taken)
            {
                slot.LockedBy = receiver;
                slot.Message = WithDeliveryCount(slot.Message, slot.Message.DeliveryCount + 1);
            }
            return [.. taken.Select(s => s.Message)];
        }
    }

    internal async Task CompleteAsync(object receiver, ServiceBusReceivedMessage message)
    {
        await Task.Yield();
        bus.Record("dlq-complete", message.MessageId);
        Slot slot;
        lock (gate)
        {
            slot = Locked(receiver, message);
            slot.Settling = true;
        }
        FakeServiceBus.Apply(() =>
        {
            lock (gate)
                slots.Remove(slot);
        }, rollback: () =>
        {
            lock (gate)
                slot.Settling = false;
        });
    }

    internal async Task AbandonAsync(object receiver, ServiceBusReceivedMessage message)
    {
        await Task.Yield();
        bus.Record("dlq-abandon", message.MessageId);
        if (AbandonFailure?.Invoke(message) is { } failure)
            throw failure;
        lock (gate)
        {
            var slot = Locked(receiver, message);
            slot.LockedBy = null;
        }
    }

    // Like the broker: only the receiver holding the lock can settle, once.
    Slot Locked(object receiver, ServiceBusReceivedMessage message) =>
        slots.FirstOrDefault(s => s.Message.SequenceNumber == message.SequenceNumber && ReferenceEquals(s.LockedBy, receiver) && !s.Settling)
            ?? throw new ServiceBusException("The lock supplied is invalid", ServiceBusFailureReason.MessageLockLost);

    static ServiceBusReceivedMessage WithDeliveryCount(ServiceBusReceivedMessage message, int deliveryCount) =>
        Create(message.Body, message.MessageId, message.CorrelationId, new Dictionary<string, object>(message.ApplicationProperties),
            deliveryCount, message.SequenceNumber, message.EnqueuedTime);

    static ServiceBusReceivedMessage Create(BinaryData body, string messageId, string? correlationId, IDictionary<string, object> properties,
        int deliveryCount, long sequenceNumber, DateTimeOffset enqueuedTime) =>
        ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: body,
            messageId: messageId,
            correlationId: correlationId,
            properties: properties,
            deliveryCount: deliveryCount,
            sequenceNumber: sequenceNumber,
            lockedUntil: DateTimeOffset.UtcNow.AddMinutes(1),
            enqueuedTime: enqueuedTime);
}

sealed class FakeServiceBusDeadLetterReceiver : IServiceBusDeadLetterReceiver
{
    readonly FakeServiceBus bus;
    readonly FakeDeadLetterSubqueue subqueue;
    long peekCursor;

    public FakeServiceBusDeadLetterReceiver(FakeServiceBus bus, FakeDeadLetterSubqueue subqueue, string entityPath, string? subscriptionName)
    {
        this.bus = bus;
        this.subqueue = subqueue;
        EntityPath = entityPath;
        SubscriptionName = subscriptionName;
    }

    public string EntityPath { get; }
    public string? SubscriptionName { get; }
    public ConcurrentQueue<TimeSpan> ReceiveWaits { get; } = new();
    public int Peeks;
    public bool Disposed { get; private set; }

    public async Task<IReadOnlyList<ServiceBusReceivedMessage>> PeekAsync(int maxMessages, CancellationToken cancellationToken)
    {
        await Task.Yield();
        Interlocked.Increment(ref Peeks);
        var page = subqueue.Peek(peekCursor, maxMessages);
        if (page.Count > 0)
            peekCursor = page[^1].SequenceNumber;
        return page;
    }

    public async Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveAsync(int maxMessages, TimeSpan maxWaitTime, CancellationToken cancellationToken)
    {
        await Task.Yield();
        ReceiveWaits.Enqueue(maxWaitTime);
        subqueue.BeforeReceive?.Invoke();
        return subqueue.Receive(this, maxMessages);
    }

    public Task CompleteAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken) => subqueue.CompleteAsync(this, message);

    public Task AbandonAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken) => subqueue.AbandonAsync(this, message);

    public ValueTask DisposeAsync()
    {
        Disposed = true;
        return ValueTask.CompletedTask;
    }
}
