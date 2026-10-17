using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Transactions;
using Azure.Core;
using Azure.Messaging.ServiceBus;
using ESE.MessageQueue.AzureServiceBus;
using ESE.MessageQueue.Serialization;
using ESE.MessageQueue.Telemetry;
using Microsoft.Extensions.Logging;

namespace ESE.MessageQueue.Tests;

// An in-process stand-in for the Service Bus SDK behind the provider's seams. It mirrors the SDK where the specs depend
// on it: sends and settlements complete asynchronously and enlist in the ambient transaction (applied on commit,
// discarded on rollback), StopAsync cancels the deliveries' tokens and waits for running handlers (as
// StopProcessingAsync does), a processor's handlers can be registered once, a delivery can be settled once, and
// batches enforce a byte limit.
sealed class FakeServiceBus : IServiceBusTransportFactory, IServiceBusTransport
{
    public ConcurrentQueue<string> Operations { get; } = new();
    public List<(AzureServiceBusConfig Config, TokenCredential? Credential)> Created { get; } = [];
    public List<FakeServiceBusSender> Senders { get; } = [];
    public List<FakeServiceBusProcessor> Processors { get; } = [];
    public List<FakeServiceBusPeeker> Peekers { get; } = [];
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
        var delivery = new FakeServiceBusDelivery(bus, message, stopping.Token) { Fail = fail };
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
