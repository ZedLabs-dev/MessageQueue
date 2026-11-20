using System.Transactions;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using ZedLabs.MessageQueue.Abstractions;
using ZedLabs.MessageQueue.Providers;

namespace ZedLabs.MessageQueue.AzureServiceBus;

// ADR 0023: the entity's dead-letter subqueue as a store. A browse-only pass peeks (no lock, Listen rights); a settling
// pass receives in PeekLock, so every message it returned stays locked until it is replayed, completed or abandoned
// when the pass ends. One receiver per pass, so every peek starts at the beginning.
sealed class ServiceBusDeadLetterStore : IDeadLetterStore
{
    const int PeekPageSize = 100;

    // Few locks at a time: a lock runs down while the pass works through the batch.
    const int ReceiveBatchSize = 10;

    // The SDK settles by the received message, body included, so a pass cannot keep only the lock of a dead letter it
    // skips. It ends once it holds this many unsettled ones, which bounds that memory (call again to go on).
    internal const int MaxHeldMessages = 1000;

    // An empty subqueue answers after this wait instead of the SDK's default (about a minute).
    internal static readonly TimeSpan ReceiveWait = TimeSpan.FromSeconds(1);

    readonly IServiceBusTransport transport;
    readonly IServiceBusSender sender;
    readonly AzureServiceBusConfig config;
    readonly ILogger logger;
    int transactionsRefused;

    public ServiceBusDeadLetterStore(IServiceBusTransport transport, IServiceBusSender sender, AzureServiceBusConfig config, ILogger logger)
    {
        this.transport = transport;
        this.sender = sender;
        this.config = config;
        this.logger = logger;
    }

    // A resend to a topic reaches every subscription, so a subscription's dead letters can be read and deleted only.
    public static DeadLetterOperations OperationsFor(AzureServiceBusConfig config) =>
        config.UsesSubscription ? DeadLetterOperations.Peek | DeadLetterOperations.Purge : DeadLetterOperations.All;

    public DeadLetterOperations Operations => OperationsFor(config);

    public Task<IDeadLetterScan> OpenScanAsync(bool browseOnly, CancellationToken cancellationToken)
    {
        var receiver = config.UsesSubscription
            ? transport.CreateDeadLetterReceiver(config.TopicName!, config.SubscriptionName)
            : transport.CreateDeadLetterReceiver(config.QueueName, null);
        return Task.FromResult<IDeadLetterScan>(new Scan(this, receiver, browseOnly));
    }

    // Send the copy, then complete the dead letter: in one transaction while TransactionalRetries is on and the entity
    // accepts it (the dead-letter subqueue and its queue; unverified against a namespace), else in that order, so a
    // failure in between leaves a duplicate, never a loss. A refusal rolled back, so nothing was applied (ADR 0009).
    async Task ReplayAsync(IServiceBusDeadLetterReceiver receiver, ServiceBusReceivedMessage deadLetter, CancellationToken cancellationToken)
    {
        var copy = ServiceBusMessages.ReplayCopy(deadLetter);
        using var timeout = Bounded(cancellationToken);
        if (config.TransactionalRetries && Volatile.Read(ref transactionsRefused) == 0)
        {
            try
            {
                using var transaction = new TransactionScope(TransactionScopeOption.RequiresNew,
                    new TransactionOptions { Timeout = config.SendTimeout }, TransactionScopeAsyncFlowOption.Enabled);
                await sender.SendAsync(copy, timeout.Token);
                await receiver.CompleteAsync(deadLetter, timeout.Token);
                transaction.Complete();
                return;
            }
            catch (Exception ex) when (ServiceBusSettlement.IsTransactionRefusal(ex))
            {
                if (Interlocked.Exchange(ref transactionsRefused, 1) == 0)
                    AzureServiceBusLog.ReplayTransactionsRefused(logger, config.QueueName, ex.GetType().Name, ex);
            }
        }

        await sender.SendAsync(copy, timeout.Token);
        await receiver.CompleteAsync(deadLetter, timeout.Token);
    }

    async Task RemoveAsync(IServiceBusDeadLetterReceiver receiver, ServiceBusReceivedMessage deadLetter, CancellationToken cancellationToken)
    {
        using var timeout = Bounded(cancellationToken);
        await receiver.CompleteAsync(deadLetter, timeout.Token);
    }

    // Each broker call is bounded by SendTimeout, like every other send and settlement.
    CancellationTokenSource Bounded(CancellationToken cancellationToken)
    {
        var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(config.SendTimeout);
        return timeout;
    }

    sealed class Scan : IDeadLetterScan
    {
        readonly ServiceBusDeadLetterStore store;
        readonly IServiceBusDeadLetterReceiver receiver;
        readonly bool browseOnly;
        readonly Queue<Entry> buffer = new();
        readonly HashSet<long> seen = [];
        readonly List<Entry> held = [];
        bool ended;

        public Scan(ServiceBusDeadLetterStore store, IServiceBusDeadLetterReceiver receiver, bool browseOnly)
        {
            this.store = store;
            this.receiver = receiver;
            this.browseOnly = browseOnly;
        }

        public async Task<IDeadLetterEntry?> NextAsync(CancellationToken cancellationToken)
        {
            if (buffer.Count == 0 && !ended)
                await FetchAsync(cancellationToken);
            return buffer.TryDequeue(out var entry) ? entry : null;
        }

        async Task FetchAsync(CancellationToken cancellationToken)
        {
            if (held.Count(e => !e.Settled) >= MaxHeldMessages)
            {
                ended = true;
                return;
            }

            var page = browseOnly
                ? await receiver.PeekAsync(PeekPageSize, cancellationToken)
                : await receiver.ReceiveAsync(ReceiveBatchSize, ReceiveWait, cancellationToken);
            if (page.Count == 0)
            {
                ended = true;
                return;
            }

            foreach (var message in page)
            {
                var entry = new Entry(store, receiver, message, browseOnly);
                if (!browseOnly)
                    held.Add(entry);
                // A message seen before came back because its lock expired: everything has been seen once, so the pass
                // ends (that message is held again and abandoned with the rest).
                if (seen.Add(message.SequenceNumber))
                    buffer.Enqueue(entry);
                else
                    ended = true;
            }
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var entry in held.Where(e => !e.Settled))
            {
                try
                {
                    await receiver.AbandonAsync(entry.Message, CancellationToken.None);
                }
                catch (Exception ex) when (ServiceBusSettlement.IsLockLost(ex))
                {
                    // The lock already expired (the message came around again): the broker has it back.
                }
                catch (Exception ex)
                {
                    AzureServiceBusLog.DeadLetterAbandonFailed(store.logger, store.config.QueueName, ex);
                }
            }

            try
            {
                await receiver.DisposeAsync();
            }
            catch (Exception ex)
            {
                AzureServiceBusLog.CloseFailed(store.logger, store.config.QueueName, ex);
            }
        }
    }

    sealed class Entry : IDeadLetterEntry
    {
        readonly ServiceBusDeadLetterStore store;
        readonly IServiceBusDeadLetterReceiver receiver;
        readonly bool browseOnly;

        public Entry(ServiceBusDeadLetterStore store, IServiceBusDeadLetterReceiver receiver, ServiceBusReceivedMessage message, bool browseOnly)
        {
            this.store = store;
            this.receiver = receiver;
            this.browseOnly = browseOnly;
            Message = message;
            Metadata = ServiceBusMessages.DeadLetterMetadata(message);
        }

        public ServiceBusReceivedMessage Message { get; }
        public bool Settled { get; private set; }
        public DeadLetterMetadata Metadata { get; }
        public ReadOnlyMemory<byte> Body => Message.Body.ToMemory();

        public async Task ReplayAsync(CancellationToken cancellationToken)
        {
            ThrowIfBrowsing();
            await store.ReplayAsync(receiver, Message, cancellationToken);
            Settled = true;
        }

        public async Task RemoveAsync(CancellationToken cancellationToken)
        {
            ThrowIfBrowsing();
            await store.RemoveAsync(receiver, Message, cancellationToken);
            Settled = true;
        }

        void ThrowIfBrowsing()
        {
            if (browseOnly)
                throw new InvalidOperationException("A browse-only pass cannot settle a dead letter");
        }
    }
}
