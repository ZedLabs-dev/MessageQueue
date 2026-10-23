using Azure.Core;
using Azure.Identity;
using Azure.Messaging.ServiceBus;

namespace ESE.MessageQueue.AzureServiceBus;

// Thin seams over the Service Bus SDK, so the provider's behavior is unit-tested without a namespace. The SDK-backed
// implementations below only forward; every decision lives in AzureServiceBusMessageQueue<T>.
interface IServiceBusTransportFactory
{
    IServiceBusTransport Create(AzureServiceBusConfig config, TokenCredential? credential);
}

// One per queue: wraps the ServiceBusClient (one AMQP connection, shared by every sender and processor).
interface IServiceBusTransport : IAsyncDisposable
{
    IServiceBusSender CreateSender(string entityPath);

    // Queue: subscriptionName is null. Topic: the subscription to receive from.
    IServiceBusProcessor CreateProcessor(string entityPath, string? subscriptionName, ServiceBusProcessorOptions options);

    IServiceBusPeeker CreatePeeker(string entityPath, string? subscriptionName);

    // A PeekLock receiver on the entity's dead-letter subqueue (ADR 0023); one per pass, so its peek cursor starts fresh.
    IServiceBusDeadLetterReceiver CreateDeadLetterReceiver(string entityPath, string? subscriptionName);
}

interface IServiceBusSender : IAsyncDisposable
{
    Task SendAsync(ServiceBusMessage message, CancellationToken cancellationToken);

    ValueTask<ServiceBusMessageBatch> CreateBatchAsync(CancellationToken cancellationToken);

    Task SendBatchAsync(ServiceBusMessageBatch batch, CancellationToken cancellationToken);
}

// One per subscription: the SDK does not allow handlers to be replaced on a processor.
interface IServiceBusProcessor : IAsyncDisposable
{
    bool IsProcessing { get; }

    Task StartAsync(Func<IServiceBusDelivery, Task> onMessage, Func<ProcessErrorEventArgs, Task> onError, CancellationToken cancellationToken);

    // Like StopProcessingAsync: cancels the deliveries' tokens and waits for running handlers.
    Task StopAsync(CancellationToken cancellationToken);
}

// The settlement actions of one received message (ProcessMessageEventArgs).
interface IServiceBusDelivery
{
    ServiceBusReceivedMessage Message { get; }

    CancellationToken CancellationToken { get; }

    Task CompleteAsync(CancellationToken cancellationToken);

    Task AbandonAsync(IDictionary<string, object>? propertiesToModify, CancellationToken cancellationToken);

    Task DeadLetterAsync(IDictionary<string, object> propertiesToModify, string reason, string description, CancellationToken cancellationToken);
}

// The dead-letter subqueue: browse without locking, or receive under a lock and settle each message once.
interface IServiceBusDeadLetterReceiver : IAsyncDisposable
{
    // Continues after the last message this receiver peeked.
    Task<IReadOnlyList<ServiceBusReceivedMessage>> PeekAsync(int maxMessages, CancellationToken cancellationToken);

    Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveAsync(int maxMessages, TimeSpan maxWaitTime, CancellationToken cancellationToken);

    Task CompleteAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken);

    Task AbandonAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken);
}

interface IServiceBusPeeker : IAsyncDisposable
{
    Task PeekAsync(CancellationToken cancellationToken);
}

sealed class ServiceBusTransportFactory : IServiceBusTransportFactory
{
    public static readonly ServiceBusTransportFactory Instance = new();

    public IServiceBusTransport Create(AzureServiceBusConfig config, TokenCredential? credential) =>
        new ServiceBusTransport(CreateClient(config, credential));

    // Identity (FullyQualifiedNamespace + TokenCredential, DefaultAzureCredential unless one is supplied) or a SAS
    // connection string; the validator guarantees exactly one is set. Neither connects until first use.
    internal static ServiceBusClient CreateClient(AzureServiceBusConfig config, TokenCredential? credential) =>
        string.IsNullOrWhiteSpace(config.FullyQualifiedNamespace)
            ? new ServiceBusClient(config.ConnectionString)
            : new ServiceBusClient(config.FullyQualifiedNamespace.Trim(), ResolveCredential(credential));

    // One DefaultAzureCredential per process: it caches tokens, and building one per queue repeats the probing.
    static readonly Lazy<TokenCredential> DefaultCredential = new(() => new DefaultAzureCredential());

    internal static TokenCredential ResolveCredential(TokenCredential? credential) => credential ?? DefaultCredential.Value;
}

sealed class ServiceBusTransport : IServiceBusTransport
{
    readonly ServiceBusClient client;

    public ServiceBusTransport(ServiceBusClient client)
    {
        this.client = client;
    }

    public IServiceBusSender CreateSender(string entityPath) => new Sender(client.CreateSender(entityPath));

    public IServiceBusProcessor CreateProcessor(string entityPath, string? subscriptionName, ServiceBusProcessorOptions options) =>
        new Processor(subscriptionName is null
            ? client.CreateProcessor(entityPath, options)
            : client.CreateProcessor(entityPath, subscriptionName, options));

    public IServiceBusPeeker CreatePeeker(string entityPath, string? subscriptionName) =>
        new Peeker(subscriptionName is null
            ? client.CreateReceiver(entityPath)
            : client.CreateReceiver(entityPath, subscriptionName));

    public IServiceBusDeadLetterReceiver CreateDeadLetterReceiver(string entityPath, string? subscriptionName)
    {
        var options = new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter, ReceiveMode = ServiceBusReceiveMode.PeekLock, PrefetchCount = 0 };
        return new DeadLetterReceiver(subscriptionName is null
            ? client.CreateReceiver(entityPath, options)
            : client.CreateReceiver(entityPath, subscriptionName, options));
    }

    public ValueTask DisposeAsync() => client.DisposeAsync();

    internal sealed class DeadLetterReceiver : IServiceBusDeadLetterReceiver
    {
        readonly ServiceBusReceiver receiver;

        public DeadLetterReceiver(ServiceBusReceiver receiver)
        {
            this.receiver = receiver;
        }

        internal ServiceBusReceiver Inner => receiver;

        public async Task<IReadOnlyList<ServiceBusReceivedMessage>> PeekAsync(int maxMessages, CancellationToken cancellationToken) =>
            await receiver.PeekMessagesAsync(maxMessages, cancellationToken: cancellationToken);

        public async Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveAsync(int maxMessages, TimeSpan maxWaitTime, CancellationToken cancellationToken) =>
            await receiver.ReceiveMessagesAsync(maxMessages, maxWaitTime, cancellationToken);

        public Task CompleteAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken) =>
            receiver.CompleteMessageAsync(message, cancellationToken);

        public Task AbandonAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken) =>
            receiver.AbandonMessageAsync(message, cancellationToken: cancellationToken);

        public ValueTask DisposeAsync() => receiver.DisposeAsync();
    }

    internal sealed class Sender : IServiceBusSender
    {
        readonly ServiceBusSender sender;

        public Sender(ServiceBusSender sender)
        {
            this.sender = sender;
        }

        internal ServiceBusSender Inner => sender;

        public Task SendAsync(ServiceBusMessage message, CancellationToken cancellationToken) =>
            sender.SendMessageAsync(message, cancellationToken);

        public ValueTask<ServiceBusMessageBatch> CreateBatchAsync(CancellationToken cancellationToken) =>
            sender.CreateMessageBatchAsync(cancellationToken);

        public Task SendBatchAsync(ServiceBusMessageBatch batch, CancellationToken cancellationToken) =>
            sender.SendMessagesAsync(batch, cancellationToken);

        public ValueTask DisposeAsync() => sender.DisposeAsync();
    }

    internal sealed class Processor : IServiceBusProcessor
    {
        readonly ServiceBusProcessor processor;

        public Processor(ServiceBusProcessor processor)
        {
            this.processor = processor;
        }

        internal ServiceBusProcessor Inner => processor;

        public bool IsProcessing => processor.IsProcessing && !processor.IsClosed;

        public Task StartAsync(Func<IServiceBusDelivery, Task> onMessage, Func<ProcessErrorEventArgs, Task> onError, CancellationToken cancellationToken)
        {
            processor.ProcessMessageAsync += args => onMessage(new Delivery(args));
            processor.ProcessErrorAsync += onError;
            return processor.StartProcessingAsync(cancellationToken);
        }

        public Task StopAsync(CancellationToken cancellationToken) => processor.StopProcessingAsync(cancellationToken);

        public ValueTask DisposeAsync() => processor.DisposeAsync();
    }

    internal sealed class Delivery : IServiceBusDelivery
    {
        readonly ProcessMessageEventArgs args;

        public Delivery(ProcessMessageEventArgs args)
        {
            this.args = args;
        }

        public ServiceBusReceivedMessage Message => args.Message;

        public CancellationToken CancellationToken => args.CancellationToken;

        public Task CompleteAsync(CancellationToken cancellationToken) =>
            args.CompleteMessageAsync(args.Message, cancellationToken);

        public Task AbandonAsync(IDictionary<string, object>? propertiesToModify, CancellationToken cancellationToken) =>
            args.AbandonMessageAsync(args.Message, propertiesToModify, cancellationToken);

        public Task DeadLetterAsync(IDictionary<string, object> propertiesToModify, string reason, string description, CancellationToken cancellationToken) =>
            args.DeadLetterMessageAsync(args.Message, propertiesToModify, reason, description, cancellationToken);
    }

    internal sealed class Peeker : IServiceBusPeeker
    {
        readonly ServiceBusReceiver receiver;

        public Peeker(ServiceBusReceiver receiver)
        {
            this.receiver = receiver;
        }

        internal ServiceBusReceiver Inner => receiver;

        // Needs only Listen rights. An empty entity peeks null, which is still a successful round trip.
        public Task PeekAsync(CancellationToken cancellationToken) => receiver.PeekMessageAsync(cancellationToken: cancellationToken);

        public ValueTask DisposeAsync() => receiver.DisposeAsync();
    }
}
