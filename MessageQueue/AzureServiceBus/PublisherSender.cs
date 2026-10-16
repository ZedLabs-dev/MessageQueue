using Azure.Messaging.ServiceBus;
using ESE.MessageQueue.Exceptions;

namespace ESE.MessageQueue.AzureServiceBus;

sealed class PublisherSender : IAsyncDisposable
{
    readonly ServiceBusSender sender;
    readonly SemaphoreSlim concurrencySemaphore;
    readonly TimeSpan sendTimeout;
    bool disposed;

    public PublisherSender(
        ServiceBusClient client,
        string queueOrTopicName,
        int maxConcurrentSends,
        TimeSpan sendTimeout)
    {
        this.sendTimeout = sendTimeout;
        concurrencySemaphore = new SemaphoreSlim(maxConcurrentSends, maxConcurrentSends);
        sender = client.CreateSender(queueOrTopicName);
    }

    public async Task SendAsync(ServiceBusMessage message, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        await concurrencySemaphore.WaitAsync(cancellationToken);

        try
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(sendTimeout);

            await sender.SendMessageAsync(message, timeoutCts.Token);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new MessageQueueException($"Send operation timed out after {sendTimeout.TotalSeconds} seconds");
        }
        finally
        {
            concurrencySemaphore.Release();
        }
    }

    public async Task SendBatchAsync(IEnumerable<ServiceBusMessage> messages, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        await concurrencySemaphore.WaitAsync(cancellationToken);

        try
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(sendTimeout);

            await sender.SendMessagesAsync(messages, timeoutCts.Token);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new MessageQueueException($"Batch send operation timed out after {sendTimeout.TotalSeconds} seconds");
        }
        finally
        {
            concurrencySemaphore.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (disposed) return;
        disposed = true;

        await sender.DisposeAsync();
        concurrencySemaphore.Dispose();
    }
}

