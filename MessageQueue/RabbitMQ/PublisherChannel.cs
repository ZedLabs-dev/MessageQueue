using System.Collections.Concurrent;
using ESE.MessageQueue.Exceptions;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ESE.MessageQueue.RabbitMQ;

internal sealed class PublisherChannel : IDisposable
{
    readonly IModel channel;
    readonly TimeSpan confirmTimeout;
    readonly SemaphoreSlim outstandingConfirmsSemaphore;
    readonly ConcurrentDictionary<ulong, TaskCompletionSource<bool>> pendingConfirms = new();
    readonly object publishLock = new();
    readonly bool enableConfirms;
    bool disposed;

    public PublisherChannel(
        IConnection connection,
        bool enableConfirms,
        TimeSpan confirmTimeout,
        int maxOutstandingConfirms)
    {
        this.enableConfirms = enableConfirms;
        this.confirmTimeout = confirmTimeout;
        outstandingConfirmsSemaphore = new SemaphoreSlim(maxOutstandingConfirms, maxOutstandingConfirms);

        channel = connection.CreateModel();

        if (enableConfirms)
        {
            channel.ConfirmSelect();
            channel.BasicAcks += OnBasicAcks;
            channel.BasicNacks += OnBasicNacks;
            channel.ModelShutdown += OnModelShutdown;
        }
    }

    public async Task PublishAsync(
        ReadOnlyMemory<byte> body,
        IBasicProperties properties,
        string exchange,
        string routingKey,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        if (!enableConfirms)
        {
            PublishWithoutConfirm(body, properties, exchange, routingKey);
            return;
        }

        await outstandingConfirmsSemaphore.WaitAsync(cancellationToken);

        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        ulong seqNo;

        try
        {
            lock (publishLock)
            {
                seqNo = channel.NextPublishSeqNo;
                pendingConfirms[seqNo] = tcs;

                channel.BasicPublish(
                    exchange: exchange,
                    routingKey: routingKey,
                    basicProperties: properties,
                    body: body);
            }
        }
        catch
        {
            outstandingConfirmsSemaphore.Release();
            throw;
        }

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(confirmTimeout);

        try
        {
            var confirmed = await tcs.Task.WaitAsync(timeoutCts.Token);
            if (!confirmed)
            {
                throw new MessageQueueException("Message was nacked by the broker");
            }
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            pendingConfirms.TryRemove(seqNo, out _);
            throw new MessageQueueException($"Publisher confirm timed out after {confirmTimeout.TotalSeconds} seconds");
        }
        catch (OperationCanceledException)
        {
            pendingConfirms.TryRemove(seqNo, out _);
            throw;
        }
        finally
        {
            outstandingConfirmsSemaphore.Release();
        }
    }

    public IBasicProperties CreateBasicProperties() => channel.CreateBasicProperties();

    void PublishWithoutConfirm(
        ReadOnlyMemory<byte> body,
        IBasicProperties properties,
        string exchange,
        string routingKey)
    {
        lock (publishLock)
        {
            channel.BasicPublish(
                exchange: exchange,
                routingKey: routingKey,
                basicProperties: properties,
                body: body);
        }
    }

    void OnBasicAcks(object? sender, BasicAckEventArgs e)
    {
        HandleConfirmation(e.DeliveryTag, e.Multiple, confirmed: true);
    }

    void OnBasicNacks(object? sender, BasicNackEventArgs e)
    {
        HandleConfirmation(e.DeliveryTag, e.Multiple, confirmed: false);
    }

    void HandleConfirmation(ulong deliveryTag, bool multiple, bool confirmed)
    {
        if (multiple)
        {
            var keysToRemove = pendingConfirms.Keys.Where(k => k <= deliveryTag).ToList();
            foreach (var key in keysToRemove)
            {
                if (pendingConfirms.TryRemove(key, out var tcs))
                {
                    tcs.TrySetResult(confirmed);
                }
            }
        }
        else
        {
            if (pendingConfirms.TryRemove(deliveryTag, out var tcs))
            {
                tcs.TrySetResult(confirmed);
            }
        }
    }

    void OnModelShutdown(object? sender, ShutdownEventArgs e)
    {
        var exception = new MessageQueueException($"Channel was shut down: {e.ReplyText}");
        foreach (var kvp in pendingConfirms)
        {
            if (pendingConfirms.TryRemove(kvp.Key, out var tcs))
            {
                tcs.TrySetException(exception);
            }
        }
    }

    public void Dispose()
    {
        if (disposed) return;
        disposed = true;

        if (enableConfirms)
        {
            channel.BasicAcks -= OnBasicAcks;
            channel.BasicNacks -= OnBasicNacks;
            channel.ModelShutdown -= OnModelShutdown;
        }

        // Fail any remaining pending confirms
        var exception = new ObjectDisposedException(nameof(PublisherChannel));
        foreach (var kvp in pendingConfirms)
        {
            if (pendingConfirms.TryRemove(kvp.Key, out var tcs))
            {
                tcs.TrySetException(exception);
            }
        }

        channel.Dispose();
        outstandingConfirmsSemaphore.Dispose();
    }
}

