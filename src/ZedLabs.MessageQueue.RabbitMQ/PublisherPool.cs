using System.Threading.RateLimiting;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using ZedLabs.MessageQueue.Exceptions;
using ZedLabs.MessageQueue.Processing;

namespace ZedLabs.MessageQueue.RabbitMQ;

// Publisher channels, picked round-robin (B1). Confirms are tracked by RabbitMQ.Client 7 itself
// (publisherConfirmationTrackingEnabled): BasicPublishAsync completes when the broker acks and throws
// PublishException on a nack or a mandatory return. This class only bounds the wait and maps the failures.
sealed class PublisherPool : IAsyncDisposable
{
    readonly IChannel[] channels;
    readonly IDisposable[] limiters;
    readonly bool confirms;
    readonly TimeSpan confirmTimeout;
    int next = -1;

    public PublisherPool(IChannel[] channels, bool confirms, TimeSpan confirmTimeout, IDisposable[]? limiters = null)
    {
        this.channels = channels;
        this.confirms = confirms;
        this.confirmTimeout = confirmTimeout;
        this.limiters = limiters ?? [];
    }

    public bool Confirms => confirms;

    public static async Task<PublisherPool> CreateAsync(
        IConnection connection,
        int size,
        bool confirms,
        int maxOutstandingConfirms,
        TimeSpan confirmTimeout,
        CancellationToken cancellationToken)
    {
        var channels = new List<IChannel>(size);
        var limiters = new List<IDisposable>(size);
        try
        {
            for (var i = 0; i < size; i++)
            {
                // Publishes beyond the outstanding-confirm limit wait in line (no fail-fast, no throttling sleeps,
                // unlike the client's default ThrottlingRateLimiter); the wait counts towards ConfirmTimeout.
                var limiter = confirms
                    ? new ConcurrencyLimiter(new ConcurrencyLimiterOptions
                    {
                        PermitLimit = maxOutstandingConfirms,
                        QueueLimit = int.MaxValue,
                        QueueProcessingOrder = QueueProcessingOrder.OldestFirst
                    })
                    : null;
                if (limiter is not null)
                    limiters.Add(limiter);

                channels.Add(await connection.CreateChannelAsync(
                    new CreateChannelOptions(
                        publisherConfirmationsEnabled: confirms,
                        publisherConfirmationTrackingEnabled: confirms,
                        outstandingPublisherConfirmationsRateLimiter: limiter),
                    cancellationToken));
            }
        }
        catch
        {
            await new PublisherPool([.. channels], confirms, confirmTimeout, [.. limiters]).DisposeAsync();
            throw;
        }

        return new PublisherPool([.. channels], confirms, confirmTimeout, [.. limiters]);
    }

    public async Task PublishAsync(
        string exchange,
        string routingKey,
        bool mandatory,
        BasicProperties properties,
        ReadOnlyMemory<byte> body,
        CancellationToken cancellationToken)
    {
        var channel = channels[RoundRobin.Next(ref next, channels.Length)];
        if (!confirms)
        {
            await channel.BasicPublishAsync(exchange, routingKey, mandatory, properties, body, cancellationToken);
            return;
        }

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(confirmTimeout);
        try
        {
            await channel.BasicPublishAsync(exchange, routingKey, mandatory, properties, body, timeout.Token);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested && timeout.IsCancellationRequested)
        {
            throw new MessageQueueException($"Publisher confirm timed out after {confirmTimeout.TotalSeconds} seconds");
        }
        catch (PublishException ex) when (ex.IsReturn)
        {
            throw new PublishRefusedException($"Message was returned by the broker: no queue is bound for routing key '{routingKey}'", ex);
        }
        catch (PublishException ex)
        {
            throw new PublishRefusedException("Message was nacked by the broker", ex);
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var channel in channels)
        {
            try
            {
                await channel.CloseAsync();
            }
            catch (Exception)
            {
                // Already closed with its connection; disposing below releases it either way.
            }
            await channel.DisposeAsync();
        }

        foreach (var limiter in limiters)
        {
            limiter.Dispose();
        }
    }
}
