using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace ZedLabs.MessageQueue.RabbitMQ;

// Settles one delivery at most once: a second ack or nack of the same tag would close the whole channel.
sealed class Settlement
{
    readonly IChannel channel;
    readonly ulong deliveryTag;
    readonly ILogger logger;
    readonly string queueName;
    int settled;

    public Settlement(IChannel channel, ulong deliveryTag, ILogger logger, string queueName)
    {
        this.channel = channel;
        this.deliveryTag = deliveryTag;
        this.logger = logger;
        this.queueName = queueName;
    }

    public bool IsSettled => Volatile.Read(ref settled) == 1;

    public async Task AckAsync()
    {
        if (Interlocked.Exchange(ref settled, 1) == 0)
            await channel.BasicAckAsync(deliveryTag, multiple: false);
    }

    // Best effort: if the channel is gone, the broker has already requeued the delivery.
    public async Task RequeueAsync()
    {
        if (Interlocked.Exchange(ref settled, 1) == 1)
            return;
        try
        {
            await channel.BasicNackAsync(deliveryTag, multiple: false, requeue: true);
        }
        catch (Exception ex)
        {
            RabbitMqLog.RequeueFailed(logger, queueName, ex);
        }
    }
}
