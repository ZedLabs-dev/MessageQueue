using System.Globalization;
using System.Text;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using ZedLabs.MessageQueue.Abstractions;
using ZedLabs.MessageQueue.Providers;

namespace ZedLabs.MessageQueue.RabbitMQ;

// ADR 0023: the dead-letter queue as a store. A pass reads it with basic.get (no ack) on its own channel, so a message
// it skipped stays unacked and is not delivered to it again (a replayed message dead-lettered again during the pass is a
// new delivery: DeadLetterQueue<T> skips its message id). The queue's depth is not a bound: right after an earlier
// pass's nacks a quorum queue can still count them as unacked. Whatever the pass still holds when it ends goes back
// with an explicit nack + requeue: a quorum queue counts a channel close as a failed delivery, and the dead-letter
// queue's delivery limit (20 by default, no dead-letter exchange behind it) would then drop the message.
sealed class RabbitMqDeadLetterStore : IDeadLetterStore
{
    // The library's dead-letter headers (RabbitMqMessageQueue) and the broker's own.
    internal const string DeadLetteredAtHeader = "x-zedlabs-dead-lettered-at";
    const string FirstDeathReasonHeader = "x-first-death-reason";
    const string DeathHeader = "x-death";

    readonly string queueName;
    readonly string deadLetterQueueName;
    readonly bool enabled;
    readonly Func<CancellationToken, ValueTask<(IConnection Amqp, PublisherPool Confirmed)>> connect;
    readonly ILogger logger;

    public RabbitMqDeadLetterStore(
        string queueName,
        string deadLetterQueueName,
        bool enabled,
        Func<CancellationToken, ValueTask<(IConnection Amqp, PublisherPool Confirmed)>> connect,
        ILogger logger)
    {
        this.queueName = queueName;
        this.deadLetterQueueName = deadLetterQueueName;
        this.enabled = enabled;
        this.connect = connect;
        this.logger = logger;
    }

    // Without EnableDeadLetterQueue there is no dead-letter queue to operate on.
    public static DeadLetterOperations OperationsFor(bool enabled) => enabled ? DeadLetterOperations.All : DeadLetterOperations.None;

    public DeadLetterOperations Operations => OperationsFor(enabled);

    // The dead-letter queue's arguments. A quorum dead-letter queue has no delivery limit (-1): a dead letter a pass holds
    // when its channel is lost goes back counted as a failed delivery, and with the default limit (20) and no
    // dead-letter exchange behind it the 21st such return would drop it. Main and retry queues keep their limits: the
    // main queue's dead-letters to this queue, and retry queues have no consumers (their messages expire).
    internal static Dictionary<string, object?> DeadLetterQueueArguments(RabbitMqConfig config) =>
        config.UseQuorumQueues
            ? new Dictionary<string, object?>
            {
                ["x-queue-type"] = "quorum",
                ["x-quorum-initial-group-size"] = config.QuorumInitialGroupSize,
                ["x-delivery-limit"] = -1
            }
            : [];

    public async Task<IDeadLetterScan> OpenScanAsync(bool browseOnly, CancellationToken cancellationToken)
    {
        var (amqp, confirmed) = await connect(cancellationToken);
        var channel = await amqp.CreateChannelAsync(cancellationToken: cancellationToken);
        return new Scan(this, channel, confirmed, browseOnly);
    }

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

    // What a replay publishes: a fresh first delivery. Every "x-" header goes (attempt, dead-letter reason, x-death and
    // whatever a broker or client added), whatever its case, so no attempt can be inherited or read as forged; the
    // message id, correlation id, publish time, content type, application and trace headers stay.
    internal static BasicProperties ReplayProperties(IReadOnlyBasicProperties original)
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

    // What the dead-letter operations report: the library's headers first, the broker's dead-lettering otherwise.
    internal static DeadLetterMetadata Metadata(IReadOnlyBasicProperties properties)
    {
        var headers = new Dictionary<string, string>();
        string? reason = null, brokerReason = null, detail = null;
        DateTimeOffset? deadLetteredAt = null, brokerDeathTime = null;
        int? attempt = null;

        if (properties.Headers is not null)
        {
            foreach (var (key, value) in properties.Headers)
            {
                switch (key)
                {
                    case RabbitMqMessageQueue<object>.DeadLetterReasonHeader:
                        reason = Text(value);
                        break;
                    case RabbitMqMessageQueue<object>.DeadLetterDetailHeader:
                        detail = Text(value);
                        break;
                    case DeadLetteredAtHeader:
                        deadLetteredAt = Text(value) is { } ms && long.TryParse(ms, NumberStyles.Integer, CultureInfo.InvariantCulture, out var unix)
                            && unix >= DateTimeOffset.MinValue.ToUnixTimeMilliseconds() && unix <= DateTimeOffset.MaxValue.ToUnixTimeMilliseconds()
                            ? DateTimeOffset.FromUnixTimeMilliseconds(unix)
                            : null;
                        break;
                    case RabbitMqMessageQueue<object>.DeliveryAttemptHeader:
                        attempt = Text(value) is { } n && int.TryParse(n, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) && parsed > 0 ? parsed : null;
                        break;
                    case FirstDeathReasonHeader:
                        brokerReason = Text(value);
                        break;
                    case DeathHeader:
                        brokerDeathTime = FirstDeathTime(value);
                        break;
                    default:
                        if (!PublishValidation.IsReserved(key) && Text(value) is { } text)
                            headers[key] = text;
                        break;
                }
            }
        }

        return new DeadLetterMetadata
        {
            MessageId = properties.MessageId ?? string.Empty,
            CorrelationId = properties.CorrelationId,
            ReasonCode = reason ?? brokerReason,
            ReasonDetail = detail,
            EnqueuedAt = properties.Timestamp.UnixTime > 0 ? DateTimeOffset.FromUnixTimeSeconds(Math.Min(properties.Timestamp.UnixTime, DateTimeOffset.MaxValue.ToUnixTimeSeconds())) : null,
            DeadLetteredAt = deadLetteredAt ?? brokerDeathTime,
            DeliveryAttempt = attempt,
            Headers = headers
        };
    }

    // The broker's dead-letter history: a list of tables, the first entry the latest death, with its "time".
    static DateTimeOffset? FirstDeathTime(object? deaths) =>
        deaths is IEnumerable<object?> entries
        && entries.FirstOrDefault() is IDictionary<string, object?> death
        && death.TryGetValue("time", out var time) && time is AmqpTimestamp { UnixTime: > 0 } stamp
        && stamp.UnixTime <= DateTimeOffset.MaxValue.ToUnixTimeSeconds()
            ? DateTimeOffset.FromUnixTimeSeconds(stamp.UnixTime)
            : null;

    static string? Text(object? value) => value switch
    {
        byte[] bytes => Encoding.UTF8.GetString(bytes),
        string s => s,
        IConvertible convertible => convertible.ToString(CultureInfo.InvariantCulture),
        _ => null
    };

    // What a pass keeps of a dead letter it returned: the delivery tag, not the body (a filtered pass over a large queue
    // holds many). Once the pass's channel shuts down (connection lost), its tags are void: the broker has requeued the
    // deliveries, the pass ends, and its entries can no longer be replayed or removed.
    sealed class Scan : IDeadLetterScan
    {
        readonly RabbitMqDeadLetterStore store;
        readonly IChannel channel;
        readonly PublisherPool confirmed;
        readonly bool browseOnly;
        readonly List<Held> held = [];
        volatile bool shutDown;

        public Scan(RabbitMqDeadLetterStore store, IChannel channel, PublisherPool confirmed, bool browseOnly)
        {
            this.store = store;
            this.channel = channel;
            this.confirmed = confirmed;
            this.browseOnly = browseOnly;
            channel.ChannelShutdownAsync += OnShutdownAsync;
        }

        bool Lost => shutDown || !channel.IsOpen;

        Task OnShutdownAsync(object sender, ShutdownEventArgs args)
        {
            shutDown = true;
            return Task.CompletedTask;
        }

        public async Task<IDeadLetterEntry?> NextAsync(CancellationToken cancellationToken)
        {
            if (Lost)
                return null;
            BasicGetResult? result;
            try
            {
                result = await channel.BasicGetAsync(store.deadLetterQueueName, autoAck: false, cancellationToken);
            }
            catch (Exception) when (Lost && !cancellationToken.IsCancellationRequested)
            {
                return null;
            }
            if (result is null)
                return null;

            var handle = new Held(result.DeliveryTag);
            held.Add(handle);
            return new Entry(this, handle, result.BasicProperties, result.Body.ToArray());
        }

        public async ValueTask DisposeAsync()
        {
            channel.ChannelShutdownAsync -= OnShutdownAsync;
            if (!Lost)
            {
                foreach (var handle in held.Where(h => !h.Settled))
                {
                    try
                    {
                        await channel.BasicNackAsync(handle.DeliveryTag, multiple: false, requeue: true);
                    }
                    catch (Exception ex)
                    {
                        RabbitMqLog.DeadLetterReturnFailed(store.logger, store.queueName, ex);
                        break;
                    }
                }
            }
            // A lost channel has nothing to close (closing it waits for the recovering connection).
            if (Lost)
                await channel.DisposeAsync();
            else
                await store.CloseQuietlyAsync(channel);
        }

        void ThrowIfUnusable()
        {
            if (browseOnly)
                throw new InvalidOperationException("A browse-only pass cannot settle a dead letter");
            if (Lost)
                throw new InvalidOperationException("The pass's channel was closed; the broker has returned the dead letter");
        }

        sealed class Held(ulong deliveryTag)
        {
            public ulong DeliveryTag { get; } = deliveryTag;
            public bool Settled { get; set; }
        }

        sealed class Entry : IDeadLetterEntry
        {
            readonly Scan scan;
            readonly Held handle;
            readonly IReadOnlyBasicProperties properties;
            readonly byte[] body;

            public Entry(Scan scan, Held handle, IReadOnlyBasicProperties properties, byte[] body)
            {
                this.scan = scan;
                this.handle = handle;
                this.properties = properties;
                this.body = body;
                Metadata = RabbitMqDeadLetterStore.Metadata(properties);
            }

            public DeadLetterMetadata Metadata { get; }
            public ReadOnlyMemory<byte> Body => body;

            // Through the default exchange straight to the queue (a custom exchange could route it to other queues
            // too), mandatory and confirmed; only then is the dead letter acked, on its own tag only. A refused or
            // unconfirmed publish throws and the dead letter stays (nacked back when the pass ends): a duplicate at
            // worst, never a loss.
            public async Task ReplayAsync(CancellationToken cancellationToken)
            {
                scan.ThrowIfUnusable();
                await scan.confirmed.PublishAsync(string.Empty, scan.store.queueName, mandatory: true, ReplayProperties(properties), body, cancellationToken);
                scan.ThrowIfUnusable();
                await scan.channel.BasicAckAsync(handle.DeliveryTag, multiple: false, CancellationToken.None);
                handle.Settled = true;
            }

            public async Task RemoveAsync(CancellationToken cancellationToken)
            {
                scan.ThrowIfUnusable();
                await scan.channel.BasicAckAsync(handle.DeliveryTag, multiple: false, CancellationToken.None);
                handle.Settled = true;
            }
        }
    }
}
