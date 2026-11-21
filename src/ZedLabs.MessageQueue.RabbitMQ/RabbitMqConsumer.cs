using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ZedLabs.MessageQueue.RabbitMQ;

// The consumer of a subscription. basic.cancel from the broker (for example, its queue was deleted) arrives in
// HandleBasicCancelAsync only; the client's own cancel and channel closes take other paths.
//
// IsRunning alone is not enough for the health check: RabbitMQ.Client 7 completes BasicConsumeAsync once the broker's
// basic.consume-ok arrived, but sets IsRunning only when its dispatcher later hands consume-ok to the consumer. So the
// consumer counts as registered from the moment BasicConsumeAsync returns its tag until the broker cancels it, its own
// cancel is confirmed or its channel shuts down; after a shutdown, automatic recovery registers it again.
// With ConsumerDispatchConcurrency above 1 the client's dispatcher workers handle consume-ok, cancel, cancel-ok and
// shutdown in any order, so a late consume-ok never revives a cancelled consumer.
sealed class RabbitMqConsumer : AsyncEventingBasicConsumer
{
    const int Pending = 0;
    const int Registered = 1;
    const int Cancelled = 2;
    const int ShutDown = 3;

    readonly Func<Task> cancelledByBroker;
    int state;

    public RabbitMqConsumer(IChannel channel, Func<Task> cancelledByBroker) : base(channel)
    {
        this.cancelledByBroker = cancelledByBroker;
        // Raised for every shutdown of the channel, also after a broker cancel removed this consumer from the client's
        // dispatcher (which then hands it no shutdown): automatic recovery still registers it again.
        channel.ChannelShutdownAsync += (_, _) =>
        {
            Volatile.Write(ref state, ShutDown);
            return Task.CompletedTask;
        };
    }

    // Test seam: awaited before a dispatched consume-ok is handled.
    public Func<Task>? BeforeConsumeOk { get; set; }

    // Whether the broker has this consumer registered.
    public bool IsConsuming => Volatile.Read(ref state) == Registered;

    // What the health check reports for a subscription: its channel is open and its consumer registered.
    public bool IsHealthy => Channel.IsOpen && IsConsuming;

    // Registers the consumer on its channel and returns its consumer tag.
    public async Task<string> StartAsync(string queue, CancellationToken cancellationToken)
    {
        var tag = await Channel.BasicConsumeAsync(queue, autoAck: false, this, cancellationToken);
        // Only from Pending: a cancel or shutdown handled before this continuation ran must not be undone.
        Interlocked.CompareExchange(ref state, Registered, Pending);
        return tag;
    }

    public override async Task HandleBasicConsumeOkAsync(string consumerTag, CancellationToken cancellationToken = default)
    {
        if (BeforeConsumeOk is { } hold)
            await hold();
        await base.HandleBasicConsumeOkAsync(consumerTag, cancellationToken);
        // From Pending (the first registration) or ShutDown (recovery), never from Cancelled.
        if (Interlocked.CompareExchange(ref state, Registered, Pending) != Pending)
            Interlocked.CompareExchange(ref state, Registered, ShutDown);
    }

    public override async Task HandleBasicCancelAsync(string consumerTag, CancellationToken cancellationToken = default)
    {
        try
        {
            await cancelledByBroker();
        }
        finally
        {
            await base.HandleBasicCancelAsync(consumerTag, cancellationToken);
        }
    }

    public override async Task HandleChannelShutdownAsync(object channel, ShutdownEventArgs reason)
    {
        await base.HandleChannelShutdownAsync(channel, reason);
        Volatile.Write(ref state, ShutDown);
    }

    // Broker cancel, cancel-ok and (before the override above marks it ShutDown) channel shutdown. A cancel handled
    // after a shutdown is stale: the consumer stays recoverable.
    protected override async Task OnCancelAsync(string[] consumerTags, CancellationToken cancellationToken = default)
    {
        if (Interlocked.CompareExchange(ref state, Cancelled, Registered) != Registered)
            Interlocked.CompareExchange(ref state, Cancelled, Pending);
        await base.OnCancelAsync(consumerTags, cancellationToken);
    }
}
