namespace ESE.MessageQueue.RabbitMQ;

// The error.type values RabbitMQ records in ese.messagequeue.transport.errors: a fixed set (ADR 0021), so a broker's
// reply text never becomes a metric tag. Listed in docs/operations/monitoring.md.
static class RabbitMqTransportErrors
{
    // Connecting or declaring the topology failed (event 2003); calls that fail fast during the backoff are not counted.
    public const string ConnectionFailed = "ConnectionFailed";

    // The open connection was closed by the broker or lost (missed heartbeats, network); the client then recovers it.
    public const string ConnectionShutdown = "ConnectionShutdown";

    // One automatic recovery attempt failed; the client tries again after NetworkRecoveryInterval.
    public const string RecoveryFailed = "RecoveryFailed";

    // The subscription's channel was closed while its connection stayed open (a channel error such as consumer_timeout).
    public const string ChannelShutdown = "ChannelShutdown";

    // The broker cancelled the subscription's consumer (its queue was deleted).
    public const string ConsumerCancelled = "ConsumerCancelled";
}
