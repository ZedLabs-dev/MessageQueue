using ZedLabs.MessageQueue.Providers;

namespace ZedLabs.MessageQueue.Telemetry;

// Span attribute and metric names. The "messaging.*", "error.type" names are the OpenTelemetry messaging semantic
// conventions'; the conventions define nothing for retries, dead letters or the attempt, so those use "zedlabs.messagequeue.*".
static class TelemetryNames
{
    public const string System = "messaging.system";
    public const string DestinationName = "messaging.destination.name";
    public const string OperationName = "messaging.operation.name";
    public const string OperationType = "messaging.operation.type";
    public const string MessageId = "messaging.message.id";
    public const string ConversationId = "messaging.message.conversation_id";
    public const string BatchMessageCount = "messaging.batch.message_count";
    public const string ErrorType = "error.type";
    public const string SubscriptionName = "messaging.destination.subscription.name";
    public const string RoutingKey = "messaging.rabbitmq.destination.routing_key";

    public const string MessageType = "zedlabs.messagequeue.message_type";
    public const string DeliveryAttempt = "zedlabs.messagequeue.delivery_attempt";
    public const string DeadLetterReason = "zedlabs.messagequeue.dead_letter.reason";
    public const string RetryNextAttempt = "zedlabs.messagequeue.retry.next_attempt";
    public const string RetryDelay = "zedlabs.messagequeue.retry.delay_ms";

    public const string SentMessages = "messaging.client.sent.messages";
    public const string OperationDuration = "messaging.client.operation.duration";
    public const string ConsumedMessages = "messaging.client.consumed.messages";
    public const string ProcessDuration = "messaging.process.duration";
    public const string RetriedMessages = "zedlabs.messagequeue.messages.retried";
    public const string DeadLetteredMessages = "zedlabs.messagequeue.messages.dead_lettered";
    public const string TransportErrors = "zedlabs.messagequeue.transport.errors";
    public const string ReplayedDeadLetters = "zedlabs.messagequeue.dead_letters.replayed";
    public const string PurgedDeadLetters = "zedlabs.messagequeue.dead_letters.purged";
}
