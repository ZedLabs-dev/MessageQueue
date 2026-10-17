namespace ESE.MessageQueue.Telemetry;

// Span attribute and metric names. The "messaging.*", "error.type" names are the OpenTelemetry messaging semantic
// conventions'; the conventions define nothing for retries, dead letters or the attempt, so those use "ese.messagequeue.*".
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

    public const string MessageType = "ese.messagequeue.message_type";
    public const string DeliveryAttempt = "ese.messagequeue.delivery_attempt";
    public const string DeadLetterReason = "ese.messagequeue.dead_letter.reason";
    public const string RetryNextAttempt = "ese.messagequeue.retry.next_attempt";
    public const string RetryDelay = "ese.messagequeue.retry.delay_ms";

    public const string SentMessages = "messaging.client.sent.messages";
    public const string OperationDuration = "messaging.client.operation.duration";
    public const string ConsumedMessages = "messaging.client.consumed.messages";
    public const string ProcessDuration = "messaging.process.duration";
    public const string RetriedMessages = "ese.messagequeue.messages.retried";
    public const string DeadLetteredMessages = "ese.messagequeue.messages.dead_lettered";
}
