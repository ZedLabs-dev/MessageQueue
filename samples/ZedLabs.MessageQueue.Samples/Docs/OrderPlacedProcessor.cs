#region snippet:processor
using ZedLabs.MessageQueue.Abstractions;
using ZedLabs.MessageQueue.Processing;
#endregion

namespace ZedLabs.MessageQueue.Samples.Docs;

#region snippet:processor
public sealed class OrderPlacedProcessor : IMessageProcessor<OrderPlaced>
{
    readonly IOrderLedger ledger;

    public OrderPlacedProcessor(IOrderLedger ledger)   // scoped: a new DI scope, and processor, per message
    {
        this.ledger = ledger;
    }

    public async Task<MessageResult> ProcessAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken)
    {
        // Can never succeed: dead-letter it now, without retrying (reason code "Rejected").
        if (message.Amount <= 0)
            return MessageResult.DeadLetter;

        // Delivery is at-least-once, and a redelivered message keeps its MessageId: use it as the idempotency key,
        // recorded in the same transaction as the work. An id seen before is a duplicate: acknowledge it, do nothing.
        if (!await ledger.RecordOnceAsync(context.MessageId, message, cancellationToken))
            return MessageResult.Acknowledge;

        // Your work here. An exception thrown counts as MessageResult.Retry: redelivered after the retry policy's
        // backoff, and dead-lettered ("MaxRetriesExceeded") once Retry:MaxRetries retries have failed.
        return MessageResult.Acknowledge;
    }
}
#endregion
