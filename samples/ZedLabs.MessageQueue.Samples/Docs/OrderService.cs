#region snippet:publisher
using ZedLabs.MessageQueue.Abstractions;
#endregion

namespace ZedLabs.MessageQueue.Samples.Docs;

#region snippet:publisher
public sealed class OrderService
{
    readonly IMessagePublisher<OrderPlaced> publisher;

    public OrderService(IMessagePublisher<OrderPlaced> publisher)
    {
        this.publisher = publisher;
    }

    public Task PlaceAsync(OrderPlaced order, string checkoutId, CancellationToken cancellationToken) =>
        publisher.PublishAsync(order, new PublishOptions
        {
            MessageId = order.OrderId,                    // the idempotency key consumers see as MessageContext.MessageId
            CorrelationId = checkoutId,                   // at most 128 characters, no control characters
            Headers = new Dictionary<string, string> { ["tenant"] = "contoso" }   // names starting with "x-" are reserved
        }, cancellationToken);
}
#endregion
