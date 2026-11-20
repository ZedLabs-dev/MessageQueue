using Microsoft.Extensions.Logging;
using ZedLabs.MessageQueue.Abstractions;
using ZedLabs.MessageQueue.Processing;

namespace ZedLabs.MessageQueue.Samples;

// Shows the three results a processor can return. The hosted service creates a DI scope per message, so this class
// may depend on scoped services such as a DbContext.
public sealed partial class OrderCreatedProcessor : IMessageProcessor<OrderCreated>
{
    readonly ILogger<OrderCreatedProcessor> logger;

    public OrderCreatedProcessor(ILogger<OrderCreatedProcessor> logger)
    {
        this.logger = logger;
    }

    public Task<MessageResult> ProcessAsync(OrderCreated message, MessageContext context, CancellationToken cancellationToken)
    {
        // DeadLetter: the message can never succeed, so retrying is pointless. It goes to the dead-letter queue.
        if (message.Amount <= 0)
        {
            LogRejected(logger, message.OrderId, message.Amount);
            return Task.FromResult(MessageResult.DeadLetter);
        }

        // Retry: a transient problem. The retry policy (MessageQueue:Retry) redelivers it after a backoff, and
        // dead-letters it once MaxRetries is used up. Here the "flaky" customer succeeds on its second attempt.
        if (message.CustomerId == OrderPublisher.FlakyCustomer && context.DeliveryAttempt == 1)
        {
            LogRetrying(logger, message.OrderId, context.DeliveryAttempt);
            return Task.FromResult(MessageResult.Retry);
        }

        // Acknowledge: done; the message is removed from the queue. (A thrown exception counts as Retry.)
        LogProcessed(logger, message.OrderId, context.DeliveryAttempt);
        return Task.FromResult(MessageResult.Acknowledge);
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Order {OrderId} processed on attempt {Attempt}")]
    static partial void LogProcessed(ILogger logger, string orderId, int attempt);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Order {OrderId} failed transiently on attempt {Attempt}; asking for a retry")]
    static partial void LogRetrying(ILogger logger, string orderId, int attempt);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Order {OrderId} has an invalid amount {Amount}; dead-lettering it")]
    static partial void LogRejected(ILogger logger, string orderId, decimal amount);
}
