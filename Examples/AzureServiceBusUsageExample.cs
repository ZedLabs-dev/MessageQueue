using AgentReview.MessageQueue.Abstractions;
using AgentReview.MessageQueue.Extensions;
using AgentReview.MessageQueue.Hosting;
using AgentReview.MessageQueue.Processing;

namespace AgentReview.Examples;

public class AzureServiceBusUsageExample
{
    public static void ConfigureServices(IServiceCollection services, IConfiguration configuration)
    {
        services.AddMessageQueue<OrderCreated>(configuration);
        services.AddMessageProcessor<OrderCreated, OrderCreatedProcessor>();
        services.AddHostedService<MessageQueueHostedService<OrderCreated>>();
    }
}

public record OrderCreated(string OrderId, string CustomerId, decimal Amount, DateTime CreatedAt);

public class OrderCreatedProcessor : IMessageProcessor<OrderCreated>
{
    public async Task<MessageResult> ProcessAsync(OrderCreated message, MessageContext context)
    {
        try
        {
            // Process the order
            await ProcessOrderLogic(message);
            return MessageResult.Acknowledge;
        }
        catch (TransientException)
        {
            return MessageResult.Retry;
        }
        catch (Exception)
        {
            return MessageResult.DeadLetter;
        }
    }

    private async Task ProcessOrderLogic(OrderCreated order)
    {
        // Implementation here
        await Task.Delay(100);
    }
}

public class TransientException : Exception
{
    public TransientException(string message) : base(message) { }
}