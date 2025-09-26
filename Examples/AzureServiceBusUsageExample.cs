using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Extensions;
using ESE.MessageQueue.Hosting;
using ESE.MessageQueue.Processing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace ESE.MessageQueue.Examples;

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

    async Task ProcessOrderLogic(OrderCreated order)
    {
        await Task.Delay(100);
    }
}

public class TransientException : Exception
{
    public TransientException(string message) : base(message) { }
}