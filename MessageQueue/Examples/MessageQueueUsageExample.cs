using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Extensions;
using ESE.MessageQueue.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace ESE.MessageQueue.Examples;

public class MessageQueueUsageExample
{
    public static void ConfigureServices(IServiceCollection services, IConfiguration configuration)
    {
        services.AddMessageQueue<OrderCreated>(configuration);
        
        services.AddMessageProcessor<OrderCreated, OrderCreatedProcessor>();
        services.AddHostedService<MessageQueueHostedService<OrderCreated>>();
    }
}

public class OrderService
{
    readonly IMessageQueue<OrderCreated> messageQueue;
    readonly ILogger<OrderService> logger;

    public OrderService(IMessageQueue<OrderCreated> messageQueue, ILogger<OrderService> logger)
    {
        this.messageQueue = messageQueue;
        this.logger = logger;
    }

    public async Task CreateOrderAsync(string customerId, decimal amount)
    {
        var orderId = Guid.NewGuid().ToString();
        var orderCreated = new OrderCreated(orderId, customerId, amount, DateTime.UtcNow);
        
        await messageQueue.PublishAsync(orderCreated);
        
        logger.LogInformation("Published OrderCreated event for order {OrderId}", orderId);
    }
}