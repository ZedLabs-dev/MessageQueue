using AgentReview.MessageQueue.Abstractions;
using AgentReview.MessageQueue.Configuration;
using AgentReview.MessageQueue.Extensions;
using AgentReview.MessageQueue.Hosting;

namespace AgentReview.Examples;

public class MessageQueueUsageExample
{
    public static void ConfigureServices(IServiceCollection services, IConfiguration configuration)
    {
        // Automatically reads from configuration and instantiates correct provider
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