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
    private readonly IMessageQueue<OrderCreated> _messageQueue;
    private readonly ILogger<OrderService> _logger;

    public OrderService(IMessageQueue<OrderCreated> messageQueue, ILogger<OrderService> logger)
    {
        _messageQueue = messageQueue;
        _logger = logger;
    }

    public async Task CreateOrderAsync(string customerId, decimal amount)
    {
        var orderId = Guid.NewGuid().ToString();
        var orderCreated = new OrderCreated(orderId, customerId, amount, DateTime.UtcNow);
        
        await _messageQueue.PublishAsync(orderCreated);
        
        _logger.LogInformation("Published OrderCreated event for order {OrderId}", orderId);
    }
}