using AgentReview.MessageQueue.Abstractions;
using AgentReview.MessageQueue.Processing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AgentReview.MessageQueue.Hosting;

public class MessageQueueHostedService<T> : BackgroundService where T : class
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<MessageQueueHostedService<T>> _logger;

    public MessageQueueHostedService(
        IServiceProvider serviceProvider,
        ILogger<MessageQueueHostedService<T>> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Starting message queue hosted service for {MessageType}", typeof(T).Name);
        
        try
        {
            using var scope = _serviceProvider.CreateScope();
            var messageQueue = scope.ServiceProvider.GetRequiredService<IMessageQueue<T>>();
            var processor = scope.ServiceProvider.GetService<IMessageProcessor<T>>();
            
            if (processor != null)
            {
                await messageQueue.SubscribeAsync(async message =>
                {
                    var context = new MessageContext
                    {
                        MessageId = Guid.NewGuid().ToString(),
                        Timestamp = DateTime.UtcNow,
                        DeliveryCount = 1
                    };
                    
                    var result = await processor.ProcessAsync(message, context);
                    return result == MessageResult.Acknowledge;
                }, stoppingToken);
            }
            else
            {
                _logger.LogWarning("No message processor registered for {MessageType}", typeof(T).Name);
            }
            
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Message queue hosted service for {MessageType} was cancelled", typeof(T).Name);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in message queue hosted service for {MessageType}", typeof(T).Name);
        }
    }
}