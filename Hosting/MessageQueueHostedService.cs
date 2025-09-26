using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Processing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ESE.MessageQueue.Hosting;

public class MessageQueueHostedService<T> : BackgroundService where T : class
{
    readonly IServiceProvider serviceProvider;
    readonly ILogger<MessageQueueHostedService<T>> logger;

    public MessageQueueHostedService(
        IServiceProvider serviceProvider,
        ILogger<MessageQueueHostedService<T>> logger)
    {
        this.serviceProvider = serviceProvider;
        this.logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Starting message queue hosted service for {MessageType}", typeof(T).Name);
        
        try
        {
            using var scope = serviceProvider.CreateScope();
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
                logger.LogWarning("No message processor registered for {MessageType}", typeof(T).Name);
            }
            
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            logger.LogInformation("Message queue hosted service for {MessageType} was cancelled", typeof(T).Name);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error in message queue hosted service for {MessageType}", typeof(T).Name);
        }
    }
}