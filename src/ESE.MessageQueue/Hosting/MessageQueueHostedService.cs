using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Processing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ESE.MessageQueue.Hosting;

/// <summary>
/// Subscribes to <see cref="IMessageSubscriber{T}"/> for the host's lifetime and hands each message to the scoped
/// <see cref="IMessageProcessor{T}"/>, in a new DI scope per message. The processor's <see cref="MessageResult"/> is
/// applied as is: acknowledge, retry with backoff, or dead-letter.
/// </summary>
public sealed class MessageQueueHostedService<T> : BackgroundService where T : class
{
    static readonly string MessageType = typeof(T).Name;

    readonly IMessageSubscriber<T> subscriber;
    readonly IServiceScopeFactory scopeFactory;
    readonly IServiceProviderIsService? serviceCatalog;
    readonly ILogger<MessageQueueHostedService<T>> logger;
    bool subscribed;

    /// <summary>
    /// Created by the host (<c>AddHostedService&lt;MessageQueueHostedService&lt;T&gt;&gt;()</c>).
    /// <paramref name="serviceCatalog"/> lets it detect a missing processor at start.
    /// </summary>
    public MessageQueueHostedService(
        IMessageSubscriber<T> subscriber,
        IServiceScopeFactory scopeFactory,
        ILogger<MessageQueueHostedService<T>> logger,
        IServiceProviderIsService? serviceCatalog = null)
    {
        this.subscriber = subscriber;
        this.scopeFactory = scopeFactory;
        this.logger = logger;
        this.serviceCatalog = serviceCatalog;
    }

    /// <summary>
    /// Subscribes during host start, so a broker or configuration failure stops the host instead of going unnoticed.
    /// Without a registered <see cref="IMessageProcessor{T}"/> it logs a warning and does not subscribe.
    /// </summary>
    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        if (serviceCatalog is not null && !serviceCatalog.IsService(typeof(IMessageProcessor<T>)))
        {
            HostedServiceLog.NoProcessor(logger, MessageType);
            return;
        }

        HostedServiceLog.Starting(logger, MessageType);
        await subscriber.SubscribeAsync(ProcessAsync, cancellationToken);
        subscribed = true;
        await base.StartAsync(cancellationToken);
    }

    /// <summary>Waits until the host stops; deliveries run on the subscriber's own loop.</summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }

    /// <summary>
    /// Unsubscribes, waiting for running handlers within the provider's <c>ShutdownTimeout</c> (or until
    /// <paramref name="cancellationToken"/> is cancelled).
    /// </summary>
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            await base.StopAsync(cancellationToken);
        }
        finally
        {
            if (subscribed)
            {
                HostedServiceLog.Stopping(logger, MessageType);
                subscribed = false;
                await subscriber.UnsubscribeAsync(cancellationToken);
            }
        }
    }

    async Task<MessageResult> ProcessAsync(T message, MessageContext context, CancellationToken cancellationToken)
    {
        var scope = scopeFactory.CreateAsyncScope();
        MessageResult result;
        try
        {
            var processor = scope.ServiceProvider.GetRequiredService<IMessageProcessor<T>>();
            result = await processor.ProcessAsync(message, context, cancellationToken);
        }
        catch
        {
            await DisposeScopeAsync(scope, context);
            throw;
        }

        // The processor's decision stands: a failure while tearing down the scope is logged, not turned into a retry.
        await DisposeScopeAsync(scope, context);
        return result;
    }

    async Task DisposeScopeAsync(AsyncServiceScope scope, MessageContext context)
    {
        try
        {
            await scope.DisposeAsync();
        }
        catch (Exception ex)
        {
            HostedServiceLog.ScopeDisposalFailed(logger, MessageType, context.MessageId, ex);
        }
    }
}

static partial class HostedServiceLog
{
    [LoggerMessage(EventId = 5001, Level = LogLevel.Information, Message = "Starting message queue hosted service for {MessageType}")]
    public static partial void Starting(ILogger logger, string messageType);

    [LoggerMessage(EventId = 5002, Level = LogLevel.Information, Message = "Stopping message queue hosted service for {MessageType}")]
    public static partial void Stopping(ILogger logger, string messageType);

    [LoggerMessage(EventId = 5003, Level = LogLevel.Warning,
        Message = "No IMessageProcessor<{MessageType}> is registered; the hosted service will not consume messages")]
    public static partial void NoProcessor(ILogger logger, string messageType);

    [LoggerMessage(EventId = 5004, Level = LogLevel.Warning,
        Message = "Disposing the DI scope for {MessageType} message {MessageId} failed; the processor's result is kept")]
    public static partial void ScopeDisposalFailed(ILogger logger, string messageType, string messageId, Exception exception);
}
