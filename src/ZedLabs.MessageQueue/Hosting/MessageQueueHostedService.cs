using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ZedLabs.MessageQueue.Abstractions;
using ZedLabs.MessageQueue.Processing;
using ZedLabs.MessageQueue.Providers;

namespace ZedLabs.MessageQueue.Hosting;

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
        WarnIfTheHostStopsWaitingFirst();
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

    // On shutdown the host waits at most HostOptions.ShutdownTimeout for StopAsync; a queue whose drain may take as
    // long is cut short, and its handlers' late results are lost (ADR 0025).
    // A host timeout of zero or less (Timeout.InfiniteTimeSpan) waits as long as it takes.
    void WarnIfTheHostStopsWaitingFirst()
    {
        if (subscriber is not IShutdownBudget budget)
            return;
        using var scope = scopeFactory.CreateScope();
        if (HostShutdownTimeout(scope.ServiceProvider) is { } host && host > TimeSpan.Zero && budget.ShutdownTimeout >= host)
            HostedServiceLog.ShutdownTimeoutNotBelowHost(logger, MessageType, budget.ShutdownTimeout, host, Setting(scope.ServiceProvider));
    }

    // The queue's own configuration key when it was bound from a section (AddMessageQueue<T>), else a description.
    static string Setting(IServiceProvider services) =>
        services.GetService<QueueSection<T>>() is { } section ? $"{section.Path}:ShutdownTimeout" : "the queue's ShutdownTimeout";

    // HostOptions belongs to Microsoft.Extensions.Hosting, which this package does not reference (the abstractions are
    // enough everywhere else); the host registers it, so it is read by name. Null without a host or if it cannot be read.
    TimeSpan? HostShutdownTimeout(IServiceProvider services)
    {
        try
        {
            if (Type.GetType("Microsoft.Extensions.Hosting.HostOptions, Microsoft.Extensions.Hosting", throwOnError: false) is not { } hostOptions)
                return null;
            var options = services.GetService(typeof(IOptions<>).MakeGenericType(hostOptions));
            var value = options?.GetType().GetProperty(nameof(IOptions<object>.Value))?.GetValue(options);
            return value?.GetType().GetProperty("ShutdownTimeout")?.GetValue(value) as TimeSpan?;
        }
        catch (Exception ex)
        {
            // Advice only: never fail a start over it.
            HostedServiceLog.HostOptionsUnreadable(logger, MessageType, ex);
            return null;
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

    [LoggerMessage(EventId = 5005, Level = LogLevel.Warning,
        Message = "The {MessageType} queue may wait up to its ShutdownTimeout ({ShutdownTimeout}) for running handlers, not less than the host's HostOptions.ShutdownTimeout ({HostShutdownTimeout}): the host can stop waiting first and lose their results. Lower {ShutdownTimeoutSetting} or raise HostOptions.ShutdownTimeout")]
    public static partial void ShutdownTimeoutNotBelowHost(ILogger logger, string messageType, TimeSpan shutdownTimeout, TimeSpan hostShutdownTimeout,
        string shutdownTimeoutSetting);

    [LoggerMessage(EventId = 5006, Level = LogLevel.Debug,
        Message = "Could not read HostOptions.ShutdownTimeout for the {MessageType} queue; the shutdown budget is not checked")]
    public static partial void HostOptionsUnreadable(ILogger logger, string messageType, Exception exception);
}
