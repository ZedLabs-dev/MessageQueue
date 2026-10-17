using Microsoft.Extensions.Logging;

namespace ESE.MessageQueue.Processing;

static partial class Cancellation
{
    // Handlers can register callbacks on the token they are given. Cancelling runs them, and one that throws makes
    // CancelAsync throw, although the token is cancelled regardless. Shutdown must carry on: log and continue.
    public static async Task CancelQuietlyAsync(CancellationTokenSource source, ILogger logger, string queueName)
    {
        try
        {
            await source.CancelAsync();
        }
        catch (Exception ex)
        {
            CallbackFailed(new SafeLogger(logger), queueName, ex);
        }
    }

    [LoggerMessage(EventId = 1008, Level = LogLevel.Warning,
        Message = "Queue {QueueName}: a cancellation callback registered by a handler threw during shutdown; the shutdown continues")]
    static partial void CallbackFailed(ILogger logger, string queueName, Exception exception);
}
