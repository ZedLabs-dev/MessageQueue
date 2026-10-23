using Microsoft.Extensions.Logging;

namespace ESE.MessageQueue.Providers;

// Successful publishes at Debug level (readiness §4): an operator enables it to trace one message or a burst. Off by
// default, because a line per message would drown everything else; failures throw to the caller and are counted in
// metrics instead. Never content or headers: the message id, the destination and the count only.
static partial class PublishLog
{
    // The id is the publisher's, and PublishOptions allows control characters in it: they are removed before it is
    // logged (DeliveryText, as on consume), so an id cannot forge log lines. The message keeps its id unchanged.
    public static void Published(ILogger logger, string messageId, string destination)
    {
        if (!logger.IsEnabled(LogLevel.Debug))
            return;
        var loggable = Processing.DeliveryText.MessageId(messageId);
        LogPublished(logger, loggable, destination);
    }

    [LoggerMessage(EventId = 1010, Level = LogLevel.Debug, Message = "Published message {MessageId} to {Destination}")]
    static partial void LogPublished(ILogger logger, string messageId, string destination);

    [LoggerMessage(EventId = 1011, Level = LogLevel.Debug, Message = "Published a batch of {MessageCount} messages to {Destination}")]
    public static partial void PublishedBatch(ILogger logger, int messageCount, string destination);
}
