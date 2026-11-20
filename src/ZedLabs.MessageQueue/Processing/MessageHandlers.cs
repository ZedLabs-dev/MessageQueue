using ZedLabs.MessageQueue.Abstractions;

namespace ZedLabs.MessageQueue.Processing;

static class MessageHandlers
{
    // The convenience overload: true acknowledges, false asks for a retry.
    public static Func<T, MessageContext, CancellationToken, Task<MessageResult>> FromPredicate<T>(Func<T, Task<bool>> handler) =>
        async (message, _, _) => await handler(message) ? MessageResult.Acknowledge : MessageResult.Retry;
}
