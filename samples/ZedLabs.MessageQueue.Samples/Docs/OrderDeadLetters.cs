#region snippet:dead-letters
using Microsoft.Extensions.Logging;
using ZedLabs.MessageQueue.Abstractions;
#endregion

namespace ZedLabs.MessageQueue.Samples.Docs;

#region snippet:dead-letters
// Run from an operator tool, an admin endpoint or a one-off job, not from the message handler.
public sealed partial class OrderDeadLetters
{
    readonly IDeadLetterQueue<OrderPlaced> deadLetters;
    readonly ILogger<OrderDeadLetters> logger;

    public OrderDeadLetters(IDeadLetterQueue<OrderPlaced> deadLetters, ILogger<OrderDeadLetters> logger)
    {
        this.deadLetters = deadLetters;
        this.logger = logger;
    }

    // Look first: read-only snapshots with the reason code ("MaxRetriesExceeded", "Rejected", ...) and detail.
    public async Task<IReadOnlyList<DeadLetterMessage<OrderPlaced>>> PeekAsync(CancellationToken cancellationToken)
    {
        var letters = await deadLetters.PeekAsync(maxCount: 50, cancellationToken);
        foreach (var letter in letters)
            LogDeadLetter(logger, letter.MessageId, letter.ReasonCode, letter.ReasonDetail, letter.DeliveryAttempt);
        return letters;
    }

    // After fixing the cause: count with a dry run, then move them back as first deliveries with a full retry budget.
    public async Task<DeadLetterReplayResult> ReplayAsync(IReadOnlyCollection<string> reasonCodes, CancellationToken cancellationToken)
    {
        var preview = await deadLetters.ReplayAsync(
            new DeadLetterReplayOptions<OrderPlaced> { MaxCount = 500, ReasonCodes = reasonCodes, DryRun = true }, cancellationToken);
        LogPreview(logger, preview.Matched);

        var result = await deadLetters.ReplayAsync(
            new DeadLetterReplayOptions<OrderPlaced> { MaxCount = 500, ReasonCodes = reasonCodes }, cancellationToken);
        LogReplayed(logger, result.Replayed, result.Failed, result.Skipped);
        return result;
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Dead letter {MessageId}: {Reason} ({Detail}) on attempt {Attempt}")]
    static partial void LogDeadLetter(ILogger logger, string messageId, string? reason, string? detail, int? attempt);

    [LoggerMessage(Level = LogLevel.Information, Message = "A replay would move {Matched} dead letter(s) back to the queue")]
    static partial void LogPreview(ILogger logger, int matched);

    [LoggerMessage(Level = LogLevel.Information, Message = "Replayed {Replayed} dead letter(s): {Failed} failed, {Skipped} skipped")]
    static partial void LogReplayed(ILogger logger, int replayed, int failed, int skipped);
}
#endregion
