using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ZedLabs.MessageQueue.Abstractions;

namespace ZedLabs.MessageQueue.Samples;

// Shows the dead-letter operations (ADR 0023) once, after Sample:DeadLetterDelay: Sample:DeadLetters = Peek lists the
// dead letters, Replay also counts them with a dry run and then moves them back to the queue. In a real application
// this belongs in an operator tool or admin endpoint, not in the worker.
public sealed partial class DeadLetterInspector : BackgroundService
{
    readonly IServiceProvider services;
    readonly SampleOptions options;
    readonly ILogger<DeadLetterInspector> logger;

    public DeadLetterInspector(IServiceProvider services, IOptions<SampleOptions> options, ILogger<DeadLetterInspector> logger)
    {
        this.services = services;
        this.options = options.Value;
        this.logger = logger;
    }

    public IReadOnlyList<DeadLetterMessage<OrderCreated>> Peeked { get; private set; } = [];

    public DeadLetterReplayResult? Previewed { get; private set; }

    public DeadLetterReplayResult? Replayed { get; private set; }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (options.DeadLetters == DeadLetterMode.None)
            return;

        await Task.Delay(options.DeadLetterDelay, stoppingToken);
        // Registered next to IMessageQueue<T> when the provider has dead-letter operations; Capabilities says which
        // ones work (RabbitMQ without a dead-letter queue has none, a Service Bus subscription cannot replay).
        var supported = services.GetService(typeof(IMessageQueue<OrderCreated>)) is IMessageQueue<OrderCreated> queue
            ? queue.Capabilities.DeadLetterOperations
            : DeadLetterOperations.None;
        var wanted = options.DeadLetters == DeadLetterMode.Replay ? DeadLetterOperations.Peek | DeadLetterOperations.Replay : DeadLetterOperations.Peek;
        if (services.GetService(typeof(IDeadLetterQueue<OrderCreated>)) is not IDeadLetterQueue<OrderCreated> deadLetters
            || (supported & wanted) != wanted)
        {
            LogUnsupported(logger, supported);
            return;
        }

        Peeked = await deadLetters.PeekAsync(10, stoppingToken);
        foreach (var letter in Peeked)
            LogDeadLetter(logger, letter.MessageId, letter.ReasonCode, letter.DeliveryAttempt, letter.DeadLetteredAt);

        if (options.DeadLetters != DeadLetterMode.Replay)
            return;

        Previewed = await deadLetters.ReplayAsync(new DeadLetterReplayOptions<OrderCreated> { MaxCount = 10, DryRun = true }, stoppingToken);
        LogDryRun(logger, Previewed.Matched);
        Replayed = await deadLetters.ReplayAsync(new DeadLetterReplayOptions<OrderCreated> { MaxCount = 10 }, stoppingToken);
        LogReplayed(logger, Replayed.Replayed, Replayed.Failed, Replayed.Skipped);
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "The configured queue does not support this dead-letter operation (it supports: {Supported})")]
    static partial void LogUnsupported(ILogger logger, DeadLetterOperations supported);

    [LoggerMessage(Level = LogLevel.Information, Message = "Dead letter {MessageId}: {Reason} on attempt {Attempt} at {DeadLetteredAt}")]
    static partial void LogDeadLetter(ILogger logger, string messageId, string? reason, int? attempt, DateTimeOffset? deadLetteredAt);

    [LoggerMessage(Level = LogLevel.Information, Message = "A replay would move {Matched} dead letter(s) back to the queue")]
    static partial void LogDryRun(ILogger logger, int matched);

    [LoggerMessage(Level = LogLevel.Information, Message = "Replayed {Replayed} dead letter(s) ({Failed} failed, {Skipped} skipped)")]
    static partial void LogReplayed(ILogger logger, int replayed, int failed, int skipped);
}
