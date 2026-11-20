namespace ZedLabs.MessageQueue.Abstractions;

/// <summary>Why one dead letter could not be replayed.</summary>
/// <param name="MessageId">The dead letter's message id.</param>
/// <param name="Error">The exception type name (bounded); never exception text. The exception is logged.</param>
public sealed record DeadLetterReplayFailure(string MessageId, string Error);
