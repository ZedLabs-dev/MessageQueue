namespace ZedLabs.MessageQueue.Abstractions;

/// <summary>The dead-letter operations a queue supports (<see cref="MessageQueueCapabilities.DeadLetterOperations"/>).</summary>
[Flags]
public enum DeadLetterOperations
{
    /// <summary>No dead-letter operations.</summary>
    None = 0,

    /// <summary><see cref="IDeadLetterQueue{T}.PeekAsync"/>.</summary>
    Peek = 1,

    /// <summary><see cref="IDeadLetterQueue{T}.ReplayAsync"/>.</summary>
    Replay = 2,

    /// <summary><see cref="IDeadLetterQueue{T}.PurgeAsync"/>.</summary>
    Purge = 4,

    /// <summary>Peek, replay and purge.</summary>
    All = Peek | Replay | Purge
}
