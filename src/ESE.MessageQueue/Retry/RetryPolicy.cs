namespace ESE.MessageQueue.Retry;

/// <summary>
/// How the shared pipeline retries a failed delivery (ADR 0001). <see cref="MaxRetries"/> counts redeliveries after
/// the first attempt, so a message is handled at most <c>MaxRetries + 1</c> times; then it is dead-lettered. The delay
/// before retry <c>n</c> (1-based) follows <see cref="Strategy"/> from <see cref="InitialDelay"/>, gets
/// ±<see cref="JitterFactor"/> jitter when <see cref="UseJitter"/> is set, and is clamped to [0, <see cref="MaxDelay"/>].
/// Bound from the <c>Retry</c> configuration section; ranges are checked at startup by the provider config validators.
/// Immutable and thread-safe; the delegates must be thread-safe too.
/// </summary>
public sealed record RetryPolicy
{
    /// <summary>Largest accepted <see cref="MaxRetries"/>.</summary>
    public const int MaxRetriesLimit = 1000;
    /// <summary>Default <see cref="MaxRetries"/>: with the default delays, about 60 seconds of retrying.</summary>
    public const int DefaultMaxRetries = 5;
    /// <summary>Default <see cref="InitialDelay"/>: 2 seconds.</summary>
    public static readonly TimeSpan DefaultInitialDelay = TimeSpan.FromSeconds(2);
    /// <summary>Default <see cref="MaxDelay"/>: 30 seconds.</summary>
    public static readonly TimeSpan DefaultMaxDelay = TimeSpan.FromSeconds(30);
    /// <summary>Largest accepted <see cref="MaxDelay"/>: one day.</summary>
    public static readonly TimeSpan MaxAllowedDelay = TimeSpan.FromDays(1);

    /// <summary>How the delay grows from one retry to the next. Default: <see cref="RetryStrategy.Exponential"/>.</summary>
    public RetryStrategy Strategy { get; init; } = RetryStrategy.Exponential;

    /// <summary>Redeliveries after the first attempt, 0 to <see cref="MaxRetriesLimit"/>; 0 dead-letters on the first failure.</summary>
    public int MaxRetries { get; init; } = DefaultMaxRetries;

    /// <summary>The delay before the first retry, and the step for <see cref="RetryStrategy.Fixed"/> and <see cref="RetryStrategy.Linear"/>.</summary>
    public TimeSpan InitialDelay { get; init; } = DefaultInitialDelay;

    /// <summary>The cap on every delay, at least <see cref="InitialDelay"/> and at most <see cref="MaxAllowedDelay"/>.</summary>
    public TimeSpan MaxDelay { get; init; } = DefaultMaxDelay;

    /// <summary>The growth factor for <see cref="RetryStrategy.Exponential"/>, 1 to 100.</summary>
    public double BackoffMultiplier { get; init; } = 2.0;

    /// <summary>Spreads delays randomly by <see cref="JitterFactor"/> so that retries of many messages do not align.</summary>
    public bool UseJitter { get; init; } = true;

    /// <summary>The jitter as a fraction of the delay, 0 to 1.</summary>
    public double JitterFactor { get; init; } = 0.1;

    /// <summary>The delay before retry <c>n</c> for <see cref="RetryStrategy.Custom"/> (required then). A throwing calculator falls back to <see cref="InitialDelay"/>.</summary>
    public Func<int, TimeSpan>? CustomDelayCalculator { get; init; }

    /// <summary>Decides whether a handler exception is worth retrying; false dead-letters at once as <c>NonRetryableError</c>. A throwing predicate counts as false. All exceptions are retryable when unset.</summary>
    public Func<Exception, bool>? IsRetryable { get; init; }

    /// <summary>The default policy: 5 exponential retries from 2 s to 30 s with jitter.</summary>
    public static RetryPolicy Default => new();

    /// <summary>No retries: the first failure dead-letters.</summary>
    public static RetryPolicy None => new() { MaxRetries = 0 };

    /// <summary>5 exponential retries from 100 ms to 30 s with jitter, for fast-recovering dependencies.</summary>
    public static RetryPolicy Aggressive => new()
    {
        MaxRetries = 5,
        InitialDelay = TimeSpan.FromMilliseconds(100),
        MaxDelay = TimeSpan.FromSeconds(30),
        BackoffMultiplier = 2.0,
        UseJitter = true
    };
}
