namespace ESE.MessageQueue.Retry;

public enum RetryStrategy
{
    Fixed,
    Linear,
    Exponential,
    Custom
}

// Semantics (ADR 0001): MaxRetries counts redeliveries after the first attempt, so a message is handled at most
// MaxRetries + 1 times. CalculateDelay(n) is the wait before retry n (1-based); every delay is clamped to [0, MaxDelay].
// Ranges are enforced by the provider config validators (DataAnnotations do not reach nested options).
public record RetryPolicy
{
    public const int MaxRetriesLimit = 1000;
    public const int DefaultMaxRetries = 5;
    public static readonly TimeSpan DefaultInitialDelay = TimeSpan.FromSeconds(2);
    public static readonly TimeSpan DefaultMaxDelay = TimeSpan.FromSeconds(30);
    public static readonly TimeSpan MaxAllowedDelay = TimeSpan.FromDays(1);

    public RetryStrategy Strategy { get; init; } = RetryStrategy.Exponential;

    public int MaxRetries { get; init; } = DefaultMaxRetries;

    public TimeSpan InitialDelay { get; init; } = DefaultInitialDelay;

    public TimeSpan MaxDelay { get; init; } = DefaultMaxDelay;

    public double BackoffMultiplier { get; init; } = 2.0;

    public bool UseJitter { get; init; } = true;

    public double JitterFactor { get; init; } = 0.1;

    public Func<int, TimeSpan>? CustomDelayCalculator { get; init; }

    public Func<Exception, bool>? IsRetryable { get; init; }

    public static RetryPolicy Default => new();

    public static RetryPolicy None => new() { MaxRetries = 0 };

    public static RetryPolicy Aggressive => new()
    {
        MaxRetries = 5,
        InitialDelay = TimeSpan.FromMilliseconds(100),
        MaxDelay = TimeSpan.FromSeconds(30),
        BackoffMultiplier = 2.0,
        UseJitter = true
    };
}
