using ZedLabs.MessageQueue.Retry;

namespace ZedLabs.MessageQueue.RabbitMQ;

// Broker-side backoff (ADR 0006). RabbitMQ only expires per-message TTLs at the head of a queue, so a single retry
// queue cannot hold different delays. Instead there is one retry queue per delay bucket: every message in it has the
// same TTL, and when the TTL expires the broker dead-letters the message back to the main queue.
static class RetryQueues
{
    // Unused classic retry queues are deleted by the broker after this long (at least an hour, at least 4 x the TTL).
    static readonly long MinimumExpiresMilliseconds = (long)TimeSpan.FromHours(1).TotalMilliseconds;

    // Buckets follow the policy's un-jittered schedule: for retry n, the step s(n) and, with jitter J,
    // s(n) x (1 - J) and s(n) x (1 + J), all capped at MaxDelay; the actual delay picks the nearest of the three.
    // That is at most three retry queues per step. Custom calculators have no known schedule, so their delays are
    // rounded to two significant digits instead.
    public static long BucketMilliseconds(RetryPolicy policy, int retryNumber, TimeSpan delay)
    {
        if (policy.Strategy == RetryStrategy.Custom || retryNumber < 1)
            return RoundToTwoSignificantDigits(delay);

        var step = new RetryExecutor(policy).ScheduledDelay(retryNumber).TotalMilliseconds;
        var jitter = policy.UseJitter ? Math.Clamp(policy.JitterFactor, 0, 1) : 0;
        var max = policy.MaxDelay.TotalMilliseconds;
        var actual = delay.TotalMilliseconds;

        return new[] { step * (1 - jitter), step, step * (1 + jitter) }
            .Select(candidate => (long)Math.Round(Math.Clamp(candidate, 0, max), MidpointRounding.AwayFromZero))
            .MinBy(candidate => Math.Abs(candidate - actual));
    }

    // At most 5% off, never more than 0.5 ms below 100 ms.
    public static long RoundToTwoSignificantDigits(TimeSpan delay)
    {
        var ms = delay.TotalMilliseconds;
        if (double.IsNaN(ms) || ms < 0.5)
            return 0;

        var magnitude = Math.Pow(10, Math.Max(0, Math.Floor(Math.Log10(ms)) - 1));
        return Math.Max(1, (long)(Math.Round(ms / magnitude, MidpointRounding.AwayFromZero) * magnitude));
    }

    public static string Prefix(string queueName) => $"{queueName}.retry.";

    public static string Name(string queueName, long bucketMilliseconds) => $"{Prefix(queueName)}{bucketMilliseconds}";

    public static long ExpiresMilliseconds(long bucketMilliseconds, TimeSpan? overrideExpires = null) =>
        overrideExpires is { } shortened ? (long)shortened.TotalMilliseconds : Math.Max(MinimumExpiresMilliseconds, 4 * bucketMilliseconds);

    // A cached declaration older than a quarter of x-expires is renewed before the next copy is published.
    public static bool IsStale(long declaredAtMilliseconds, long nowMilliseconds, long bucketMilliseconds, TimeSpan? overrideExpires = null) =>
        nowMilliseconds - declaredAtMilliseconds > ExpiresMilliseconds(bucketMilliseconds, overrideExpires) / 4;

    public static Dictionary<string, object?> Arguments(RabbitMqConfig config, long bucketMilliseconds)
    {
        var arguments = new Dictionary<string, object?>
        {
            ["x-message-ttl"] = bucketMilliseconds,
            ["x-dead-letter-exchange"] = string.Empty,
            ["x-dead-letter-routing-key"] = config.QueueName
        };

        // Only classic retry queues expire when unused. On RabbitMQ 4.3.6, declaring a quorum queue does not reset
        // its x-expires timer, so a busy quorum retry queue would be deleted holding messages (ADR 0006).
        if (!config.UseQuorumQueues)
            arguments["x-expires"] = ExpiresMilliseconds(bucketMilliseconds, config.RetryQueueExpiresOverride);

        if (config.UseQuorumQueues)
        {
            arguments["x-queue-type"] = "quorum";
            arguments["x-quorum-initial-group-size"] = config.QuorumInitialGroupSize;
            // Quorum queues dead-letter at-most-once by default; a retry must not be lost on its way back.
            // At-least-once dead-lettering requires reject-publish overflow.
            arguments["x-dead-letter-strategy"] = "at-least-once";
            arguments["x-overflow"] = "reject-publish";
        }

        return arguments;
    }
}
