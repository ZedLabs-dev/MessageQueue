using System.Text;
using AwesomeAssertions;
using Microsoft.Extensions.Logging;
using ZedLabs.MessageQueue.Abstractions;
using ZedLabs.MessageQueue.InMemory;
using ZedLabs.MessageQueue.Processing;
using ZedLabs.MessageQueue.Providers;
using ZedLabs.MessageQueue.Telemetry;
using ZedLabs.MessageQueue.Tests.Telemetry;

namespace ZedLabs.MessageQueue.Tests.Providers;

// ADR 0023: the shared dead-letter operations over a provider's store. Selection, limits, dry runs, snapshots and
// counting are decided here once, for every provider; the store only reads, replays, removes and returns.
[Trait("Category", "Unit")]
public sealed class Given_a_dead_letter_queue
{
    static readonly TelemetryTarget Target = new("test", "orders", nameof(TestMessage));

    readonly FakeDeadLetterStore store = new();
    readonly TestTelemetryProvider telemetry = new();
    readonly RecordingLogger logger = new();

    DeadLetterQueue<TestMessage> Create(InMemoryConfig? config = null) =>
        new(store, config ?? new InMemoryConfig { QueueName = "orders" }, Target, telemetry, logger);

    [Fact]
    public async Task When_Peeking()
    {
        var enqueued = DateTimeOffset.UtcNow.AddMinutes(-5);
        var dead = DateTimeOffset.UtcNow.AddMinutes(-1);
        store.Add(new DeadLetterMetadata
        {
            MessageId = "m-1",
            CorrelationId = "c-1",
            ReasonCode = DeadLetterReasons.MaxRetriesExceeded,
            ReasonDetail = "System.TimeoutException",
            EnqueuedAt = enqueued,
            DeadLetteredAt = dead,
            DeliveryAttempt = 6,
            Headers = new Dictionary<string, string> { ["tenant"] = "acme" }
        }, Encoding.UTF8.GetBytes("""{"id":"m-1","content":"hello"}"""));
        store.Add("m-2", DeadLetterReasons.Rejected);

        var letters = await Create().PeekAsync(10, TestContext.Current.CancellationToken);

        letters.Should().HaveCount(2, "it should return every dead letter up to the count");
        var first = letters[0];
        first.MessageId.Should().Be("m-1", "it should report the message id");
        first.CorrelationId.Should().Be("c-1", "it should report the correlation id");
        first.ReasonCode.Should().Be(DeadLetterReasons.MaxRetriesExceeded, "it should report why it was dead-lettered");
        first.ReasonDetail.Should().Be("System.TimeoutException", "it should report the detail");
        first.EnqueuedAt.Should().Be(enqueued, "it should report when it was published");
        first.DeadLetteredAt.Should().Be(dead, "it should report when it was dead-lettered");
        first.DeliveryAttempt.Should().Be(6, "it should report the attempt it died on");
        first.Headers.Should().Contain("tenant", "acme", "it should report the application headers");
        first.Message.Should().Be(new TestMessage("m-1", "hello"), "it should deserialize the message");
        first.BodyLength.Should().Be(30, "it should report the body length");
        store.Letters.Should().HaveCount(2, "it should not remove anything");
        store.Scans.Should().Equal([true], "it should peek through a browse-only pass");
        store.Returned.Should().Be(2, "it should return every dead letter it held");
    }

    [Fact]
    public async Task When_Peeking_Fewer_Than_The_Store_Holds()
    {
        for (var i = 0; i < 5; i++)
            store.Add($"m-{i}");

        var letters = await Create().PeekAsync(3, TestContext.Current.CancellationToken);

        letters.Select(l => l.MessageId).Should().Equal(["m-0", "m-1", "m-2"], "it should return the first dead letters in the store's order");
        store.Examined.Should().Be(3, "it should not read past the count");
    }

    [Fact]
    public async Task When_Peeking_A_Body_Over_The_Size_Limit()
    {
        store.Add("big", DeadLetterReasons.MessageTooLarge, new TestMessage("big", new string('x', 100)));

        var letter = (await Create(new InMemoryConfig { QueueName = "orders", MaxMessageSizeBytes = 64 }).PeekAsync(1, TestContext.Current.CancellationToken)).Single();

        letter.Message.Should().BeNull("it should not deserialize a body larger than MaxMessageSizeBytes");
        letter.BodyLength.Should().BeGreaterThan(64, "it should report the body length instead");
    }

    [Fact]
    public async Task When_Peeking_A_Body_Exactly_At_The_Size_Limit()
    {
        var body = Encoding.UTF8.GetBytes("""{"id":"edge","content":""}""");
        store.Add(new DeadLetterMetadata { MessageId = "edge" }, body);

        var letter = (await Create(new InMemoryConfig { QueueName = "orders", MaxMessageSizeBytes = body.Length }).PeekAsync(1, TestContext.Current.CancellationToken)).Single();

        letter.Message.Should().Be(new TestMessage("edge", ""), "it should deserialize a body exactly at the limit, as the dispatcher does");
    }

    [Fact]
    public async Task When_Peeking_A_Body_That_Does_Not_Deserialize()
    {
        store.Add("bad", DeadLetterReasons.DeserializationFailed, body: Encoding.UTF8.GetBytes("not json"));

        var letter = (await Create().PeekAsync(1, TestContext.Current.CancellationToken)).Single();

        letter.Message.Should().BeNull("it should report an undeserializable body without the message");
        letter.BodyLength.Should().Be(8, "it should report the body length");
    }

    [Fact]
    public async Task When_Peeking_Hostile_Metadata()
    {
        var headers = Enumerable.Range(0, 100).ToDictionary(i => $"h{i}", i => "v");
        headers["huge"] = new string('v', PublishOptions.MaxHeaderValueLength + 1);
        store.Add(new DeadLetterMetadata
        {
            MessageId = "id\r\nforged log line" + new string('a', 200),
            CorrelationId = "corr\u0000",
            ReasonCode = new string('r', 10_000),
            ReasonDetail = new string('d', 10_000),
            DeliveryAttempt = 0,
            Headers = headers
        }, Encoding.UTF8.GetBytes("{}"));

        var letter = (await Create().PeekAsync(1, TestContext.Current.CancellationToken)).Single();

        letter.MessageId.Should().NotContainAny(["\r", "\n"], "it should strip control characters from the id")
            .And.HaveLength(PublishOptions.MaxMessageIdLength, "it should cut the id to the publish-side limit");
        letter.CorrelationId.Should().Be("corr", "it should sanitize the correlation id");
        letter.ReasonCode.Should().HaveLength(PublishOptions.MaxHeaderValueLength, "it should bound the reason code");
        letter.ReasonDetail.Should().HaveLength(PublishOptions.MaxHeaderValueLength, "it should bound the reason detail");
        letter.DeliveryAttempt.Should().BeNull("it should not report an attempt below 1");
        letter.Headers.Should().HaveCount(PublishOptions.MaxHeaders, "it should cap the number of headers")
            .And.NotContainKey("huge", "it should drop a header value over the limit");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(DeadLetterLimits.MaxPeekCount + 1)]
    public async Task When_Peeking_An_Out_Of_Range_Count(int maxCount)
    {
        var peek = () => Create().PeekAsync(maxCount, TestContext.Current.CancellationToken);

        await peek.Should().ThrowAsync<ArgumentOutOfRangeException>("it should refuse a count outside 1 to MaxPeekCount");
        store.Scans.Should().BeEmpty("it should not open a pass for an invalid call");
    }

    [Fact]
    public async Task When_Peeking_The_Largest_Count()
    {
        store.Add("m-1");

        var letters = await Create().PeekAsync(DeadLetterLimits.MaxPeekCount, TestContext.Current.CancellationToken);

        letters.Should().ContainSingle("it should accept MaxPeekCount");
    }

    [Fact]
    public async Task When_Replaying_With_Default_Options()
    {
        store.Add("retries", DeadLetterReasons.MaxRetriesExceeded);
        store.Add("too-large", DeadLetterReasons.MessageTooLarge);
        store.Add("rejected", DeadLetterReasons.Rejected);
        store.Add("undeserializable", DeadLetterReasons.DeserializationFailed);
        store.Add("broker", reason: null);

        var result = await Create().ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);

        store.Replayed.Select(l => l.Metadata.MessageId).Should().Equal(["retries", "rejected", "broker"],
            "it should replay every reason except the two that would only fail again");
        store.Letters.Select(l => l.Metadata.MessageId).Should().Equal(["too-large", "undeserializable"],
            "it should leave MessageTooLarge and DeserializationFailed in place unless asked for");
        result.Should().BeEquivalentTo(new DeadLetterReplayResult { Matched = 3, Replayed = 3, Skipped = 2, Failed = 0 },
            "it should count what it replayed and skipped");
        store.Scans.Should().Equal([false], "it should replay through a settling pass");
        store.Returned.Should().Be(2, "it should return the skipped dead letters when the pass ends");
    }

    [Fact]
    public async Task When_Replaying_With_Reason_Codes()
    {
        store.Add("retries", DeadLetterReasons.MaxRetriesExceeded);
        store.Add("undeserializable", DeadLetterReasons.DeserializationFailed);
        store.Add("broker", reason: null);

        var result = await Create().ReplayAsync(new DeadLetterReplayOptions<TestMessage>
        {
            MaxCount = 10,
            ReasonCodes = [DeadLetterReasons.DeserializationFailed]
        }, TestContext.Current.CancellationToken);

        store.Replayed.Select(l => l.Metadata.MessageId).Should().Equal(["undeserializable"],
            "it should replay only the named reasons, including one excluded by default when named explicitly");
        result.Skipped.Should().Be(2, "it should skip the other reasons, and a dead letter without a reason");
    }

    [Fact]
    public async Task When_Replaying_With_A_Filter()
    {
        store.Add("a", DeadLetterReasons.Rejected, headers: new Dictionary<string, string> { ["tenant"] = "acme" });
        store.Add("b", DeadLetterReasons.Rejected, headers: new Dictionary<string, string> { ["tenant"] = "other" });
        store.Add("c", DeadLetterReasons.Rejected, new TestMessage("c", "replay me"));

        var result = await Create().ReplayAsync(new DeadLetterReplayOptions<TestMessage>
        {
            MaxCount = 10,
            Filter = m => m.Headers.GetValueOrDefault("tenant") == "acme" || m.Message?.Content == "replay me"
        }, TestContext.Current.CancellationToken);

        store.Replayed.Select(l => l.Metadata.MessageId).Should().Equal(["a", "c"], "it should replay only what the filter accepts");
        result.Skipped.Should().Be(1, "it should count the dead letter the filter refused");
    }

    [Fact]
    public async Task When_Replaying_With_Reason_Codes_And_A_Filter()
    {
        store.Add("a", DeadLetterReasons.Rejected);
        store.Add("b", DeadLetterReasons.MaxRetriesExceeded);

        await Create().ReplayAsync(new DeadLetterReplayOptions<TestMessage>
        {
            MaxCount = 10,
            ReasonCodes = [DeadLetterReasons.Rejected, DeadLetterReasons.MaxRetriesExceeded],
            Filter = m => m.MessageId == "b"
        }, TestContext.Current.CancellationToken);

        store.Replayed.Select(l => l.Metadata.MessageId).Should().Equal(["b"], "it should require both the reason code and the filter");
    }

    [Fact]
    public async Task When_Replaying_With_A_Max_Count()
    {
        for (var i = 0; i < 5; i++)
            store.Add($"m-{i}", DeadLetterReasons.Rejected);

        var result = await Create().ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 2 }, TestContext.Current.CancellationToken);

        store.Replayed.Select(l => l.Metadata.MessageId).Should().Equal(["m-0", "m-1"], "it should replay exactly MaxCount");
        result.Replayed.Should().Be(2, "it should count them");
        store.Examined.Should().Be(2, "it should stop reading once MaxCount is reached");
    }

    [Fact]
    public async Task When_Replaying_As_A_Dry_Run()
    {
        store.Add("a", DeadLetterReasons.Rejected);
        store.Add("b", DeadLetterReasons.MessageTooLarge);
        store.Add("c", DeadLetterReasons.MaxRetriesExceeded);

        var result = await Create().ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10, DryRun = true }, TestContext.Current.CancellationToken);

        result.Should().BeEquivalentTo(new DeadLetterReplayResult { DryRun = true, Matched = 2, Replayed = 0, Skipped = 1 },
            "it should count what it would replay without replaying");
        store.Replayed.Should().BeEmpty("it should not replay anything in a dry run");
        store.Letters.Should().HaveCount(3, "it should leave every dead letter in place");
        store.Scans.Should().Equal([true], "it should read through a browse-only pass");
        telemetry.DeadLettersReplayed.Should().BeEmpty("it should not count a dry run as replayed");
    }

    [Fact]
    public async Task When_A_Replay_Fails()
    {
        store.Add("ok-1", DeadLetterReasons.Rejected);
        store.Add("fails", DeadLetterReasons.Rejected);
        store.Add("ok-2", DeadLetterReasons.Rejected);
        store.ReplayFailure = letter => letter.Metadata.MessageId == "fails" ? new InvalidOperationException("broker said 4111-1111 no") : null;

        var result = await Create().ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);

        result.Replayed.Should().Be(2, "it should go on after one failure");
        result.Failed.Should().Be(1, "it should count the failure");
        result.Matched.Should().Be(3, "it should count the failed one as selected");
        result.Failures.Should().Equal([new DeadLetterReplayFailure("fails", typeof(InvalidOperationException).FullName!)],
            "it should report the message id and the exception type, never the exception text");
        store.Letters.Select(l => l.Metadata.MessageId).Should().Equal(["fails"], "it should leave the failed one in the dead letters");
        store.Returned.Should().Be(1, "it should return the failed one when the pass ends");
        logger.Entries.Should().Contain(e => e.Level == LogLevel.Warning && e.Exception is InvalidOperationException,
            "it should log the failure with its exception");
    }

    [Fact]
    public async Task When_Many_Replays_Fail()
    {
        for (var i = 0; i < DeadLetterLimits.MaxReportedFailures + 5; i++)
            store.Add($"m-{i}", DeadLetterReasons.Rejected);
        store.ReplayFailure = _ => new TimeoutException();

        var result = await Create().ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 1000 }, TestContext.Current.CancellationToken);

        result.Failed.Should().Be(DeadLetterLimits.MaxReportedFailures + 5, "it should count every failure");
        result.Failures.Should().HaveCount(DeadLetterLimits.MaxReportedFailures, "it should list at most MaxReportedFailures");
    }

    [Fact]
    public async Task When_A_Message_Id_Comes_Around_Again()
    {
        store.Add("same", DeadLetterReasons.Rejected);
        store.Add("other", DeadLetterReasons.Rejected);
        store.Add("same", DeadLetterReasons.Rejected);

        var result = await Create().ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);

        store.Replayed.Select(l => l.Metadata.MessageId).Should().Equal(["same", "other"],
            "it should replay a message id once per call, so a replayed message dead-lettered again does not loop");
        result.Skipped.Should().Be(1, "it should count the repeat as skipped");
    }

    [Fact]
    public async Task When_Messages_Without_An_Id_Come_Around_Again()
    {
        store.Add("", DeadLetterReasons.Rejected, new TestMessage("x", "first"));
        store.Add("", DeadLetterReasons.Rejected, new TestMessage("x", "second"));
        store.Add("", DeadLetterReasons.Rejected, new TestMessage("x", "first"));
        store.Add("", DeadLetterReasons.Rejected, new TestMessage("x", "first"), correlationId: "other-workflow");

        var result = await Create().ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);

        store.Replayed.Select(l => (Encoding.UTF8.GetString(l.Body), l.Metadata.CorrelationId)).Should().Equal(
            [("""{"id":"x","content":"first"}""", null), ("""{"id":"x","content":"second"}""", null), ("""{"id":"x","content":"first"}""", "other-workflow")],
            "it should key a message without an id by its body, correlation id and headers, so it is replayed once per call too");
        result.Skipped.Should().Be(1, "it should skip the identical repeat");
    }

    [Fact]
    public async Task When_The_Store_Holds_More_Than_One_Call_Examines()
    {
        for (var i = 0; i < DeadLetterLimits.MaxScanCount + 5; i++)
            store.Add($"m-{i}", DeadLetterReasons.MessageTooLarge);

        var result = await Create().ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);

        store.Examined.Should().Be(DeadLetterLimits.MaxScanCount, "it should examine at most MaxScanCount dead letters per call");
        result.Skipped.Should().Be(DeadLetterLimits.MaxScanCount, "it should count them as skipped");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_A_Replay_Is_Cancelled()
    {
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        store.Add("first", DeadLetterReasons.Rejected);
        store.Add("second", DeadLetterReasons.Rejected);
        store.Add("third", DeadLetterReasons.Rejected);
        store.BeforeReplay = async (letter, _) =>
        {
            if (letter.Metadata.MessageId == "first")
                await cancellation.CancelAsync();
        };

        var replay = () => Create().ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, cancellation.Token);

        await replay.Should().ThrowAsync<OperationCanceledException>("it should honor the cancellation");
        store.Replayed.Select(l => l.Metadata.MessageId).Should().Equal(["first"], "it should finish the message in hand and start no other");
        store.Letters.Select(l => l.Metadata.MessageId).Should().Equal(["second", "third"], "it should leave the rest as dead letters, losing none");
        store.Log.Should().EndWith("dispose", "it should end the pass, returning what it held");
        telemetry.DeadLettersReplayed.Should().Equal([(Target, 1)], "it should count the replay it did before the cancellation");
        logger.Entries.Should().Contain(e => e.Message.Contains("cancelled", StringComparison.Ordinal), "it should log how far it got");
    }

    [Fact]
    public async Task When_The_Replay_Options_Are_Invalid()
    {
        var queue = Create();

        await ((Func<Task>)(() => queue.ReplayAsync(null!, TestContext.Current.CancellationToken))).Should().ThrowAsync<ArgumentNullException>("it should refuse null options");
        await ((Func<Task>)(() => queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 0 }, TestContext.Current.CancellationToken)))
            .Should().ThrowAsync<ArgumentOutOfRangeException>("it should refuse a MaxCount below 1");
        await ((Func<Task>)(() => queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 1, ReasonCodes = [] }, TestContext.Current.CancellationToken)))
            .Should().ThrowAsync<ArgumentException>("it should refuse an empty reason-code list, which would select nothing");
        await ((Func<Task>)(() => queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 1, ReasonCodes = [" "] }, TestContext.Current.CancellationToken)))
            .Should().ThrowAsync<ArgumentException>("it should refuse a blank reason code");
        store.Scans.Should().BeEmpty("it should not open a pass for an invalid call");
    }

    [Fact]
    public async Task When_Purging()
    {
        store.Add("a", DeadLetterReasons.DeserializationFailed);
        store.Add("b", DeadLetterReasons.MessageTooLarge);
        store.Add("c", DeadLetterReasons.Rejected);
        store.Add("d", reason: null);

        var purged = await Create().PurgeAsync(new DeadLetterPurgeOptions<TestMessage> { MaxCount = 3 }, TestContext.Current.CancellationToken);

        purged.Should().Be(3, "it should return how many it deleted");
        store.Removed.Select(l => l.Metadata.MessageId).Should().Equal(["a", "b", "c"], "it should delete every reason, up to MaxCount");
        store.Letters.Select(l => l.Metadata.MessageId).Should().Equal(["d"], "it should leave the rest");
        store.Examined.Should().Be(3, "it should stop reading once MaxCount is reached");
        store.Replayed.Should().BeEmpty("it should never replay while purging");
        telemetry.DeadLettersPurged.Should().Equal([(Target, 3)], "it should count the deleted dead letters");
    }

    [Fact]
    public async Task When_Purging_With_Reason_Codes_And_A_Filter()
    {
        store.Add("a", DeadLetterReasons.DeserializationFailed);
        store.Add("b", DeadLetterReasons.Rejected);
        store.Add("c", DeadLetterReasons.DeserializationFailed, headers: new Dictionary<string, string> { ["keep"] = "yes" });

        var purged = await Create().PurgeAsync(new DeadLetterPurgeOptions<TestMessage>
        {
            MaxCount = 10,
            ReasonCodes = [DeadLetterReasons.DeserializationFailed],
            Filter = m => !m.Headers.ContainsKey("keep")
        }, TestContext.Current.CancellationToken);

        purged.Should().Be(1, "it should delete only what the reason codes and the filter select");
        store.Letters.Select(l => l.Metadata.MessageId).Should().Equal(["b", "c"], "it should leave everything else");
        store.Returned.Should().Be(2, "it should return the skipped dead letters");
    }

    [Fact]
    public async Task When_The_Purge_Options_Are_Invalid()
    {
        var queue = Create();

        await ((Func<Task>)(() => queue.PurgeAsync(null!, TestContext.Current.CancellationToken))).Should().ThrowAsync<ArgumentNullException>("it should refuse null options");
        await ((Func<Task>)(() => queue.PurgeAsync(new DeadLetterPurgeOptions<TestMessage> { MaxCount = -1 }, TestContext.Current.CancellationToken)))
            .Should().ThrowAsync<ArgumentOutOfRangeException>("it should refuse a MaxCount below 1");
        await ((Func<Task>)(() => queue.PurgeAsync(new DeadLetterPurgeOptions<TestMessage> { MaxCount = 0 }, TestContext.Current.CancellationToken)))
            .Should().ThrowAsync<ArgumentOutOfRangeException>("it should refuse a MaxCount of 0");
        await ((Func<Task>)(() => queue.PurgeAsync(new DeadLetterPurgeOptions<TestMessage> { MaxCount = 1, ReasonCodes = [] }, TestContext.Current.CancellationToken)))
            .Should().ThrowAsync<ArgumentException>("it should refuse an empty reason-code list");
        store.Scans.Should().BeEmpty("it should not open a pass for an invalid call");
    }

    [Fact]
    public async Task When_An_Operation_Is_Not_Supported()
    {
        store.Operations = DeadLetterOperations.Peek | DeadLetterOperations.Purge;
        store.Add("a", DeadLetterReasons.Rejected);
        var queue = Create();

        var replay = () => queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 1 }, TestContext.Current.CancellationToken);
        await replay.Should().ThrowAsync<NotSupportedException>("it should refuse an operation the store does not support")
            .WithMessage("*Replay*", "it should name the operation");
        store.Operations = DeadLetterOperations.None;
        await ((Func<Task>)(() => queue.PeekAsync(1, TestContext.Current.CancellationToken))).Should().ThrowAsync<NotSupportedException>("it should refuse a peek too");
        await ((Func<Task>)(() => queue.PurgeAsync(new DeadLetterPurgeOptions<TestMessage> { MaxCount = 1 }, TestContext.Current.CancellationToken)))
            .Should().ThrowAsync<NotSupportedException>("it should refuse a purge too");
        store.Scans.Should().BeEmpty("it should not open a pass");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Operations_Overlap()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        store.Add("a", DeadLetterReasons.Rejected);
        store.BeforeReplay = async (_, _) =>
        {
            entered.TrySetResult();
            await release.Task;
        };
        var queue = Create();

        var replay = queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 1 }, TestContext.Current.CancellationToken);
        await entered.Task.WaitAsync(TestContext.Current.CancellationToken);
        var peek = queue.PeekAsync(1, TestContext.Current.CancellationToken);
        await Task.Delay(100, TestContext.Current.CancellationToken);
        var scansWhileReplaying = store.Scans.Count;
        release.SetResult();
        await replay;
        var letters = await peek;

        scansWhileReplaying.Should().Be(1, "it should run one operation at a time per queue");
        letters.Should().BeEmpty("it should peek after the replay finished");
    }

    [Fact]
    public async Task When_Returning_Held_Dead_Letters_Fails()
    {
        store.Add("a", DeadLetterReasons.Rejected);
        store.DisposeFailure = new InvalidOperationException("channel closed");

        var letters = await Create().PeekAsync(1, TestContext.Current.CancellationToken);

        letters.Should().ContainSingle("it should still return what it read");
        logger.Entries.Should().Contain(e => e.Exception is InvalidOperationException, "it should log the failure to return them");
    }

    [Fact]
    public async Task When_Replaying_With_Telemetry_And_Logging()
    {
        store.Add("a", DeadLetterReasons.Rejected);
        store.Add("b", DeadLetterReasons.Rejected);

        await Create().ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);

        telemetry.DeadLettersReplayed.Should().Equal([(Target, 2)], "it should count the replayed dead letters once per call");
        logger.Entries.Should().Contain(e => e.Level == LogLevel.Information && e.Message.Contains("2 replayed", StringComparison.Ordinal),
            "it should log a summary");
    }

    [Fact]
    public async Task When_Reason_Codes_Differ_Only_In_Case()
    {
        store.Add("a", DeadLetterReasons.Rejected);

        var result = await Create().ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10, ReasonCodes = ["rejected"] },
            TestContext.Current.CancellationToken);

        result.Replayed.Should().Be(0, "it should match reason codes exactly, case included");
    }

    [Fact]
    public async Task When_The_Config_Changes_After_Creation()
    {
        var config = new InMemoryConfig { QueueName = "orders" };
        store.Add("a", DeadLetterReasons.Rejected);
        var queue = new DeadLetterQueue<TestMessage>(store, config, Target);
        config.MaxMessageSizeBytes = 8;

        var letter = (await queue.PeekAsync(1, TestContext.Current.CancellationToken)).Single();

        letter.Message.Should().NotBeNull("it should use the config as it was when the queue was created");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_A_Purge_Is_Cancelled()
    {
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        store.Add("first", DeadLetterReasons.Rejected);
        store.Add("second", DeadLetterReasons.Rejected);
        store.Add("third", DeadLetterReasons.Rejected);

        var purge = () => Create().PurgeAsync(new DeadLetterPurgeOptions<TestMessage>
        {
            MaxCount = 10,
            Filter = _ =>
            {
                cancellation.Cancel();
                return true;
            }
        }, cancellation.Token);

        await purge.Should().ThrowAsync<OperationCanceledException>("it should honor the cancellation between messages");
        store.Removed.Select(l => l.Metadata.MessageId).Should().Equal(["first"], "it should finish the message in hand and start no other");
        telemetry.DeadLettersPurged.Should().Equal([(Target, 1)], "it should count what it deleted before the cancellation");
    }

    [Fact]
    public async Task When_Purging_A_Store_Larger_Than_One_Call_Examines()
    {
        for (var i = 0; i < DeadLetterLimits.MaxScanCount + 5; i++)
            store.Add($"m-{i}", DeadLetterReasons.Rejected);

        var purged = await Create().PurgeAsync(new DeadLetterPurgeOptions<TestMessage> { MaxCount = 10, Filter = _ => false },
            TestContext.Current.CancellationToken);

        purged.Should().Be(0, "it should delete nothing the filter refuses");
        store.Examined.Should().Be(DeadLetterLimits.MaxScanCount, "it should examine at most MaxScanCount dead letters per purge");
    }

    [Fact]
    public async Task When_The_Store_Times_Out_On_One_Message()
    {
        store.Add("slow", DeadLetterReasons.Rejected);
        store.Add("fine", DeadLetterReasons.Rejected);
        store.ReplayFailure = letter => letter.Metadata.MessageId == "slow" ? new TaskCanceledException("send timed out") : null;

        var result = await Create().ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);

        result.Failed.Should().Be(1, "it should count a store's own timeout as a failed replay, not a cancellation of the call");
        result.Replayed.Should().Be(1, "it should go on with the next message");
    }

    [Fact]
    public async Task When_Telemetry_Throws()
    {
        store.Add("a", DeadLetterReasons.Rejected);
        store.Add("b", DeadLetterReasons.Rejected);
        var queue = new DeadLetterQueue<TestMessage>(store, new InMemoryConfig { QueueName = "orders" }, Target, new ThrowingTelemetryProvider(), logger);

        var result = await queue.ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 1 }, TestContext.Current.CancellationToken);
        var purged = await queue.PurgeAsync(new DeadLetterPurgeOptions<TestMessage> { MaxCount = 1 }, TestContext.Current.CancellationToken);

        result.Replayed.Should().Be(1, "it should replay even when telemetry fails");
        purged.Should().Be(1, "it should purge even when telemetry fails");
    }

    [Fact]
    public async Task When_The_Pass_Fails_Midway()
    {
        for (var i = 0; i < 5; i++)
            store.Add($"m-{i}", DeadLetterReasons.Rejected);
        store.NextFailure = n => n == 3 ? new InvalidOperationException("connection to amqp://user:s3cret@host lost") : null;

        var replay = () => Create().ReplayAsync(new DeadLetterReplayOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);

        var thrown = (await replay.Should().ThrowAsync<ZedLabs.MessageQueue.Exceptions.MessageQueueException>(
            "it should report a failed pass as a MessageQueueException")).Which;
        thrown.ToString().Should().NotContain("s3cret", "it should name only the exception type, never client text that could carry secrets");
        thrown.Message.Should().Contain(nameof(InvalidOperationException), "it should name the type of the failure");
        telemetry.DeadLettersReplayed.Should().Equal([(Target, 3)], "it should count the replays the pass did before it failed");
        logger.Entries.Should().Contain(e => e.Level == LogLevel.Warning && e.Message.Contains("aborted after 3", StringComparison.Ordinal),
            "it should log how far the pass got");
        store.Log.Should().EndWith("dispose", "it should hand back what it held");
    }

    [Fact]
    public async Task When_The_Filter_Throws()
    {
        for (var i = 0; i < 5; i++)
            store.Add($"m-{i}", DeadLetterReasons.Rejected);
        var examined = 0;

        var replay = () => Create().ReplayAsync(new DeadLetterReplayOptions<TestMessage>
        {
            MaxCount = 10,
            Filter = _ => ++examined == 3 ? throw new FormatException("bad filter") : true
        }, TestContext.Current.CancellationToken);

        await replay.Should().ThrowAsync<FormatException>("it should let the application's own filter exception through unchanged");
        telemetry.DeadLettersReplayed.Should().Equal([(Target, 2)], "it should count the replays done before the filter threw");
        logger.Entries.Should().Contain(e => e.Message.Contains("aborted after 2", StringComparison.Ordinal), "it should log how far it got");
        store.Letters.Should().HaveCount(3, "it should leave the rest as dead letters");
    }

    [Fact]
    public async Task When_A_Purge_Fails_Midway()
    {
        store.Add("a", DeadLetterReasons.Rejected);
        store.Add("b", DeadLetterReasons.Rejected);
        store.RemoveFailure = letter => letter.Metadata.MessageId == "b" ? new IOException("socket to host:5672 reset, password=s3cret") : null;

        var purge = () => Create().PurgeAsync(new DeadLetterPurgeOptions<TestMessage> { MaxCount = 10 }, TestContext.Current.CancellationToken);

        var thrown = (await purge.Should().ThrowAsync<ZedLabs.MessageQueue.Exceptions.MessageQueueException>("it should report the failed purge")).Which;
        thrown.ToString().Should().NotContain("s3cret", "it should not carry the client's text");
        telemetry.DeadLettersPurged.Should().Equal([(Target, 1)], "it should count what it deleted before the failure");
    }

    [Fact]
    public async Task When_The_Store_Cannot_Open_A_Pass()
    {
        store.OnOpen = () => throw new TimeoutException("could not reach broker.internal with key=s3cret");

        var peek = () => Create().PeekAsync(1, TestContext.Current.CancellationToken);

        var thrown = (await peek.Should().ThrowAsync<ZedLabs.MessageQueue.Exceptions.MessageQueueException>("it should wrap the client's failure")).Which;
        thrown.ToString().Should().NotContain("s3cret", "it should not carry the client's text");
        var again = () => Create().PeekAsync(1, TestContext.Current.CancellationToken);
        store.OnOpen = null;
        await again.Should().NotThrowAsync("it should not stay locked after a failed open");
    }

    [Fact]
    public void When_It_Is_Created_Without_Its_Collaborators()
    {
        var config = new InMemoryConfig { QueueName = "orders" };

        ((Action)(() => _ = new DeadLetterQueue<TestMessage>(null!, config, Target))).Should().Throw<ArgumentNullException>("it should require a store");
        ((Action)(() => _ = new DeadLetterQueue<TestMessage>(store, null!, Target))).Should().Throw<ArgumentNullException>("it should require a config");
        ((Action)(() => _ = new DeadLetterQueue<TestMessage>(store, config, null!))).Should().Throw<ArgumentNullException>("it should require a target");
    }
}
