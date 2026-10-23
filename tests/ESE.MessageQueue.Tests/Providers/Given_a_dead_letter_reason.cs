using AwesomeAssertions;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Processing;
using ESE.MessageQueue.Providers;

namespace ESE.MessageQueue.Tests.Providers;

[Trait("Category", "Unit")]
public class Given_a_dead_letter_reason
{
    [Fact]
    public void When_Comparing_Equal_Values()
    {
        var reason = new DeadLetterReason(DeadLetterReasons.NonRetryableError, "System.InvalidOperationException");
        var same = new DeadLetterReason(DeadLetterReasons.NonRetryableError, "System.InvalidOperationException");

        reason.Equals(same).Should().BeTrue("it should compare by value");
        reason.Equals((object)same).Should().BeTrue("it should compare by value when boxed");
        (reason == same).Should().BeTrue("it should compare by value with ==");
        (reason != same).Should().BeFalse("it should compare by value with !=");
        reason.GetHashCode().Should().Be(same.GetHashCode(), "it should hash equal values alike");
    }

    [Fact]
    public void When_Comparing_Different_Values()
    {
        var reason = new DeadLetterReason(DeadLetterReasons.NonRetryableError, "A");

        reason.Should().NotBe(new DeadLetterReason(DeadLetterReasons.NonRetryableError, "B"), "it should compare the detail");
        reason.Should().NotBe(new DeadLetterReason(DeadLetterReasons.Rejected, "A"), "it should compare the code");
        (reason != new DeadLetterReason(DeadLetterReasons.Rejected, "A")).Should().BeTrue("it should compare by value with !=");
        reason.Equals((object?)null).Should().BeFalse("it should not equal null");
        reason.Equals("NonRetryableError").Should().BeFalse("it should not equal another type");
        default(DeadLetterReason).Should().NotBe(new DeadLetterReason(DeadLetterReasons.DeliveryFailed),
            "it should keep a default reason distinct from an explicit one, as a record struct does");
    }

    [Fact]
    public void When_Formatting_It() =>
        new DeadLetterReason(DeadLetterReasons.Rejected, "detail").ToString()
            .Should().Be("DeadLetterReason { Code = Rejected, Detail = detail }", "it should print like a record");

    [Fact]
    public void When_It_Is_Default()
    {
        default(DeadLetterReason).Code.Should().Be(DeadLetterReasons.DeliveryFailed, "it should read a default reason as DeliveryFailed");
        default(DeadLetterReason).ToString().Should().Be("DeadLetterReason { Code = DeliveryFailed, Detail =  }", "it should print like a record");
    }

    // S6: one limit for every free-text value the library writes into a broker, a span or a log.
    const int Limit = PublishOptions.MaxHeaderValueLength;

    [Fact]
    public void When_The_Detail_Is_Longer_Than_The_Limit()
    {
        var reason = new DeadLetterReason(DeadLetterReasons.DeliveryFailed, new string('d', Limit + 1));

        reason.Detail.Should().Be(new string('d', Limit), "it should cut a long detail to the shared limit");
    }

    [Fact]
    public void When_The_Detail_Is_Exactly_At_The_Limit()
    {
        var detail = new string('d', Limit);

        new DeadLetterReason(DeadLetterReasons.DeliveryFailed, detail).Detail.Should().BeSameAs(detail,
            "it should keep a detail that fits unchanged");
    }

    [Fact]
    public void When_A_Value_That_Fits_Ends_With_Half_A_Surrogate_Pair()
    {
        var detail = new string('d', Limit - 1) + "\uD83D";

        new DeadLetterReason(DeadLetterReasons.DeliveryFailed, detail).Detail.Should().Be(detail,
            "it should cut only values over the limit, whatever they end with");
    }

    [Fact]
    public void When_The_Code_Is_Longer_Than_The_Limit() =>
        new DeadLetterReason(new string('c', Limit * 2)).Code.Should().HaveLength(Limit,
            "it should cut a long code too: providers store it on the message and tag metrics with it");

    [Fact]
    public void When_The_Cut_Would_Split_A_Surrogate_Pair()
    {
        var detail = new string('d', Limit - 1) + "\U0001F600tail";

        var cut = new DeadLetterReason(DeadLetterReasons.DeliveryFailed, detail).Detail!;

        cut.Should().Be(new string('d', Limit - 1), "it should not leave half of a surrogate pair at the end");
    }

    [Fact]
    public void When_Details_Differ_Only_In_Case() =>
        new DeadLetterReason(DeadLetterReasons.Rejected, "detail").Should().NotBe(new DeadLetterReason(DeadLetterReasons.Rejected, "DETAIL"),
            "it should compare the detail ordinally");
}
