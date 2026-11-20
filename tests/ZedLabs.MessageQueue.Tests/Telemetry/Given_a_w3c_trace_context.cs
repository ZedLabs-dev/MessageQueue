using AwesomeAssertions;
using ZedLabs.MessageQueue.Telemetry;
using ZedLabs.MessageQueue.Telemetry.OpenTelemetry;

namespace ZedLabs.MessageQueue.Tests.Telemetry;

// Trace headers arrive from the broker, so from anyone who can publish: parsing is strict (W3C Trace Context level 1),
// bounded, and never throws. A bad traceparent means no parent; a bad tracestate is dropped on its own.
[Trait("Category", "Unit")]
public class Given_a_w3c_trace_context
{
    const string TraceId = "0af7651916cd43dd8448eb211c80319c";
    const string SpanId = "b7ad6b7169203331";
    const string Valid = $"00-{TraceId}-{SpanId}-01";

    [Fact]
    public void When_Parsing_A_Valid_Traceparent()
    {
        var context = TraceContext.Parse(Valid, null);

        context.Should().NotBeNull("it should accept a well-formed version 00 traceparent");
        context!.TraceId.Should().Be(TraceId, "it should read the trace id");
        context.SpanId.Should().Be(SpanId, "it should read the parent span id");
        context.Sampled.Should().BeTrue("it should read the sampled flag");
        context.ToTraceParent().Should().Be(Valid, "it should format the same traceparent back");
    }

    [Fact]
    public void When_The_Traceparent_Is_Not_Sampled()
    {
        TraceContext.Parse($"00-{TraceId}-{SpanId}-00", null)!.Sampled.Should().BeFalse("it should read a cleared sampled flag");
    }

    [Theory]
    [InlineData("00", "00")]
    [InlineData("01", "01")]
    [InlineData("02", "02")]
    [InlineData("03", "03")]
    [InlineData("ff", "03")]
    [InlineData("05", "01")]
    public void When_Round_Tripping_The_Trace_Flags(string flags, string expected)
    {
        var context = TraceContext.Parse($"00-{TraceId}-{SpanId}-{flags}", null);

        context!.ToTraceParent().Should().Be($"00-{TraceId}-{SpanId}-{expected}",
            "it should keep the sampled and random flags (W3C level 2) and clear the flags it does not know");
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("00")]
    [InlineData("00-" + TraceId + "-" + SpanId)]
    [InlineData("00-" + TraceId + "-" + SpanId + "-")]
    [InlineData("00-" + TraceId + "-" + SpanId + "-1")]
    [InlineData("00-" + TraceId + "-" + SpanId + "-01-extra")]
    [InlineData("00-0AF7651916CD43DD8448EB211C80319C-" + SpanId + "-01")]
    [InlineData("00-" + TraceId + "-B7AD6B7169203331-01")]
    [InlineData("00-00000000000000000000000000000000-" + SpanId + "-01")]
    [InlineData("00-" + TraceId + "-0000000000000000-01")]
    [InlineData("ff-" + TraceId + "-" + SpanId + "-01")]
    [InlineData("0g-" + TraceId + "-" + SpanId + "-01")]
    [InlineData("00-" + TraceId + "-" + SpanId + "-0z")]
    [InlineData("00-xyz-abc-01")]
    [InlineData("00_" + TraceId + "_" + SpanId + "_01")]
    [InlineData("00-" + TraceId + "1-" + SpanId + "-01")]
    [InlineData("00-" + TraceId + "-" + SpanId + "1-01")]
    [InlineData(" 00-" + TraceId + "-" + SpanId + "-01")]
    [InlineData("01-" + TraceId + "-" + SpanId + "-01extra")]
    [InlineData("00-0af7651916cd43dd8448eb211c80319é-" + SpanId + "-01")]
    public void When_The_Traceparent_Is_Malformed(string traceParent)
    {
        var parse = () => TraceContext.Parse(traceParent, "vendor=value");

        parse.Should().NotThrow("it should never throw on broker input")
            .Which.Should().BeNull("it should ignore a malformed traceparent");
    }

    [Fact]
    public void When_The_Traceparent_Is_Huge()
    {
        var huge = "01-" + TraceId + "-" + SpanId + "-01-" + new string('a', 100_000);

        TraceContext.Parse(huge, null).Should().BeNull("it should refuse a traceparent beyond the length bound without parsing it");
    }

    [Fact]
    public void When_A_Future_Version_Adds_Fields()
    {
        var context = TraceContext.Parse($"cc-{TraceId}-{SpanId}-01-what-the-future-holds", null);

        context.Should().NotBeNull("it should read the version 00 fields of a later version, as the specification requires");
        context!.TraceId.Should().Be(TraceId, "it should read the trace id of a later version");
    }

    [Fact]
    public void When_The_Tracestate_Is_Valid()
    {
        var context = TraceContext.Parse(Valid, "congo=t61rcWkgMzE, rojo=00f067aa0ba902b7,tenant@vendor=x");

        context!.TraceState.Should().Be("congo=t61rcWkgMzE, rojo=00f067aa0ba902b7,tenant@vendor=x", "it should keep a valid tracestate");
    }

    [Theory]
    [InlineData("no-equals-sign")]
    [InlineData("UPPER=value")]
    [InlineData("key=")]
    [InlineData("=value")]
    [InlineData("key=va,lue=x=y")]
    [InlineData("key=value\u0001")]
    [InlineData("key=va\u007Fl")]
    [InlineData("dup=1,dup=2")]
    public void When_The_Tracestate_Is_Malformed(string traceState)
    {
        var context = TraceContext.Parse(Valid, traceState);

        context.Should().NotBeNull("it should keep the traceparent when only the tracestate is bad");
        context!.TraceState.Should().BeNull("it should drop a malformed tracestate");
    }

    [Fact]
    public void When_The_Tracestate_Is_Too_Long()
    {
        var members = string.Join(',', Enumerable.Range(0, 33).Select(i => $"k{i}=v"));
        var longValue = "key=" + new string('v', 600);

        TraceContext.Parse(Valid, members)!.TraceState.Should().BeNull("it should drop a tracestate with more than 32 members");
        TraceContext.Parse(Valid, longValue)!.TraceState.Should().BeNull("it should drop a tracestate beyond the length bound");
    }

    [Fact]
    public void When_The_Provider_Extracts_A_Malformed_Header()
    {
        var provider = new OpenTelemetryProvider("ZedLabs.MessageQueue.Tests.W3C");
        var headers = new Dictionary<string, string> { ["traceparent"] = "00-xyz-abc-01", ["tracestate"] = "a=b" };

        var extract = () => provider.ExtractTraceContext(headers);

        extract.Should().NotThrow("it should never throw on broker input").Which.Should().BeNull("it should ignore a malformed traceparent");
    }
}
