using AwesomeAssertions;
using ZedLabs.MessageQueue.Processing;

namespace ZedLabs.MessageQueue.Tests.Processing;

[Trait("Category", "Unit")]
public class Given_a_round_robin_counter
{
    [Fact]
    public void When_The_Counter_Wraps_Past_IntMaxValue()
    {
        var counter = int.MaxValue - 2;

        var indexes = Enumerable.Range(0, 6).Select(_ => RoundRobin.Next(ref counter, 3)).ToList();

        indexes.Should().OnlyContain(i => i >= 0 && i < 3, "it should stay in range after the counter overflows (B1)");
    }

    [Fact]
    public void When_Cycling_Through_A_Pool()
    {
        var counter = 0;

        var indexes = Enumerable.Range(0, 4).Select(_ => RoundRobin.Next(ref counter, 2)).ToList();

        indexes.Should().Equal([1, 0, 1, 0], "it should alternate between pool members");
    }
}
