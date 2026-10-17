using AwesomeAssertions;
using ESE.MessageQueue.Abstractions;

namespace ESE.MessageQueue.Tests.Abstractions;

[Trait("Category", "Unit")]
public class Given_a_message_context
{
    [Fact]
    public void When_A_Handler_Tries_To_Change_Default_Headers()
    {
        var context = new MessageContext { MessageId = "m-1" };

        var act = () => ((IDictionary<string, string>)context.Headers).Add("leak", "x");

        act.Should().Throw<NotSupportedException>("it should not let one context's mutation leak into every other context");
        new MessageContext { MessageId = "m-2" }.Headers.Should().BeEmpty("it should keep the shared empty headers empty");
    }

    [Fact]
    public void When_A_Handler_Tries_To_Change_Headers()
    {
        var context = new MessageContext { MessageId = "m-1", Headers = new Dictionary<string, string> { ["tenant"] = "a" } };

        var act = () => ((IDictionary<string, string>)context.Headers)["tenant"] = "b";

        act.Should().Throw<NotSupportedException>("it should not let a handler change headers that a retry will see again");
    }

    [Fact]
    public void When_The_Source_Dictionary_Changes_Later()
    {
        var source = new Dictionary<string, string> { ["tenant"] = "a" };
        var context = new MessageContext { MessageId = "m-1", Headers = source };

        source["tenant"] = "b";

        context.Headers["tenant"].Should().Be("a", "it should copy the headers it was given");
    }
}
