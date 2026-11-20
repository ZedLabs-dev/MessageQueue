using System.Reflection;
using AwesomeAssertions;
using Microsoft.Extensions.Logging;
using ZedLabs.MessageQueue.AzureServiceBus;
using ZedLabs.MessageQueue.Configuration;
using ZedLabs.MessageQueue.RabbitMQ;

namespace ZedLabs.MessageQueue.Tests.Processing;

// Event ids are how operators filter and alert on log lines; two messages sharing one would be indistinguishable.
[Trait("Category", "Unit")]
public class Given_the_library_log_messages
{
    [Fact]
    public void When_Listing_Their_Event_Ids()
    {
        // Across the core and provider packages: an app logs all of them through one ILoggerFactory.
        var assemblies = new[] { typeof(MessageQueueConfig).Assembly, typeof(RabbitMqConfig).Assembly, typeof(AzureServiceBusConfig).Assembly };
        var messages = assemblies.SelectMany(a => a.GetTypes())
            .SelectMany(t => t.GetMethods(BindingFlags.Static | BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly))
            .Select(m => (Method: $"{m.DeclaringType!.Name}.{m.Name}", Attribute: m.GetCustomAttribute<LoggerMessageAttribute>()))
            .Where(m => m.Attribute is not null)
            .ToList();

        messages.Should().HaveCountGreaterThan(20, "it should find the library's source-generated log messages");
        messages.Should().AllSatisfy(m => m.Attribute!.EventId.Should().BePositive($"it should give {m.Method} an explicit event id"));
        messages.GroupBy(m => m.Attribute!.EventId).Where(g => g.Count() > 1)
            .Select(g => $"{g.Key}: {string.Join(", ", g.Select(m => m.Method))}")
            .Should().BeEmpty("it should give every log message its own event id");
    }
}
