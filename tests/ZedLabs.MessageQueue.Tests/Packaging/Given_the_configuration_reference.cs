using System.Globalization;
using System.Reflection;
using AwesomeAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ZedLabs.MessageQueue.AzureServiceBus;
using ZedLabs.MessageQueue.Configuration;
using ZedLabs.MessageQueue.Extensions;
using ZedLabs.MessageQueue.InMemory;
using ZedLabs.MessageQueue.RabbitMQ;
using ZedLabs.MessageQueue.Retry;

namespace ZedLabs.MessageQueue.Tests.Packaging;

// docs/configuration.md is the settings reference operators work from: every setting of every config class is listed
// with its real default, and every quoted startup error is the text the library actually produces for that value.
[Trait("Category", "Unit")]
public class Given_the_configuration_reference
{
    const string Page = "docs/configuration.md";

    // The config type each reference table documents, and an instance holding its defaults.
    static readonly (Type Type, object Defaults)[] ConfigTypes =
    [
        (typeof(MessageQueueConfig), new InMemoryConfig()),
        (typeof(RetryPolicy), new RetryPolicy()),
        (typeof(InMemoryConfig), new InMemoryConfig()),
        (typeof(RabbitMqConfig), new RabbitMqConfig()),
        (typeof(RabbitMqTlsConfig), new RabbitMqTlsConfig()),
        (typeof(AzureServiceBusConfig), new AzureServiceBusConfig())
    ];

    static readonly Dictionary<string, Dictionary<string, string?>> Baselines = new(StringComparer.OrdinalIgnoreCase)
    {
        ["InMemory"] = new() { ["MessageQueue:Provider"] = "InMemory", ["MessageQueue:QueueName"] = "orders" },
        ["RabbitMq"] = new()
        {
            ["MessageQueue:Provider"] = "RabbitMq",
            ["MessageQueue:ConnectionString"] = "amqps://rabbit.example.com",
            ["MessageQueue:QueueName"] = "orders"
        },
        ["AzureServiceBus"] = new()
        {
            ["MessageQueue:Provider"] = "AzureServiceBus",
            ["MessageQueue:FullyQualifiedNamespace"] = "contoso.servicebus.windows.net",
            ["MessageQueue:QueueName"] = "orders"
        }
    };

    public static TheoryData<string> TypeNames() => [.. ConfigTypes.Select(c => c.Type.Name)];

    [Theory]
    [MemberData(nameof(TypeNames))]
    public void When_Reading_The_Table_Of_A_Config_Type(string typeName)
    {
        var (type, defaults) = ConfigTypes.Single(c => c.Type.Name == typeName);
        var table = SettingsTable(type);
        var setting = table.Column("Setting");
        var defaultColumn = table.Column("Default");
        var documented = table.Rows.ToDictionary(
            row => MarkdownDocument.CodeSpans(row[setting])[0].Split(':')[^1],
            row => row[defaultColumn],
            StringComparer.Ordinal);

        documented.Keys.Should().BeEquivalentTo(Settings(type).Select(p => p.Name), $"it should list every setting of {type.Name}, and only those");
        foreach (var property in Settings(type).Where(p => p.Name != nameof(MessageQueueConfig.Provider)))
        {
            var cell = documented[property.Name];
            var spans = MarkdownDocument.CodeSpans(cell);
            var shown = spans.Count > 0 ? spans[0] : cell.Trim();
            shown.Should().Be(Format(property.GetValue(defaults)), $"it should show the default of {type.Name}.{property.Name}");
        }
    }

    [Fact]
    public void When_Reading_The_Startup_Errors()
    {
        var table = MarkdownDocument.Load(Page).Tables.Single(t => t.Heading == "Startup errors");
        var provider = table.Column("Provider");
        var setting = table.Column("Setting");
        var message = table.Column("Message");

        table.Rows.Should().HaveCountGreaterThan(30, "it should quote the startup errors operators meet");
        foreach (var row in table.Rows)
        {
            var overrides = MarkdownDocument.CodeSpans(row[setting]);
            var expected = MarkdownDocument.CodeSpans(row[message]).Single();
            var start = Startup(row[provider], overrides);

            start.Should().Throw<Exception>($"it should fail to start with {string.Join(", ", overrides)} on {row[provider]}")
                .Which.Message.Should().Contain(expected, $"it should quote the error for {string.Join(", ", overrides)} as the library reports it");
        }
    }

    // Registration (AddMessageQueue: the provider) and host start (binding and validation), as a host runs them.
    static Action Startup(string provider, IReadOnlyList<string> overrides)
    {
        var values = new Dictionary<string, string?>(Baselines[provider]);
        foreach (var assignment in overrides)
        {
            var separator = assignment.IndexOf('=', StringComparison.Ordinal);
            values[$"MessageQueue:{assignment[..separator]}"] = assignment[(separator + 1)..];
        }
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(values).Build();
        return () =>
        {
            var services = new ServiceCollection().AddRabbitMqProvider().AddAzureServiceBusProvider();
            services.AddMessageQueue<TestMessage>(configuration);
            using var container = services.BuildServiceProvider();
            container.GetRequiredService<IStartupValidator>().Validate();
        };
    }

    static MarkdownDocument.Table SettingsTable(Type type)
    {
        var tables = MarkdownDocument.Load(Page).Tables
            .Where(t => t.Heading.Contains($"`{type.Name}`", StringComparison.Ordinal) && t.Header.Contains("Setting"))
            .ToList();
        tables.Should().ContainSingle($"it should have one reference table for {type.Name} in {Page}");
        return tables[0];
    }

    // The settings a type declares itself: bindable public properties, plus the code-only ones (JsonTypeInfoResolver,
    // the retry delegates) and Provider, which every derived type overrides and the base table documents.
    static IEnumerable<PropertyInfo> Settings(Type type) =>
        type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(p => p.GetIndexParameters().Length == 0)
            .Where(p => type == typeof(MessageQueueConfig) || p.Name != nameof(MessageQueueConfig.Provider));

    // How the reference writes a default: the configuration value an operator would type.
    static string Format(object? value) => value switch
    {
        null => "(unset)",
        string { Length: 0 } => "(empty)",
        string text => text,
        bool flag => flag ? "true" : "false",
        TimeSpan span => span.ToString("c", CultureInfo.InvariantCulture),
        Enum member => member.ToString(),
        IFormattable number => number.ToString(null, CultureInfo.InvariantCulture),
        _ => "(section)"
    };
}
