using System.Text.Json;
using System.Text.RegularExpressions;
using AwesomeAssertions;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Extensions;
using ESE.MessageQueue.Hosting;
using ESE.MessageQueue.InMemory;
using ESE.MessageQueue.Processing;
using ESE.MessageQueue.Samples.Docs;
using ESE.MessageQueue.Telemetry;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using AzureServiceBusGuide = ESE.MessageQueue.Samples.Docs.AzureServiceBus.OrderMessaging;
using InMemoryGuide = ESE.MessageQueue.Samples.Docs.InMemory.OrderMessaging;
using RabbitMqGuide = ESE.MessageQueue.Samples.Docs.RabbitMq.OrderMessaging;

namespace ESE.MessageQueue.Tests.Packaging;

// The getting-started guides must not drift from the API. Every C# block in docs/getting-started is a copy of a
// "#region snippet:<name>" in samples/ESE.MessageQueue.Samples/Docs, which the build compiles; the in-memory guide is
// executed end to end. Every appsettings.json block and environment-variable block in the guides, the configuration
// reference and the runbooks is bound strictly and validated as at host start, so a renamed or removed setting fails here.
[Trait("Category", "Unit")]
public partial class Given_the_documentation_snippets
{
    const string GuideFolder = "docs/getting-started";
    const string SnippetFolder = "samples/ESE.MessageQueue.Samples/Docs";

    static readonly string[] ConfigurationPages =
    [
        "docs/getting-started/in-memory.md",
        "docs/getting-started/rabbitmq.md",
        "docs/getting-started/azure-service-bus.md",
        "docs/configuration.md",
        "docs/operations/runbooks.md",
        "docs/message-queue-api.md",
        "docs/azure-service-bus-api.md",
        "README.md"
    ];

    [GeneratedRegex(@"^\s*#region snippet:(?<name>[\w-]+)\s*$")]
    private static partial Regex RegionStart();

    [GeneratedRegex(@"^\s*#endregion\s*$")]
    private static partial Regex RegionEnd();

    [GeneratedRegex(@"^\s*(?:export\s+)?(?<key>[A-Za-z][A-Za-z0-9_]*__[A-Za-z0-9_]+)=(?<value>.*)$")]
    private static partial Regex EnvironmentVariable();

    public static TheoryData<string> Guides() => [.. GuidePaths()];

    public static TheoryData<string> Pages() => [.. ConfigurationPages];

    static IEnumerable<string> GuidePaths() =>
        Directory.GetFiles(RepositoryFiles.Path(GuideFolder), "*.md").Order(StringComparer.Ordinal)
            .Select(path => $"{GuideFolder}/{System.IO.Path.GetFileName(path)}");

    [Fact]
    public void When_Listing_The_Guides()
    {
        GuidePaths().Should().BeEquivalentTo(
            ["docs/getting-started/azure-service-bus.md", "docs/getting-started/in-memory.md", "docs/getting-started/rabbitmq.md"],
            "it should have one getting-started guide per provider");
        Regions().Should().HaveCountGreaterThan(10, "it should find the compiled snippet regions");
    }

    [Theory]
    [MemberData(nameof(Guides))]
    public void When_Comparing_A_Guide_With_The_Compiled_Code(string page)
    {
        var regions = Regions();
        var blocks = MarkdownDocument.Load(page).Blocks.Where(b => b.Language == "csharp").ToList();

        blocks.Should().HaveCountGreaterThanOrEqualTo(5, $"it should show the code of {page}");
        foreach (var block in blocks)
        {
            block.Snippet.Should().NotBeNull($"it should mark the C# block at {page}:{block.Line} with <!-- snippet: name -->");
            regions.Should().ContainKey(block.Snippet!, $"it should compile the snippet '{block.Snippet}' shown at {page}:{block.Line}");
            Normalize(block.Text).Should().Be(regions[block.Snippet!],
                $"it should show the compiled code of snippet '{block.Snippet}' unchanged at {page}:{block.Line}");
        }
    }

    [Theory]
    [MemberData(nameof(Pages))]
    public void When_Validating_The_Documented_Settings(string page)
    {
        var document = MarkdownDocument.Load(page);
        var json = document.Blocks.Where(b => b.Language == "json").ToList();
        var environment = document.Blocks.Where(b => b.Language == "bash" && b.Text.Split('\n').Any(l => EnvironmentVariable().IsMatch(l))).ToList();

        (json.Count + environment.Count).Should().BePositive($"it should find configuration examples in {page}");
        foreach (var block in json)
            ValidateQueues(FromJson(block.Text), $"{page}:{block.Line}");
        foreach (var block in environment)
            ValidateQueues(FromEnvironment(block.Text), $"{page}:{block.Line}");
    }

    [Fact]
    public void When_A_Documented_Setting_No_Longer_Binds()
    {
        var misspelt = new Dictionary<string, string?>
        {
            ["MessageQueue:Provider"] = "InMemory",
            ["MessageQueue:QueueName"] = "orders",
            ["MessageQueue:Retry:MaxRetry"] = "3"
        };

        var validate = () => ValidateQueues(misspelt, "a misspelt example");

        validate.Should().Throw<InvalidOperationException>("it should fail an example whose setting does not exist")
            .WithMessage("*MaxRetry*");
    }

    [Fact(Timeout = 30_000)]
    public async Task When_Running_The_In_Memory_Guide()
    {
        var token = TestContext.Current.CancellationToken;
        var settings = FromJson(MarkdownDocument.Load("docs/getting-started/in-memory.md").Blocks.First(b => b.Language == "json").Text);
        var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Configuration.AddInMemoryCollection(settings);
        InMemoryGuide.AddOrderMessaging(builder.Services, builder.Configuration);
        builder.Services.AddOrderTelemetry();
        builder.Services.AddLogging();
        using var host = builder.Build();

        await host.StartAsync(token);
        var orders = host.Services.GetRequiredService<OrderService>();
        await orders.PlaceAsync(new OrderPlaced("order-1", "customer-1", 42m), "checkout-1", token);
        await orders.PlaceAsync(new OrderPlaced("order-2", "customer-1", 0m), "checkout-2", token);
        await host.Services.GetRequiredService<InMemoryMessageQueue<OrderPlaced>>().WaitForIdleAsync(token);

        var ledger = (InMemoryOrderLedger)host.Services.GetRequiredService<IOrderLedger>();
        ledger.Orders.Keys.Should().Equal(["order-1"], "it should record the valid order under its message id");
        var deadLetters = host.Services.GetRequiredService<OrderDeadLetters>();
        var peeked = await deadLetters.PeekAsync(token);
        peeked.Should().ContainSingle("it should dead-letter the order with a zero amount")
            .Which.ReasonCode.Should().Be(DeadLetterReasons.Rejected, "it should dead-letter it as the processor's rejection");
        var replayed = await deadLetters.ReplayAsync([DeadLetterReasons.Rejected], token);
        replayed.Replayed.Should().Be(1, "it should replay the selected dead letter after counting it");
        var health = await host.Services.GetRequiredService<HealthCheckService>().CheckHealthAsync(token);
        health.Status.Should().Be(HealthStatus.Healthy, "it should register a passing health check");
        host.Services.GetRequiredService<ITelemetryProvider>().Should().NotBeSameAs(NullTelemetryProvider.Instance,
            "it should register the library's telemetry");
        await host.StopAsync(token);
    }

    [Theory]
    [InlineData("docs/getting-started/rabbitmq.md", "RabbitMq")]
    [InlineData("docs/getting-started/azure-service-bus.md", "AzureServiceBus")]
    public void When_Registering_A_Broker_Guide(string page, string provider)
    {
        var settings = FromJson(MarkdownDocument.Load(page).Blocks.First(b => b.Language == "json").Text);
        var services = new ServiceCollection().AddLogging();
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(settings).Build();
        if (provider == "RabbitMq")
            RabbitMqGuide.AddOrderMessaging(services, configuration);
        else
            AzureServiceBusGuide.AddOrderMessagingIdentity(
                AzureServiceBusGuide.AddOrderMessaging(services, configuration), "00000000-0000-0000-0000-000000000001");
        services.AddOrderTelemetry();
        using var container = services.BuildServiceProvider();

        container.GetRequiredService<IStartupValidator>().Validate();
        configuration["MessageQueue:Provider"].Should().Be(provider, $"it should configure the {provider} provider in {page}");
        container.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value.Registrations.Should()
            .ContainSingle($"it should register the {provider} health check");
        services.Should().ContainSingle(d => d.ImplementationType == typeof(MessageQueueHostedService<OrderPlaced>),
            "it should register the consuming hosted service");
    }

    static Dictionary<string, string> Regions()
    {
        var regions = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var file in Directory.GetFiles(RepositoryFiles.Path(SnippetFolder), "*.cs", SearchOption.AllDirectories).Order(StringComparer.Ordinal))
        {
            var parts = new Dictionary<string, List<List<string>>>(StringComparer.Ordinal);
            List<string>? current = null;
            foreach (var line in File.ReadLines(file))
            {
                if (RegionStart().Match(line) is { Success: true } start)
                {
                    var name = start.Groups["name"].Value;
                    if (!parts.TryGetValue(name, out var list))
                        parts[name] = list = [];
                    current = [];
                    list.Add(current);
                    continue;
                }
                if (current is not null && RegionEnd().IsMatch(line))
                {
                    current = null;
                    continue;
                }
                current?.Add(line);
            }

            foreach (var (name, regionParts) in parts)
            {
                if (regions.ContainsKey(name))
                    throw new InvalidOperationException($"Snippet '{name}' is defined in more than one file");
                regions[name] = Normalize(string.Join("\n\n", regionParts.Select(part => Normalize(string.Join('\n', Dedent(part))))));
            }
        }
        return regions;
    }

    // The parts of a region in one file are joined with a blank line (the namespace line between them is not shown);
    // a part inside a method loses that method's indentation.
    static IEnumerable<string> Dedent(List<string> lines)
    {
        var indent = lines.Where(l => l.Trim().Length > 0).Select(l => l.Length - l.TrimStart().Length).DefaultIfEmpty(0).Min();
        return lines.Select(l => l.Length >= indent ? l[indent..] : l.TrimStart());
    }

    static string Normalize(string text) =>
        string.Join('\n', text.Replace("\r", "", StringComparison.Ordinal).Split('\n').Select(l => l.TrimEnd())).Trim('\n');

    static Dictionary<string, string?> FromJson(string json)
    {
        var settings = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        using var document = JsonDocument.Parse(json, new JsonDocumentOptions { CommentHandling = JsonCommentHandling.Skip });
        Flatten(document.RootElement, string.Empty, settings);
        return settings;
    }

    static void Flatten(JsonElement element, string prefix, Dictionary<string, string?> settings)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                foreach (var property in element.EnumerateObject())
                    Flatten(property.Value, prefix.Length == 0 ? property.Name : $"{prefix}:{property.Name}", settings);
                break;
            case JsonValueKind.Array:
                var index = 0;
                foreach (var item in element.EnumerateArray())
                    Flatten(item, $"{prefix}:{index++}", settings);
                break;
            case JsonValueKind.Null:
                settings[prefix] = null;
                break;
            default:
                settings[prefix] = element.ValueKind == JsonValueKind.String ? element.GetString() : element.GetRawText();
                break;
        }
    }

    // `export MessageQueue__Retry__MaxRetries=5` is the key MessageQueue:Retry:MaxRetries, as the host's
    // environment-variable configuration provider reads it.
    static Dictionary<string, string?> FromEnvironment(string script)
    {
        var settings = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        foreach (var line in script.Split('\n'))
        {
            if (EnvironmentVariable().Match(line) is not { Success: true } variable)
                continue;
            var value = variable.Groups["value"].Value.Trim();
            if (value.Length >= 2 && (value[0] == '"' || value[0] == '\'') && value[^1] == value[0])
                value = value[1..^1];
            settings[variable.Groups["key"].Value.Replace("__", ":", StringComparison.Ordinal)] = value;
        }
        return settings;
    }

    // Each top-level section with a Provider is a queue: registered as AddMessageQueue<T>(configuration, section) with
    // every first-party provider, and validated as at host start (strict binding, enum names, every startup rule).
    static void ValidateQueues(Dictionary<string, string?> settings, string where)
    {
        var sections = settings.Keys.Where(k => k.EndsWith(":Provider", StringComparison.OrdinalIgnoreCase) && k.Count(c => c == ':') == 1)
            .Select(k => k[..k.IndexOf(':', StringComparison.Ordinal)]).ToList();
        sections.Should().NotBeEmpty($"it should configure at least one queue in the example at {where}");
        settings.Keys.Where(k => !sections.Any(s => k.StartsWith(s + ":", StringComparison.OrdinalIgnoreCase)))
            .Where(k => !k.StartsWith("Logging:", StringComparison.OrdinalIgnoreCase))
            .Should().BeEmpty($"it should only show queue (or Logging) settings in the example at {where}");

        var configuration = new ConfigurationBuilder().AddInMemoryCollection(settings).Build();
        var registrations = new (string Section, Action<IServiceCollection> Register)[]
        {
            ("first", s => s.AddMessageQueue<DocumentedQueue1>(configuration, sections[0])),
            ("second", s => s.AddMessageQueue<DocumentedQueue2>(configuration, sections[1])),
            ("third", s => s.AddMessageQueue<DocumentedQueue3>(configuration, sections[2]))
        };
        sections.Should().HaveCountLessThanOrEqualTo(registrations.Length, $"it should show at most three queues per example at {where}");

        var services = new ServiceCollection().AddRabbitMqProvider().AddAzureServiceBusProvider();
        foreach (var registration in registrations.Take(sections.Count))
            registration.Register(services);
        using var container = services.BuildServiceProvider();
        try
        {
            container.GetRequiredService<IStartupValidator>().Validate();
        }
        catch (Exception error) when (error is OptionsValidationException or InvalidOperationException)
        {
            throw new InvalidOperationException($"The example at {where} does not start: {error.Message}", error);
        }
    }

    sealed record DocumentedQueue1(string Id);

    sealed record DocumentedQueue2(string Id);

    sealed record DocumentedQueue3(string Id);
}
