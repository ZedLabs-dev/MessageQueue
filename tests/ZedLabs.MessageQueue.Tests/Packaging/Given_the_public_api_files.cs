using AwesomeAssertions;

namespace ZedLabs.MessageQueue.Tests.Packaging;

// ADR 0016: nullable-oblivious ("~") entries hide whether a member accepts or returns null. New API is declared with
// its annotations; the oblivious entries already in PublicAPI.Shipped.txt are retired the analyzer's way, by a
// "*REMOVED*~" line or an annotated replacement in PublicAPI.Unshipped.txt, until /release moves Unshipped to Shipped.
[Trait("Category", "Unit")]
public class Given_the_public_api_files
{
    const string Removed = "*REMOVED*";

    public static TheoryData<string> Packages() => ["ZedLabs.MessageQueue", "ZedLabs.MessageQueue.RabbitMQ", "ZedLabs.MessageQueue.AzureServiceBus"];

    static string[] Lines(string package, string file) => File.ReadAllLines(RepositoryFiles.Path($"src/{package}/{file}"));

    // The entry without nullable annotations: the analyzer treats "~X(object obj)" and "X(object? obj)" as one member.
    static string Unannotated(string entry) => entry.TrimStart('~').Replace("!", "", StringComparison.Ordinal).Replace("?", "", StringComparison.Ordinal);

    [Theory]
    [MemberData(nameof(Packages))]
    public void When_Reading_The_Unshipped_Entries(string package) =>
        Lines(package, "PublicAPI.Unshipped.txt").Where(line => line.TrimStart().StartsWith('~'))
            .Should().BeEmpty("it should declare every new public member with nullable annotations, not as oblivious");

    [Theory]
    [MemberData(nameof(Packages))]
    public void When_Reading_The_Shipped_Oblivious_Entries(string package)
    {
        var unshipped = Lines(package, "PublicAPI.Unshipped.txt");
        var retired = unshipped.Where(l => l.StartsWith(Removed, StringComparison.Ordinal)).Select(l => l[Removed.Length..]).ToHashSet();
        var replacements = unshipped.Where(l => !l.StartsWith(Removed, StringComparison.Ordinal) && !l.StartsWith('~')).Select(Unannotated).ToHashSet();

        Lines(package, "PublicAPI.Shipped.txt").Where(l => l.StartsWith('~'))
            .Where(l => !retired.Contains(l) && !replacements.Contains(Unannotated(l)))
            .Should().BeEmpty("it should retire every shipped oblivious entry with a *REMOVED* line or an annotated replacement");
    }
}
