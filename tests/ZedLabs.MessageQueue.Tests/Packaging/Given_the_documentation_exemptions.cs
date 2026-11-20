using System.Text.RegularExpressions;
using AwesomeAssertions;

namespace ZedLabs.MessageQueue.Tests.Packaging;

// .editorconfig exempts settings classes from CS1591 per file. A second type added to such a file would be exempt too,
// so every exempt file must declare exactly one type: the convention is enforced here, not just written down.
[Trait("Category", "Unit")]
public partial class Given_the_documentation_exemptions
{
    const string Exemption = "dotnet_diagnostic.CS1591.severity = none";

    // Comments, then string and character literals (verbatim, interpolated and regular), so that words in them do not count.
    [GeneratedRegex("""//[^\n]*|/\*.*?\*/|@"(?:[^"]|"")*"|\$?"(?:[^"\\\n]|\\.)*"|'(?:[^'\\\n]|\\.)+'""", RegexOptions.Singleline)]
    private static partial Regex CommentsAndLiterals();

    // Type declarations: "record struct"/"record class" count once, a generic constraint ("where T : class") never.
    [GeneratedRegex("""\brecord\s+(?:class|struct)\s+[A-Za-z_]|\b(?:class|struct|interface|enum|record)\s+[A-Za-z_]\w*\s*[\s({<:;]|\bdelegate\s+[\w<>\[\],?.\s]+?\s+[A-Za-z_]\w*\s*[(<]""")]
    private static partial Regex TypeDeclarations();

    [GeneratedRegex("""^\[(?<glob>.+)\]\s*$""")]
    private static partial Regex Section();

    public static IReadOnlyList<string> ExemptFiles()
    {
        var files = new List<string>();
        string? glob = null;
        foreach (var line in File.ReadLines(RepositoryFiles.Path(".editorconfig")).Select(l => l.Trim()))
        {
            if (Section().Match(line) is { Success: true } section)
                glob = section.Groups["glob"].Value;
            else if (glob is not null && line.Replace(" ", "", StringComparison.Ordinal) == Exemption.Replace(" ", "", StringComparison.Ordinal))
                files.AddRange(glob.Trim('{', '}').Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries));
        }
        return files;
    }

    public static int CountTypes(string source) =>
        TypeDeclarations().Count(CommentsAndLiterals().Replace(source, " "));

    public static TheoryData<string> Files() => [.. ExemptFiles()];

    [Fact]
    public void When_Reading_The_Exemptions() =>
        ExemptFiles().Should().NotBeEmpty("it should find the settings files exempted from CS1591")
            .And.OnlyContain(f => f.EndsWith(".cs", StringComparison.Ordinal) && !f.Contains('*', StringComparison.Ordinal),
                "it should exempt named files only, never a pattern that could match new files");

    [Theory]
    [MemberData(nameof(Files))]
    public void When_Inspecting_An_Exempt_File(string file)
    {
        var path = RepositoryFiles.Path(file);
        File.Exists(path).Should().BeTrue($"it should exempt only files that exist ({file})");

        CountTypes(File.ReadAllText(path)).Should().Be(1, $"it should declare exactly one settings type in {file}");
    }

    [Theory]
    [InlineData("public sealed record A { }", 1)]
    [InlineData("public sealed record A;\npublic sealed class B { }", 2)]
    [InlineData("public readonly record struct A(int X);", 1)]
    [InlineData("public class A<T> where T : class, new() { }", 1)]
    [InlineData("// class Hidden\n/* struct Hidden */ public enum E { A }", 1)]
    [InlineData("public record A { string S = \"class Hidden\"; }", 1)]
    [InlineData("public class A { } public interface I { } public delegate void D(int x);", 3)]
    public void When_Counting_Type_Declarations(string source, int expected) =>
        CountTypes(source).Should().Be(expected, "it should count type declarations, not keywords in comments, strings or constraints");
}
