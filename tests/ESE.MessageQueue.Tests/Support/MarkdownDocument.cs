using System.Text.RegularExpressions;

namespace ESE.MessageQueue.Tests;

// The parts of a documentation page the documentation specs check: fenced code blocks (with the
// "<!-- snippet: name -->" marker that may precede one) and the rows of pipe tables under each heading.
sealed partial class MarkdownDocument
{
    [GeneratedRegex(@"^<!--\s*snippet:\s*(?<name>[\w-]+)\s*-->$")]
    private static partial Regex SnippetMarker();

    [GeneratedRegex(@"^#{1,6}\s+(?<title>.+)$")]
    private static partial Regex Heading();

    MarkdownDocument(string path, IReadOnlyList<CodeBlock> blocks, IReadOnlyList<Table> tables)
    {
        Path = path;
        Blocks = blocks;
        Tables = tables;
    }

    public string Path { get; }

    public IReadOnlyList<CodeBlock> Blocks { get; }

    public IReadOnlyList<Table> Tables { get; }

    public static MarkdownDocument Load(string relativePath)
    {
        var lines = File.ReadAllLines(RepositoryFiles.Path(relativePath));
        var blocks = new List<CodeBlock>();
        var tables = new List<Table>();
        var heading = string.Empty;
        string? marker = null;
        for (var i = 0; i < lines.Length; i++)
        {
            var line = lines[i];
            if (line.StartsWith("```", StringComparison.Ordinal))
            {
                var language = line[3..].Trim();
                var start = i + 1;
                var end = start;
                while (end < lines.Length && !lines[end].StartsWith("```", StringComparison.Ordinal))
                    end++;
                blocks.Add(new CodeBlock(language, string.Join('\n', lines[start..end]), marker, start + 1));
                marker = null;
                i = end;
                continue;
            }

            if (SnippetMarker().Match(line.Trim()) is { Success: true } snippet)
            {
                marker = snippet.Groups["name"].Value;
                continue;
            }

            if (Heading().Match(line) is { Success: true } title)
                heading = title.Groups["title"].Value.Trim();

            if (line.TrimStart().StartsWith('|') && i + 1 < lines.Length && IsSeparator(lines[i + 1]))
            {
                var header = Cells(line);
                var rows = new List<IReadOnlyList<string>>();
                for (i += 2; i < lines.Length && lines[i].TrimStart().StartsWith('|'); i++)
                    rows.Add(Cells(lines[i]));
                tables.Add(new Table(heading, header, rows));
                i--;
            }

            if (line.Trim().Length > 0)
                marker = null;
        }
        return new MarkdownDocument(relativePath, blocks, tables);
    }

    static bool IsSeparator(string line) =>
        line.TrimStart().StartsWith('|') && line.Trim().Trim('|').Split('|').All(cell => cell.Trim().Trim(':').Trim('-').Length == 0);

    // Splits a table row on the pipes that are not inside backticks or escaped.
    static List<string> Cells(string row)
    {
        var cells = new List<string>();
        var current = new System.Text.StringBuilder();
        var inCode = false;
        var text = row.Trim();
        for (var i = 0; i < text.Length; i++)
        {
            var c = text[i];
            if (c == '`')
                inCode = !inCode;
            if (c == '\\' && i + 1 < text.Length && text[i + 1] == '|')
            {
                current.Append('|');
                i++;
                continue;
            }
            if (c == '|' && !inCode)
            {
                cells.Add(current.ToString().Trim());
                current.Clear();
                continue;
            }
            current.Append(c);
        }
        cells.Add(current.ToString().Trim());
        // A row starts and ends with a pipe: drop the empty cells outside them.
        return cells.Count >= 2 ? cells[1..^1] : cells;
    }

    public sealed record CodeBlock(string Language, string Text, string? Snippet, int Line);

    public sealed record Table(string Heading, IReadOnlyList<string> Header, IReadOnlyList<IReadOnlyList<string>> Rows)
    {
        public int Column(string name)
        {
            var index = Header.ToList().FindIndex(h => string.Equals(h, name, StringComparison.OrdinalIgnoreCase));
            if (index < 0)
                throw new InvalidOperationException($"The table under '{Heading}' has no '{name}' column");
            return index;
        }
    }

    [GeneratedRegex("`([^`]*)`")]
    private static partial Regex CodeSpan();

    // The text of every `code span` in a cell, in order.
    public static IReadOnlyList<string> CodeSpans(string cell) =>
        [.. CodeSpan().Matches(cell).Select(m => m.Groups[1].Value)];
}
