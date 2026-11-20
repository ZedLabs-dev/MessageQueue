using System.Text.RegularExpressions;

namespace ZedLabs.MessageQueue.Benchmarks;

// Mirrors BenchmarkDotNet's --filter globs ("*" and "?", matched against Namespace.Type.Method), so Program knows
// whether a benchmark class will run before deciding to start a broker for it.
static class BenchmarkFilter
{
    public static bool Selects(string[] args, Type benchmarks)
    {
        var patterns = new List<string>();
        for (var i = 0; i < args.Length; i++)
        {
            if (args[i] is not ("--filter" or "-f"))
                continue;
            for (var j = i + 1; j < args.Length && !args[j].StartsWith('-'); j++)
                patterns.Add(args[j]);
        }

        var names = benchmarks.GetMethods().Where(m => m.GetCustomAttributes(typeof(BenchmarkDotNet.Attributes.BenchmarkAttribute), false).Length > 0)
            .Select(m => $"{benchmarks.FullName}.{m.Name}");
        return patterns.Any(pattern => names.Any(name => Glob(pattern).IsMatch(name)));
    }

    static Regex Glob(string pattern) =>
        new("^" + Regex.Escape(pattern).Replace(@"\*", ".*", StringComparison.Ordinal).Replace(@"\?", ".", StringComparison.Ordinal) + "$",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));
}
