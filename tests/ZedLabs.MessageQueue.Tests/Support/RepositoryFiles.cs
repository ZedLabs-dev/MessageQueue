namespace ZedLabs.MessageQueue.Tests;

// Files of the repository the tests were built from: the folder holding ZedLabs.MessageQueue.sln above the test binaries.
static class RepositoryFiles
{
    static readonly Lazy<string> Root = new(() =>
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
        {
            if (File.Exists(System.IO.Path.Combine(dir.FullName, "ZedLabs.MessageQueue.sln")))
                return dir.FullName;
        }
        throw new InvalidOperationException($"No ZedLabs.MessageQueue.sln above {AppContext.BaseDirectory}");
    });

    public static string Path(string relative) => System.IO.Path.Combine(Root.Value, relative);
}
