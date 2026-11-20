using System.Diagnostics;
using System.IO.Compression;
using System.Reflection;
using System.Xml.Linq;
using AwesomeAssertions;

namespace ZedLabs.MessageQueue.Tests.Packaging;

// ADR 0016: every package ships under Apache-2.0 by ZedLabs from the ZedLabs repository, with LICENSE and NOTICE packed.
// Packs the already-built projects (--no-build, the test assembly's configuration): no restore, no network.
[Trait("Category", "Unit")]
public class Given_the_packed_packages
{
    const string RepositoryUrl = "https://github.com/ZedLabs-dev/MessageQueue";

    public static TheoryData<string> Packages() => ["ZedLabs.MessageQueue", "ZedLabs.MessageQueue.RabbitMQ", "ZedLabs.MessageQueue.AzureServiceBus"];

    static string Configuration =>
        typeof(Given_the_packed_packages).Assembly.GetCustomAttribute<AssemblyConfigurationAttribute>()?.Configuration ?? "Debug";

    static async Task<ZipArchive> PackAsync(string package, string output, CancellationToken cancellationToken)
    {
        var start = new ProcessStartInfo("dotnet")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            WorkingDirectory = RepositoryFiles.Path("."),
        };
        foreach (var argument in new[]
                 {
                     "pack", RepositoryFiles.Path($"src/{package}/{package}.csproj"), "--no-build", "--no-restore",
                     "--configuration", Configuration, "--output", output, "--disable-build-servers", "-nologo",
                 })
        {
            start.ArgumentList.Add(argument);
        }

        using var process = Process.Start(start)!;
        var stdout = process.StandardOutput.ReadToEndAsync(cancellationToken);
        var stderr = process.StandardError.ReadToEndAsync(cancellationToken);
        await process.WaitForExitAsync(cancellationToken);
        process.ExitCode.Should().Be(0, $"it should pack {package}: {await stdout}{await stderr}");

        var nupkg = Directory.GetFiles(output, $"{package}.*.nupkg").Single(f => !f.EndsWith(".snupkg", StringComparison.Ordinal));
        return ZipFile.OpenRead(nupkg);
    }

    [Theory(Timeout = 120_000)]
    [MemberData(nameof(Packages))]
    public async Task When_Packing_The_Package(string package)
    {
        var output = Directory.CreateTempSubdirectory("zedlabs-mq-pack-").FullName;
        try
        {
            using var archive = await PackAsync(package, output, TestContext.Current.CancellationToken);
            var entry = archive.Entries.Single(e => e.FullName == $"{package}.nuspec");
            XDocument nuspec;
            using (var stream = entry.Open())
                nuspec = XDocument.Load(stream);
            var metadata = nuspec.Root!.Elements().Single(e => e.Name.LocalName == "metadata");
            string? Value(string name) => metadata.Elements().SingleOrDefault(e => e.Name.LocalName == name)?.Value;
            var license = metadata.Elements().Single(e => e.Name.LocalName == "license");
            var repository = metadata.Elements().Single(e => e.Name.LocalName == "repository");

            ((string?)license.Attribute("type")).Should().Be("expression", "it should declare the license as an SPDX expression");
            license.Value.Should().Be("Apache-2.0", "it should license the package under Apache-2.0");
            Value("authors").Should().Be("ZedLabs", "it should name ZedLabs as the author");
            Value("copyright").Should().Be("Copyright 2026 ZedLabs", "it should name ZedLabs as the copyright holder");
            Value("projectUrl").Should().Be(RepositoryUrl, "it should link the ZedLabs repository as the project page");
            ((string?)repository.Attribute("url")).Should().Be(RepositoryUrl, "it should point the repository metadata at ZedLabs-dev/MessageQueue");
            archive.Entries.Select(e => e.FullName).Should().Contain(["LICENSE", "NOTICE"], "it should pack the license and notice files");
        }
        finally
        {
            Directory.Delete(output, recursive: true);
        }
    }
}
