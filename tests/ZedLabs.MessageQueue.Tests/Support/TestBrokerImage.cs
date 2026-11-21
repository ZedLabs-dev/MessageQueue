namespace ZedLabs.MessageQueue.Tests;

// Reads test images from FROM lines of the Dockerfiles in docker/test-broker and docker/test-servicebus, copied to the
// output folder, so Dependabot can bump them.
static class TestBrokerImage
{
    // The RabbitMQ image: the first FROM line of docker/test-broker/Dockerfile.
    public static string Read() => FromLines("test-broker.Dockerfile").First().Image;

    // The image of stage `stage` (FROM image AS stage) in docker/test-servicebus/Dockerfile.
    public static string ReadServiceBus(string stage) =>
        FromLines("test-servicebus.Dockerfile").FirstOrDefault(from => from.Stage == stage).Image
        ?? throw new InvalidOperationException($"No FROM line for stage '{stage}' in test-servicebus.Dockerfile");

    static List<(string Image, string? Stage)> FromLines(string fileName)
    {
        var path = Path.Combine(AppContext.BaseDirectory, fileName);
        var lines = File.ReadLines(path)
            .Select(line => line.Trim())
            .Where(line => line.StartsWith("FROM ", StringComparison.OrdinalIgnoreCase))
            .Select(line => line[5..].Split(' ', StringSplitOptions.RemoveEmptyEntries))
            .Select(parts => (parts[0], parts.Length == 3 && parts[1].Equals("AS", StringComparison.OrdinalIgnoreCase) ? parts[2] : null))
            .ToList();
        return lines.Count > 0 ? lines : throw new InvalidOperationException($"No FROM line in {path}");
    }
}
