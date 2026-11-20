namespace ZedLabs.MessageQueue.Tests;

// Reads the RabbitMQ image from the FROM line of docker/test-broker/Dockerfile, copied to the output folder.
static class TestBrokerImage
{
    public static string Read()
    {
        var path = Path.Combine(AppContext.BaseDirectory, "test-broker.Dockerfile");
        return File.ReadLines(path)
            .Select(line => line.Trim())
            .FirstOrDefault(line => line.StartsWith("FROM ", StringComparison.OrdinalIgnoreCase))?[5..].Trim()
            ?? throw new InvalidOperationException($"No FROM line in {path}");
    }
}
