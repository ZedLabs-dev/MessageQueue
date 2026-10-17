using System.Net.Security;

namespace ESE.MessageQueue.RabbitMQ;

public sealed record RabbitMqTlsConfig
{
    public bool Enabled { get; set; }

    public string ServerName { get; set; } = string.Empty;

    public SslPolicyErrors AcceptablePolicyErrors { get; set; } = SslPolicyErrors.None;

    public bool AllowInsecureCertificates { get; set; }
}
