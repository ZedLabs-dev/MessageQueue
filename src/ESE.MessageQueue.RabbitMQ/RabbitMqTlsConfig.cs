using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using ESE.MessageQueue.Configuration;

namespace ESE.MessageQueue.RabbitMQ;

public sealed record RabbitMqTlsConfig
{
    public bool Enabled { get; set; }

    public string ServerName { get; set; } = string.Empty;

    public SslPolicyErrors AcceptablePolicyErrors { get; set; } = SslPolicyErrors.None;

    public bool AllowInsecureCertificates { get; set; }

    public string CaCertificatePath { get; set; } = string.Empty;

    public string ClientCertificatePath { get; set; } = string.Empty;

    public string ClientCertificateKeyPath { get; set; } = string.Empty;

    public string ClientCertificatePassword { get; set; } = string.Empty;

    public string ClientCertificateThumbprint { get; set; } = string.Empty;

    public StoreName ClientCertificateStoreName { get; set; } = StoreName.My;

    public StoreLocation ClientCertificateStoreLocation { get; set; } = StoreLocation.CurrentUser;

    bool PrintMembers(StringBuilder builder) =>
        ConfigValidation.PrintMembers(this, builder, nameof(ClientCertificatePassword));
}
