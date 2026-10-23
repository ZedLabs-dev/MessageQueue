using System.Security.Cryptography.X509Certificates;
using AwesomeAssertions;
using ESE.MessageQueue.RabbitMQ;
using RabbitMQ.Client;

namespace ESE.MessageQueue.Tests.RabbitMQ;

// Mutual TLS from the OS store (ADR 0022): the generated certificate is added to CurrentUser/My and removed again.
// Linux only: there the store is a folder under the user's profile, while adding to it on macOS (the login keychain)
// or Windows can prompt or persist beyond the run. CI runs on Linux, so these run there.
[Trait("Category", "Unit")]
public sealed class Given_a_rabbitmq_client_certificate_in_the_store : IDisposable
{
    readonly TestCertificates certificates = new();
    readonly List<X509Certificate2> added = [];

    public void Dispose()
    {
        if (added.Count > 0)
        {
            using var store = new X509Store(StoreName.My, StoreLocation.CurrentUser);
            store.Open(OpenFlags.ReadWrite);
            foreach (var certificate in added)
            {
                store.Remove(certificate);
                certificate.Dispose();
            }
        }
        certificates.Dispose();
    }

    void AddToStore(X509Certificate2 certificate)
    {
        using var store = new X509Store(StoreName.My, StoreLocation.CurrentUser);
        store.Open(OpenFlags.ReadWrite);
        store.Add(certificate);
        added.Add(certificate);
    }

    RabbitMqConfig Config() => new()
    {
        ConnectionString = "amqps://user:pw@rabbit.internal/",
        QueueName = "orders",
        // Lower case with colons, as some tools print it.
        Tls = new RabbitMqTlsConfig { ClientCertificateThumbprint = string.Join(':', certificates.Thumbprint.ToLowerInvariant().Chunk(2).Select(c => new string(c))) }
    };

    [Fact]
    public void When_The_Thumbprint_Names_A_Certificate_With_Its_Key()
    {
        Assert.SkipUnless(OperatingSystem.IsLinux(), "adds to CurrentUser/My, which only Linux keeps as plain files");
        AddToStore(X509CertificateLoader.LoadPkcs12FromFile(certificates.PfxPath, TestCertificates.Password, X509KeyStorageFlags.Exportable));
        var config = Config();

        new RabbitMqConfigValidator().Validate(null, config).Succeeded.Should().BeTrue("it should find the certificate by its thumbprint");
        using var queue = new DisposableFactory(RabbitMqConnections.CreateFactory(config));
        var certificate = queue.Factory.Ssl.Certs!.Cast<X509Certificate2>().Should().ContainSingle("it should present the store's certificate").Subject;
        certificate.Thumbprint.Should().Be(certificates.Thumbprint, "it should present the certificate the thumbprint names");
        certificate.HasPrivateKey.Should().BeTrue("it should load the private key needed to authenticate");
    }

    [Fact]
    public void When_The_Store_Holds_The_Certificate_Without_Its_Key()
    {
        Assert.SkipUnless(OperatingSystem.IsLinux(), "adds to CurrentUser/My, which only Linux keeps as plain files");
        AddToStore(X509CertificateLoader.LoadCertificateFromFile(certificates.CertificatePemPath));

        new RabbitMqConfigValidator().Validate(null, Config()).FailureMessage
            .Should().Contain("was not found with a private key", "it should not present a certificate it cannot authenticate with");
    }

    sealed class DisposableFactory : IDisposable
    {
        public DisposableFactory(ConnectionFactory factory)
        {
            Factory = factory;
        }

        public ConnectionFactory Factory { get; }

        public void Dispose()
        {
            foreach (var certificate in Factory.Ssl.Certs?.Cast<X509Certificate2>() ?? [])
                certificate.Dispose();
        }
    }
}
