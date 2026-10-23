using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace ESE.MessageQueue.Tests;

// A throwaway certificate authority for mutual TLS: a CA, a server certificate for localhost / 127.0.0.1 / ::1 and a
// client certificate, written as PEM (and the client also as PFX) to a fresh temp folder that Dispose deletes. Nothing
// is added to an OS certificate store and no key is ever committed: everything is generated per run.
public sealed class TestCertificateAuthority : IDisposable
{
    public const string Password = "ca-pfx-S3cret!";
    public const string ServerName = "localhost";

    static readonly Oid ServerAuthentication = new("1.3.6.1.5.5.7.3.1");
    static readonly Oid ClientAuthentication = new("1.3.6.1.5.5.7.3.2");

    readonly RSA caKey;

    public TestCertificateAuthority(string name = "ese-messagequeue-test-ca")
    {
        Folder = Directory.CreateTempSubdirectory("ese-mq-ca-").FullName;
        caKey = RSA.Create(2048);
        var request = new CertificateRequest($"CN={name}", caKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
        request.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(request.PublicKey, false));
        Certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddHours(-1), DateTimeOffset.UtcNow.AddDays(2));
        CaPemPath = Write("ca.crt", Certificate.ExportCertificatePem());
        CaDerPath = Write("ca.der", Certificate.RawData);

        var (serverCertificate, serverKey) = Issue("CN=localhost", ServerAuthentication, san =>
        {
            san.AddDnsName(ServerName);
            san.AddIpAddress(IPAddress.Loopback);
            san.AddIpAddress(IPAddress.IPv6Loopback);
        });
        using (serverCertificate)
        using (serverKey)
        {
            Server = X509CertificateLoader.LoadCertificate(serverCertificate.RawData);
            ServerCertificatePemPath = Write("server.crt", serverCertificate.ExportCertificatePem());
            ServerKeyPemPath = Write("server.key", serverKey.ExportPkcs8PrivateKeyPem());
        }

        var (clientCertificate, clientKey) = Issue("CN=ese-messagequeue-test-client", ClientAuthentication, san => san.AddDnsName("client"));
        using (clientCertificate)
        using (clientKey)
        {
            ClientThumbprint = clientCertificate.Thumbprint;
            ClientCertificatePemPath = Write("client.crt", clientCertificate.ExportCertificatePem());
            ClientKeyPemPath = Write("client.key", clientKey.ExportPkcs8PrivateKeyPem());
            ClientPfxPath = Write("client.pfx", clientCertificate.CopyWithPrivateKey(clientKey).Export(X509ContentType.Pkcs12, Password));
        }
    }

    public string Folder { get; }
    public X509Certificate2 Certificate { get; }
    public X509Certificate2 Server { get; }
    public string CaPemPath { get; }
    public string CaDerPath { get; }
    public string ServerCertificatePemPath { get; }
    public string ServerKeyPemPath { get; }
    public string ClientCertificatePemPath { get; }
    public string ClientKeyPemPath { get; }
    public string ClientPfxPath { get; }
    public string ClientThumbprint { get; }

    // A further certificate signed by this CA, e.g. a server certificate with the wrong usage or an expired one.
    public X509Certificate2 IssueServer(Oid usage, DateTimeOffset? notAfter = null)
    {
        var (certificate, key) = Issue("CN=localhost", usage, san => san.AddDnsName(ServerName), notAfter);
        key.Dispose();
        return certificate;
    }

    // An intermediate CA signed by this CA and a server certificate signed by the intermediate.
    public (X509Certificate2 Intermediate, X509Certificate2 Server) IssueThroughIntermediate()
    {
        using var intermediateKey = RSA.Create(2048);
        var request = new CertificateRequest("CN=ese-messagequeue-test-intermediate", intermediateKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
        request.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(request.PublicKey, false));
        request.CertificateExtensions.Add(X509AuthorityKeyIdentifierExtension.CreateFromCertificate(Certificate, true, false));
        using var signed = request.Create(Certificate, DateTimeOffset.UtcNow.AddMinutes(-30), DateTimeOffset.UtcNow.AddDays(1), RandomNumberGenerator.GetBytes(8));
        using var intermediate = signed.CopyWithPrivateKey(intermediateKey);

        using var serverKey = RSA.Create(2048);
        var server = new CertificateRequest("CN=localhost", serverKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        server.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension([ServerAuthentication], false));
        server.CertificateExtensions.Add(X509AuthorityKeyIdentifierExtension.CreateFromCertificate(intermediate, true, false));
        var leaf = server.Create(intermediate, DateTimeOffset.UtcNow.AddMinutes(-20), DateTimeOffset.UtcNow.AddHours(12), RandomNumberGenerator.GetBytes(8));
        return (X509CertificateLoader.LoadCertificate(intermediate.RawData), leaf);
    }

    (X509Certificate2 Certificate, RSA Key) Issue(string subject, Oid usage, Action<SubjectAlternativeNameBuilder> names, DateTimeOffset? notAfter = null)
    {
        var key = RSA.Create(2048);
        var request = new CertificateRequest(subject, key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension([usage], false));
        request.CertificateExtensions.Add(X509AuthorityKeyIdentifierExtension.CreateFromCertificate(Certificate, true, false));
        var san = new SubjectAlternativeNameBuilder();
        names(san);
        request.CertificateExtensions.Add(san.Build());
        var serial = RandomNumberGenerator.GetBytes(16);
        serial[0] &= 0x7F;
        var notBefore = DateTimeOffset.UtcNow.AddMinutes(-30);
        var certificate = request.Create(Certificate, notBefore, notAfter ?? DateTimeOffset.UtcNow.AddDays(1), serial);
        return (certificate, key);
    }

    public string Write(string name, string content)
    {
        var path = Path.Combine(Folder, name);
        File.WriteAllText(path, content);
        return path;
    }

    string Write(string name, byte[] content)
    {
        var path = Path.Combine(Folder, name);
        File.WriteAllBytes(path, content);
        return path;
    }

    public void Dispose()
    {
        Server.Dispose();
        Certificate.Dispose();
        caKey.Dispose();
        Directory.Delete(Folder, recursive: true);
    }
}
