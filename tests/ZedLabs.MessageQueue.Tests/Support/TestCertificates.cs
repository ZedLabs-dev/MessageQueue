using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace ZedLabs.MessageQueue.Tests;

// A self-signed client certificate written to a fresh temp folder as PFX and PEM files; deleted on Dispose.
// Nothing is added to an OS certificate store.
public sealed class TestCertificates : IDisposable
{
    public const string Password = "pfx-S3cret!";

    public TestCertificates()
    {
        Folder = Directory.CreateTempSubdirectory("zedlabs-mq-certs-").FullName;
        using var key = RSA.Create(2048);
        var request = new CertificateRequest("CN=zedlabs-messagequeue-test-client", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension([new Oid("1.3.6.1.5.5.7.3.2")], critical: false));
        using var certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddDays(1));
        Thumbprint = certificate.Thumbprint;

        PfxPath = Write("client.pfx", certificate.Export(X509ContentType.Pkcs12, Password));
        using (var publicOnly = X509CertificateLoader.LoadCertificate(certificate.RawData))
            CertificateOnlyPfxPath = Write("public-only.pfx", publicOnly.Export(X509ContentType.Pkcs12));
        var certificatePem = certificate.ExportCertificatePem();
        var keyPem = key.ExportPkcs8PrivateKeyPem();
        CertificatePemPath = Write("client.crt", certificatePem);
        KeyPemPath = Write("client.key", keyPem);
        EncryptedKeyPemPath = Write("client-encrypted.key", key.ExportEncryptedPkcs8PrivateKeyPem(Password,
            new PbeParameters(PbeEncryptionAlgorithm.Aes256Cbc, HashAlgorithmName.SHA256, 100_000)));
        CombinedPemPath = Write("client.pem", certificatePem + "\n" + keyPem);
        BomPemPath = Write("client-bom.pem", [.. "\uFEFF"u8, .. System.Text.Encoding.ASCII.GetBytes(certificatePem + "\n" + keyPem)]);
    }

    public string Folder { get; }
    public string Thumbprint { get; }
    public string PfxPath { get; }
    public string CertificateOnlyPfxPath { get; }
    public string CertificatePemPath { get; }
    public string KeyPemPath { get; }
    public string EncryptedKeyPemPath { get; }
    public string CombinedPemPath { get; }
    public string BomPemPath { get; }
    public string MissingPath => Path.Combine(Folder, "missing.pfx");

    string Write(string name, byte[] content)
    {
        var path = Path.Combine(Folder, name);
        File.WriteAllBytes(path, content);
        return path;
    }

    string Write(string name, string content)
    {
        var path = Path.Combine(Folder, name);
        File.WriteAllText(path, content);
        return path;
    }

    public void Dispose() => Directory.Delete(Folder, recursive: true);
}
