using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using ESE.MessageQueue.Exceptions;

namespace ESE.MessageQueue.RabbitMQ;

// Tls:CaCertificatePath (ADR 0022): the CA certificates (PEM, one or more, or a single DER certificate) the broker's
// certificate must chain to, instead of the machine's trust store. Only the configured CAs are trusted; the server name
// is still checked, revocation is not (the SslStream client default), and AcceptablePolicyErrors still decides what may
// be overlooked, because a validation callback replaces RabbitMQ.Client's own AcceptablePolicyErrors check.
static class RabbitMqTrustAnchors
{
    const string Setting = "Tls:CaCertificatePath";

    static readonly Oid ServerAuthentication = new("1.3.6.1.5.5.7.3.1");

    public static bool IsConfigured(RabbitMqTlsConfig tls) => !string.IsNullOrWhiteSpace(tls.CaCertificatePath);

    public static void Validate(RabbitMqTlsConfig tls, bool usesTls, ICollection<string> errors, bool load = true)
    {
        if (!IsConfigured(tls))
            return;
        if (!usesTls)
        {
            errors.Add($"{Setting} needs TLS: use an amqps:// ConnectionString (or Tls:Enabled with HostName)");
            return;
        }

        if (!load)
            return;
        try
        {
            Dispose(Load(tls));
        }
        catch (ConfigurationException error)
        {
            errors.Add(error.Message);
        }
    }

    // New certificates; the caller owns (and disposes) them.
    public static X509Certificate2Collection Load(RabbitMqTlsConfig tls)
    {
        var path = tls.CaCertificatePath;
        var content = RabbitMqClientCertificates.Read(path, Setting);
        var anchors = new X509Certificate2Collection();
        try
        {
            if (RabbitMqClientCertificates.IsPem(content))
                anchors.ImportFromPem(Encoding.UTF8.GetString(content));
            else
                anchors.Add(X509CertificateLoader.LoadCertificate(content));
        }
        catch (CryptographicException error)
        {
            Dispose(anchors);
            throw new ConfigurationException($"{Setting} '{path}' is not a PEM or DER certificate ({error.GetType().Name})");
        }

        return anchors.Count > 0 ? anchors : throw new ConfigurationException($"{Setting} '{path}' holds no certificate");
    }

    public static RemoteCertificateValidationCallback Callback(X509Certificate2Collection anchors, SslPolicyErrors acceptable) =>
        (_, certificate, chain, errors) => (Evaluate(certificate, chain, errors, anchors) & ~acceptable) == SslPolicyErrors.None;

    // The policy errors left once the chain is judged against the anchors alone: a missing certificate and a name
    // mismatch stay; chain errors are recomputed.
    static SslPolicyErrors Evaluate(X509Certificate? certificate, X509Chain? presented, SslPolicyErrors errors, X509Certificate2Collection anchors)
    {
        if (certificate is null)
            return errors;

        errors &= ~SslPolicyErrors.RemoteCertificateChainErrors;
        using var chain = new X509Chain();
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.AddRange(anchors);
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        chain.ChainPolicy.ApplicationPolicy.Add(ServerAuthentication);
        // Only what the broker sent and the anchors: never fetch an issuer from a URL the certificate names (AIA).
        chain.ChainPolicy.DisableCertificateDownloads = true;
        // Intermediate certificates the broker sent along, as candidates only, never as anchors.
        if (presented is not null)
            chain.ChainPolicy.ExtraStore.AddRange(presented.ChainPolicy.ExtraStore);

        var leaf = certificate as X509Certificate2;
        using var copy = leaf is null ? X509CertificateLoader.LoadCertificate(certificate.GetRawCertData()) : null;
        try
        {
            if (!chain.Build(leaf ?? copy!))
                errors |= SslPolicyErrors.RemoteCertificateChainErrors;
            return errors;
        }
        finally
        {
            DisposeElements(chain, anchors, leaf);
        }
    }

    // The chain hands out its own certificate instances per element; dispose those, never one the caller, the anchors
    // or the broker's ExtraStore own (the same instance can come back on some platforms).
    static void DisposeElements(X509Chain chain, X509Certificate2Collection anchors, X509Certificate2? leaf)
    {
        foreach (var element in chain.ChainElements)
        {
            var issued = element.Certificate;
            if (!ReferenceEquals(issued, leaf) && !Contains(anchors, issued) && !Contains(chain.ChainPolicy.ExtraStore, issued))
                issued.Dispose();
        }
    }

    static bool Contains(X509Certificate2Collection certificates, X509Certificate2 certificate)
    {
        foreach (var each in certificates)
        {
            if (ReferenceEquals(each, certificate))
                return true;
        }
        return false;
    }

    public static void Dispose(X509Certificate2Collection anchors)
    {
        foreach (var anchor in anchors)
            anchor.Dispose();
    }
}
