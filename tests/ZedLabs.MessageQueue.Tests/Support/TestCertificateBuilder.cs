using System.Formats.Asn1;
using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace ZedLabs.MessageQueue.Tests;

// Hand-built certificates for the trust-anchor specs: CAs with a chosen name, key identifier and path length, and
// leaves with a chosen issuer, usages, names and AIA location, including the hostile ones (a leaf signing a leaf, a
// rogue root named like the anchor). Every key is generated in memory; nothing is written or stored.
static class TestCertificateBuilder
{
    public static readonly Oid ServerAuthentication = new("1.3.6.1.5.5.7.3.1");
    public static readonly Oid ClientAuthentication = new("1.3.6.1.5.5.7.3.2");
    public static readonly Oid AnyExtendedKeyUsage = new("2.5.29.37.0");

    public static byte[] SubjectKeyIdentifier(X509Certificate2 certificate) =>
        certificate.Extensions.OfType<X509SubjectKeyIdentifierExtension>().Single().SubjectKeyIdentifierBytes.ToArray();

    // A CA certificate with its private key: self-signed when issuer is null.
    public static X509Certificate2 Ca(string name, X509Certificate2? issuer = null, int? pathLength = null, byte[]? subjectKeyIdentifier = null,
        X509Extension? nameConstraints = null)
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest(name, key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, pathLength.HasValue, pathLength ?? 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
        request.CertificateExtensions.Add(subjectKeyIdentifier is null
            ? new X509SubjectKeyIdentifierExtension(request.PublicKey, false)
            : new X509SubjectKeyIdentifierExtension(subjectKeyIdentifier, false));
        if (nameConstraints is not null)
            request.CertificateExtensions.Add(nameConstraints);
        if (issuer is null)
            return request.CreateSelfSigned(DateTimeOffset.UtcNow.AddHours(-2), DateTimeOffset.UtcNow.AddDays(2));
        request.CertificateExtensions.Add(X509AuthorityKeyIdentifierExtension.CreateFromCertificate(issuer, true, false));
        // Within the issuer's validity, which an issuer built a moment earlier ends a moment earlier.
        var notAfter = new DateTimeOffset(issuer.NotAfter).AddHours(-1);
        using var signed = request.Create(issuer, DateTimeOffset.UtcNow.AddMinutes(-50), notAfter, RandomNumberGenerator.GetBytes(8));
        return signed.CopyWithPrivateKey(key);
    }

    // A leaf with its private key, signed by issuer's key whatever issuer is (a CA or, for forgeries, another leaf).
    // isCa null leaves out basic constraints; usages null leaves out the extended key usage.
    public static X509Certificate2 Leaf(X509Certificate2 issuer, Oid[]? usages, string[]? names = null, bool? isCa = false,
        string? authorityInformationAccess = null, byte[]? authorityKeyIdentifier = null)
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest("CN=localhost", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        if (isCa is not null)
            request.CertificateExtensions.Add(new X509BasicConstraintsExtension(isCa.Value, false, 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
        if (usages is not null)
            request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension([.. usages], false));
        var san = new SubjectAlternativeNameBuilder();
        foreach (var name in names ?? ["localhost"])
            san.AddDnsName(name);
        san.AddIpAddress(IPAddress.Loopback);
        request.CertificateExtensions.Add(san.Build());
        request.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(request.PublicKey, false));
        if (authorityKeyIdentifier is not null)
            request.CertificateExtensions.Add(X509AuthorityKeyIdentifierExtension.CreateFromSubjectKeyIdentifier(authorityKeyIdentifier));
        else if (issuer.Extensions.OfType<X509SubjectKeyIdentifierExtension>().Any())
            request.CertificateExtensions.Add(X509AuthorityKeyIdentifierExtension.CreateFromCertificate(issuer, true, false));
        if (authorityInformationAccess is not null)
            request.CertificateExtensions.Add(new X509AuthorityInformationAccessExtension(null, [authorityInformationAccess]));
        using var issuerKey = issuer.GetRSAPrivateKey()!;
        using var signed = request.Create(issuer.SubjectName, X509SignatureGenerator.CreateForRSA(issuerKey, RSASignaturePadding.Pkcs1),
            DateTimeOffset.UtcNow.AddMinutes(-30), DateTimeOffset.UtcNow.AddHours(12), RandomNumberGenerator.GetBytes(8));
        return signed.CopyWithPrivateKey(key);
    }

    // A critical name-constraints extension that permits one DNS subtree.
    public static X509Extension PermittedDnsSubtree(string dnsName)
    {
        var writer = new AsnWriter(AsnEncodingRules.DER);
        using (writer.PushSequence())
        using (writer.PushSequence(new Asn1Tag(TagClass.ContextSpecific, 0, true)))
        using (writer.PushSequence())
            writer.WriteCharacterString(UniversalTagNumber.IA5String, dnsName, new Asn1Tag(TagClass.ContextSpecific, 2));
        return new X509Extension("2.5.29.30", writer.Encode(), true);
    }
}
