using System.Buffers;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using ZedLabs.MessageQueue.Exceptions;

namespace ZedLabs.MessageQueue.RabbitMQ;

// Mutual TLS (ADR 0022): the optional client certificate, from a PKCS#12 (PFX) or PEM file or from the OS certificate
// store by thumbprint. Startup validation loads it once (and disposes it), so a missing file, a wrong password or a
// certificate without its private key fails the host instead of the first connection; each queue then loads it once
// for its connections and disposes it with the queue. Messages name the setting and the path, never the password or
// the loader's exception text. File contents, decoded PEM text and the PKCS#12 round trip are zeroed after use.
static class RabbitMqClientCertificates
{
    const string PathSetting = "Tls:ClientCertificatePath";
    const string KeyPathSetting = "Tls:ClientCertificateKeyPath";
    const string PasswordSetting = "Tls:ClientCertificatePassword";
    const string ThumbprintSetting = "Tls:ClientCertificateThumbprint";

    public static bool IsConfigured(RabbitMqTlsConfig tls) => HasFile(tls) || HasThumbprint(tls);

    public static void Validate(RabbitMqTlsConfig tls, bool usesTls, ICollection<string> errors, bool load = true)
    {
        var count = errors.Count;
        if (HasFile(tls) && HasThumbprint(tls))
            errors.Add($"Set either {PathSetting} or {ThumbprintSetting}, not both");
        if (!HasFile(tls) && tls.ClientCertificatePassword.Length > 0)
            errors.Add($"{PasswordSetting} applies only with {PathSetting}");
        if (!HasFile(tls) && !string.IsNullOrWhiteSpace(tls.ClientCertificateKeyPath))
            errors.Add($"{KeyPathSetting} applies only with a PEM {PathSetting}");
        if (!Enum.IsDefined(tls.ClientCertificateStoreLocation))
            errors.Add($"Tls:ClientCertificateStoreLocation '{tls.ClientCertificateStoreLocation}' is not a store location");
        if (!Enum.IsDefined(tls.ClientCertificateStoreName))
            errors.Add($"Tls:ClientCertificateStoreName '{tls.ClientCertificateStoreName}' is not a store name");
        if (HasThumbprint(tls) && Thumbprint(tls) is null)
            errors.Add($"{ThumbprintSetting} must be a SHA-1 thumbprint: 40 hexadecimal characters (whitespace, ':', '-' and direction marks are ignored)");
        if (IsConfigured(tls) && !usesTls)
            errors.Add($"A client certificate ({PathSetting} or {ThumbprintSetting}) needs TLS: use an amqps:// ConnectionString (or Tls:Enabled with HostName)");

        if (!load || errors.Count > count || !IsConfigured(tls))
            return;
        try
        {
            using var certificate = Load(tls);
        }
        catch (ConfigurationException error)
        {
            errors.Add(error.Message);
        }
    }

    // A new certificate with its private key; the caller owns it.
    public static X509Certificate2 Load(RabbitMqTlsConfig tls) => HasFile(tls) ? FromFile(tls) : FromStore(tls);

    static X509Certificate2 FromFile(RabbitMqTlsConfig tls)
    {
        var path = tls.ClientCertificatePath;
        var content = Read(path, PathSetting);
        X509Certificate2 certificate;
        try
        {
            certificate = IsPem(content) ? FromPem(tls, content) : X509CertificateLoader.LoadPkcs12(content, Password(tls));
        }
        catch (CryptographicException error)
        {
            throw new ConfigurationException(
                $"{PathSetting} '{path}' could not be loaded with a private key ({error.GetType().Name}): check the file, {KeyPathSetting} and {PasswordSetting}");
        }
        finally
        {
            CryptographicOperations.ZeroMemory(content);
        }

        return WithPrivateKey(certificate, $"{PathSetting} '{path}'");
    }

    static X509Certificate2 FromPem(RabbitMqTlsConfig tls, byte[] certificateContent)
    {
        byte[]? keyContent = null;
        if (!string.IsNullOrWhiteSpace(tls.ClientCertificateKeyPath))
            keyContent = Read(tls.ClientCertificateKeyPath, KeyPathSetting);

        var certificatePem = Decode(certificateContent);
        var keyPem = keyContent is null ? certificatePem : Decode(keyContent);
        try
        {
            using var pem = tls.ClientCertificatePassword.Length > 0
                ? X509Certificate2.CreateFromEncryptedPem(certificatePem.Span, keyPem.Span, tls.ClientCertificatePassword)
                : X509Certificate2.CreateFromPem(certificatePem.Span, keyPem.Span);
            // A PEM key is ephemeral, which SslStream on Windows cannot use; a PKCS#12 round trip gives every
            // platform a certificate it can present.
            var pkcs12 = pem.Export(X509ContentType.Pkcs12);
            try
            {
                return X509CertificateLoader.LoadPkcs12(pkcs12, null);
            }
            finally
            {
                CryptographicOperations.ZeroMemory(pkcs12);
            }
        }
        finally
        {
            certificatePem.Clear();
            if (keyContent is not null)
            {
                keyPem.Clear();
                CryptographicOperations.ZeroMemory(keyContent);
            }
        }
    }

    static X509Certificate2 FromStore(RabbitMqTlsConfig tls)
    {
        var thumbprint = Thumbprint(tls)!;
        var where = $"{tls.ClientCertificateStoreLocation}/{tls.ClientCertificateStoreName}";
        X509Certificate2Collection all;
        try
        {
            using var store = new X509Store(tls.ClientCertificateStoreName, tls.ClientCertificateStoreLocation);
            store.Open(OpenFlags.ReadOnly | OpenFlags.OpenExistingOnly);
            all = store.Certificates;
        }
        catch (CryptographicException error)
        {
            throw new ConfigurationException($"{ThumbprintSetting} {thumbprint}: the store {where} could not be opened ({error.GetType().Name})");
        }

        // Every certificate the store handed out is disposed except the one returned (Find may return the same
        // instances or copies).
        var found = all.Find(X509FindType.FindByThumbprint, thumbprint, validOnly: false);
        var certificate = found.FirstOrDefault(c => c.HasPrivateKey);
        foreach (var other in all.Concat(found).Where(c => !ReferenceEquals(c, certificate)))
            other.Dispose();
        return certificate ?? throw new ConfigurationException(
            $"{ThumbprintSetting} {thumbprint} was not found with a private key in the store {where}");
    }

    static X509Certificate2 WithPrivateKey(X509Certificate2 certificate, string source)
    {
        if (certificate.HasPrivateKey)
            return certificate;
        certificate.Dispose();
        throw new ConfigurationException($"{source} has no private key, so it cannot authenticate the client");
    }

    public static byte[] Read(string path, string setting)
    {
        try
        {
            return File.ReadAllBytes(path);
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException or ArgumentException or NotSupportedException)
        {
            throw new ConfigurationException($"{setting} '{path}' does not exist or cannot be read ({error.GetType().Name})");
        }
    }

    // PEM text in a pooled buffer the caller clears; a UTF-8 byte order mark is dropped.
    static PemText Decode(byte[] content)
    {
        var bytes = content.AsSpan();
        if (bytes.StartsWith(Utf8Bom))
            bytes = bytes[Utf8Bom.Length..];
        var chars = ArrayPool<char>.Shared.Rent(Encoding.UTF8.GetMaxCharCount(bytes.Length));
        return new PemText(chars, Encoding.UTF8.GetChars(bytes, chars));
    }

    sealed class PemText
    {
        readonly char[] buffer;
        readonly int length;

        public PemText(char[] buffer, int length)
        {
            this.buffer = buffer;
            this.length = length;
        }

        public ReadOnlySpan<char> Span => buffer.AsSpan(0, length);

        public void Clear()
        {
            buffer.AsSpan().Clear();
            ArrayPool<char>.Shared.Return(buffer);
        }
    }

    static ReadOnlySpan<byte> Utf8Bom => "\uFEFF"u8;

    // PEM is text starting with a "-----BEGIN" line (after optional whitespace or a UTF-8 byte order mark); anything
    // else is read as PKCS#12.
    public static bool IsPem(ReadOnlySpan<byte> content)
    {
        if (content.StartsWith(Utf8Bom))
            content = content[Utf8Bom.Length..];
        return content.TrimStart(" \t\r\n"u8).StartsWith("-----BEGIN"u8);
    }

    static string? Password(RabbitMqTlsConfig tls) => tls.ClientCertificatePassword.Length > 0 ? tls.ClientCertificatePassword : null;

    // Upper-case hex, or null when it is not a SHA-1 thumbprint. Whitespace, ':' and '-' separators and the
    // left-to-right / right-to-left marks that certificate dialogs copy along are ignored.
    static string? Thumbprint(RabbitMqTlsConfig tls)
    {
        var hex = string.Concat(tls.ClientCertificateThumbprint.Where(c => !char.IsWhiteSpace(c) && c is not (':' or '-' or '\u200E' or '\u200F')))
            .ToUpperInvariant();
        return hex.Length == 40 && hex.All(char.IsAsciiHexDigit) ? hex : null;
    }

    static bool HasFile(RabbitMqTlsConfig tls) => !string.IsNullOrWhiteSpace(tls.ClientCertificatePath);

    static bool HasThumbprint(RabbitMqTlsConfig tls) => !string.IsNullOrWhiteSpace(tls.ClientCertificateThumbprint);
}
