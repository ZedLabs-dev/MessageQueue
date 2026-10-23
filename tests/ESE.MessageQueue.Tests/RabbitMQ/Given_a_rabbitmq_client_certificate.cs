using System.Security.Cryptography.X509Certificates;
using AwesomeAssertions;
using ESE.MessageQueue.RabbitMQ;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace ESE.MessageQueue.Tests.RabbitMQ;

// Mutual TLS (ADR 0008): a client certificate from a PFX or PEM file, or from the OS store by thumbprint, validated at
// startup and handed to the connection factory. Nothing here connects; the certificate is generated per test class.
[Trait("Category", "Unit")]
public sealed class Given_a_rabbitmq_client_certificate : IDisposable
{
    readonly TestCertificates certificates = new();

    public void Dispose() => certificates.Dispose();

    static RabbitMqConfig Config(Action<RabbitMqTlsConfig> tls)
    {
        var config = new RabbitMqConfig { ConnectionString = "amqps://user:pw@rabbit.internal/", QueueName = "orders" };
        tls(config.Tls);
        return config;
    }

    static ValidateOptionsResult Validate(RabbitMqConfig config) => new RabbitMqConfigValidator().Validate(null, config);

    void ShouldCarryTheCertificate(ConnectionFactory factory, string because)
    {
        factory.Ssl.Certs.Should().NotBeNull(because);
        var certificate = factory.Ssl.Certs!.Cast<X509Certificate>().Should().ContainSingle(because).Subject
            .Should().BeAssignableTo<X509Certificate2>(because).Subject;
        certificate.Thumbprint.Should().Be(certificates.Thumbprint, because);
        certificate.HasPrivateKey.Should().BeTrue("it should load the private key needed to authenticate");
    }

    [Fact]
    public void When_A_Pfx_File_Is_Configured()
    {
        var config = Config(tls =>
        {
            tls.ClientCertificatePath = certificates.PfxPath;
            tls.ClientCertificatePassword = TestCertificates.Password;
        });

        Validate(config).Succeeded.Should().BeTrue("it should accept a readable PFX file with its password");
        ShouldCarryTheCertificate(RabbitMqConnections.CreateFactory(config), "it should present the PFX certificate to the broker");
    }

    [Fact]
    public void When_A_Pem_Certificate_And_Key_Are_Configured()
    {
        var config = Config(tls =>
        {
            tls.ClientCertificatePath = certificates.CertificatePemPath;
            tls.ClientCertificateKeyPath = certificates.KeyPemPath;
        });

        Validate(config).Succeeded.Should().BeTrue("it should accept a PEM certificate with a separate key file");
        ShouldCarryTheCertificate(RabbitMqConnections.CreateFactory(config), "it should present the PEM certificate to the broker");
    }

    [Fact]
    public void When_The_Pem_Key_Is_In_The_Certificate_File()
    {
        var config = Config(tls => tls.ClientCertificatePath = certificates.CombinedPemPath);

        Validate(config).Succeeded.Should().BeTrue("it should read the key from the certificate file when no key file is set");
        ShouldCarryTheCertificate(RabbitMqConnections.CreateFactory(config), "it should present the PEM certificate to the broker");
    }

    [Fact]
    public void When_The_Pem_File_Starts_With_A_Byte_Order_Mark()
    {
        var config = Config(tls => tls.ClientCertificatePath = certificates.BomPemPath);

        Validate(config).Succeeded.Should().BeTrue("it should read a PEM file saved with a UTF-8 byte order mark as PEM");
        ShouldCarryTheCertificate(RabbitMqConnections.CreateFactory(config), "it should present the PEM certificate to the broker");
    }

    [Fact]
    public void When_The_Pem_Key_Is_Encrypted()
    {
        var config = Config(tls =>
        {
            tls.ClientCertificatePath = certificates.CertificatePemPath;
            tls.ClientCertificateKeyPath = certificates.EncryptedKeyPemPath;
            tls.ClientCertificatePassword = TestCertificates.Password;
        });

        Validate(config).Succeeded.Should().BeTrue("it should decrypt an encrypted PEM key with the password");
        ShouldCarryTheCertificate(RabbitMqConnections.CreateFactory(config), "it should present the PEM certificate to the broker");
    }

    [Fact]
    public void When_Tls_Is_Enabled_With_HostName()
    {
        var config = new RabbitMqConfig
        {
            QueueName = "orders",
            HostName = "rabbit.internal",
            Tls = new RabbitMqTlsConfig
            {
                Enabled = true,
                ClientCertificatePath = certificates.PfxPath,
                ClientCertificatePassword = TestCertificates.Password
            }
        };

        Validate(config).Succeeded.Should().BeTrue("it should accept a client certificate with HostName and Tls:Enabled");
        ShouldCarryTheCertificate(RabbitMqConnections.CreateFactory(config), "it should present the certificate on the HostName path too");
    }

    [Fact]
    public void When_No_Client_Certificate_Is_Configured() =>
        (RabbitMqConnections.CreateFactory(Config(_ => { })).Ssl.Certs?.Count ?? 0).Should().Be(0,
            "it should present no client certificate unless one is configured");

    [Fact]
    public void When_The_Certificate_File_Does_Not_Exist()
    {
        var result = Validate(Config(tls => tls.ClientCertificatePath = certificates.MissingPath));

        result.Failed.Should().BeTrue("it should fail at startup, not at connect time");
        result.FailureMessage.Should().Contain("Tls:ClientCertificatePath", "it should name the setting");
    }

    [Fact]
    public void When_The_Key_File_Does_Not_Exist()
    {
        var result = Validate(Config(tls =>
        {
            tls.ClientCertificatePath = certificates.CertificatePemPath;
            tls.ClientCertificateKeyPath = certificates.MissingPath;
        }));

        result.FailureMessage.Should().Contain("Tls:ClientCertificateKeyPath", "it should name the setting");
    }

    [Fact]
    public void When_The_Password_Is_Wrong()
    {
        var result = Validate(Config(tls =>
        {
            tls.ClientCertificatePath = certificates.PfxPath;
            tls.ClientCertificatePassword = "wrong-Pa55word";
        }));

        result.Failed.Should().BeTrue("it should fail at startup when the certificate cannot be loaded");
        result.FailureMessage.Should().Contain("Tls:ClientCertificatePath", "it should name the setting")
            .And.NotContain("wrong-Pa55word", "it should never echo the password");
    }

    [Fact]
    public void When_The_Certificate_Has_No_Private_Key()
    {
        var result = Validate(Config(tls => tls.ClientCertificatePath = certificates.CertificatePemPath));

        result.FailureMessage.Should().Contain("private key", "it should reject a certificate that cannot authenticate the client");
    }

    [Fact]
    public void When_The_Pfx_Holds_No_Private_Key()
    {
        var result = Validate(Config(tls => tls.ClientCertificatePath = certificates.CertificateOnlyPfxPath));

        result.FailureMessage.Should().Contain("has no private key", "it should reject a PFX that cannot authenticate the client");
    }

    [Fact]
    public void When_A_Certificate_Is_Configured_Without_Tls()
    {
        var config = Config(tls => tls.ClientCertificatePath = certificates.PfxPath) with
        {
            ConnectionString = "amqp://user:pw@localhost/"
        };

        Validate(config).FailureMessage.Should().Contain("ClientCertificate", "it should reject a client certificate on a connection without TLS")
            .And.Contain("TLS", "it should say what is missing");
    }

    [Fact]
    public void When_Both_A_File_And_A_Thumbprint_Are_Configured()
    {
        var result = Validate(Config(tls =>
        {
            tls.ClientCertificatePath = certificates.PfxPath;
            tls.ClientCertificatePassword = TestCertificates.Password;
            tls.ClientCertificateThumbprint = certificates.Thumbprint;
        }));

        result.FailureMessage.Should().Contain("either", "it should accept one certificate source, not both");
    }

    [Fact]
    public void When_A_Password_Is_Set_Without_A_File() =>
        Validate(Config(tls => tls.ClientCertificatePassword = "orphan")).FailureMessage
            .Should().Contain("ClientCertificatePassword", "it should reject a password that applies to nothing");

    [Fact]
    public void When_A_Key_File_Is_Set_Without_A_Certificate_File() =>
        Validate(Config(tls => tls.ClientCertificateKeyPath = certificates.KeyPemPath)).FailureMessage
            .Should().Contain("ClientCertificateKeyPath", "it should reject a key file that applies to nothing");

    [Fact]
    public void When_The_Thumbprint_Is_Not_In_The_Store()
    {
        var result = Validate(Config(tls => tls.ClientCertificateThumbprint = "00 11 22 33 44 55 66 77 88 99 AA BB CC DD EE FF 00 11 22 33"));

        result.FailureMessage.Should().Contain("Tls:ClientCertificateThumbprint", "it should name the setting")
            .And.Contain("0011223344556677", "it should report the normalized thumbprint it looked for");
    }

    [Theory]
    [InlineData("not-hex")]
    [InlineData("0011")]
    public void When_The_Thumbprint_Is_Malformed(string thumbprint) =>
        Validate(Config(tls => tls.ClientCertificateThumbprint = thumbprint)).FailureMessage
            .Should().Contain("hexadecimal", "it should say what a thumbprint looks like");

    [Fact]
    public void When_The_Store_Settings_Are_Not_Defined()
    {
        var result = Validate(Config(tls =>
        {
            tls.ClientCertificateThumbprint = certificates.Thumbprint;
            tls.ClientCertificateStoreLocation = (StoreLocation)99;
            tls.ClientCertificateStoreName = (StoreName)99;
        }));

        result.FailureMessage.Should().Contain("ClientCertificateStoreLocation", "it should reject an undefined store location")
            .And.Contain("ClientCertificateStoreName", "it should reject an undefined store name");
    }

    [Fact]
    public void When_Printing_The_Config()
    {
        var config = Config(tls =>
        {
            tls.ClientCertificatePath = certificates.PfxPath;
            tls.ClientCertificatePassword = TestCertificates.Password;
        });

        config.Tls.ToString().Should().NotContain(TestCertificates.Password, "it should redact the certificate password")
            .And.Contain("ClientCertificatePassword = ***", "it should show that a password is set");
        config.ToString().Should().NotContain(TestCertificates.Password, "it should redact the password in the queue's config too");
    }

    [Fact]
    public async Task When_A_Queue_Connects_More_Than_Once()
    {
        var queue = new RabbitMqMessageQueue<TestMessage>(Config(tls =>
        {
            tls.ClientCertificatePath = certificates.PfxPath;
            tls.ClientCertificatePassword = TestCertificates.Password;
        }));

        var first = queue.CreateConnectionFactory().Ssl.Certs!.Cast<X509Certificate2>().Single();
        var second = queue.CreateConnectionFactory().Ssl.Certs!.Cast<X509Certificate2>().Single();
        await queue.DisposeAsync();

        second.Should().BeSameAs(first, "it should load the client certificate once per queue, not once per connection");
        queue.ClientCertificate.Should().BeSameAs(first, "it should own the certificate it presents");
        first.Handle.Should().Be(IntPtr.Zero, "it should dispose the certificate with the queue");
    }

    [Theory]
    [InlineData("0011223344556677889900112233445566778899AA")]
    [InlineData("00112233445566778899001122334455667788")]
    public void When_The_Thumbprint_Has_The_Wrong_Length(string thumbprint) =>
        Validate(Config(tls => tls.ClientCertificateThumbprint = thumbprint)).FailureMessage
            .Should().Contain("40 hexadecimal", "it should accept a SHA-1 thumbprint only");

    [Theory]
    [InlineData("00:11:22:33:44:55:66:77:88:99:aa:bb:cc:dd:ee:ff:00:11:22:33")]
    [InlineData("00-11-22-33-44-55-66-77-88-99-AA-BB-CC-DD-EE-FF-00-11-22-33")]
    [InlineData("\u200E00112233445566778899aabbccddeeff00112233\t\n")]
    [InlineData(" 00 11 22 33 44 55 66 77 88 99 AA BB CC DD EE FF 00 11 22 33 \u200F")]
    public void When_The_Thumbprint_Is_Copied_With_Separators(string thumbprint) =>
        Validate(Config(tls => tls.ClientCertificateThumbprint = thumbprint)).FailureMessage
            .Should().Contain("00112233445566778899AABBCCDDEEFF00112233 was not found",
                "it should ignore separators, whitespace and direction marks and look the thumbprint up in upper case");

    [Fact]
    public void When_The_Store_Cannot_Be_Opened()
    {
        Assert.SkipWhen(OperatingSystem.IsWindows(), "Windows opens every LocalMachine store; Linux and macOS refuse LocalMachine/AddressBook");

        var result = Validate(Config(tls =>
        {
            tls.ClientCertificateThumbprint = certificates.Thumbprint;
            tls.ClientCertificateStoreLocation = StoreLocation.LocalMachine;
            tls.ClientCertificateStoreName = StoreName.AddressBook;
        }));

        result.FailureMessage.Should().Contain("could not be opened", "it should report a store it cannot open as a startup failure")
            .And.Contain("LocalMachine/AddressBook", "it should name the store");
    }
}
