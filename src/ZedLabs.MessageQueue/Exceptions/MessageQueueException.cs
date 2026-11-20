namespace ZedLabs.MessageQueue.Exceptions;

/// <summary>
/// The base of exceptions the library throws for broker, publish and configuration failures. Messages never
/// contain connection strings, passwords or exception text from the broker client (S10).
/// </summary>
public class MessageQueueException : Exception
{
    /// <summary>Creates the exception with <paramref name="message"/>.</summary>
    public MessageQueueException(string message) : base(message) { }

    /// <summary>Creates the exception with <paramref name="message"/> and the failure that caused it.</summary>
    public MessageQueueException(string message, Exception innerException) : base(message, innerException) { }
}

/// <summary>
/// The message queue registration or configuration is invalid, for example an unknown <c>Provider</c>. Thrown during
/// service registration; option values are validated at host start (<c>OptionsValidationException</c>).
/// </summary>
public sealed class ConfigurationException : MessageQueueException
{
    /// <summary>Creates the exception with <paramref name="message"/>.</summary>
    public ConfigurationException(string message) : base(message) { }

    /// <summary>Creates the exception with <paramref name="message"/> and the failure that caused it.</summary>
    public ConfigurationException(string message, Exception innerException) : base(message, innerException) { }
}
