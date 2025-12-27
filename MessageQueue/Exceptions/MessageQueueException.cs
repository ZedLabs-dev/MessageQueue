namespace ESE.MessageQueue.Exceptions;

public class MessageQueueException : Exception
{
    public MessageQueueException(string message) : base(message) { }

    public MessageQueueException(string message, Exception innerException) : base(message, innerException) { }
}

public class TransientMessageException : MessageQueueException
{
    public TransientMessageException(string message) : base(message) { }

    public TransientMessageException(string message, Exception innerException) : base(message, innerException) { }
}

public class ConfigurationException : MessageQueueException
{
    public ConfigurationException(string message) : base(message) { }

    public ConfigurationException(string message, Exception innerException) : base(message, innerException) { }
}