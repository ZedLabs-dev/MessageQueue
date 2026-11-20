using ZedLabs.MessageQueue.Exceptions;

namespace ZedLabs.MessageQueue.RabbitMQ;

// The broker answered a publish with a nack or a mandatory return: the message was definitely not stored, so it is
// safe to publish it again. A confirm timeout is not this: the message may have been stored.
sealed class PublishRefusedException : MessageQueueException
{
    public PublishRefusedException(string message, Exception innerException) : base(message, innerException) { }
}
