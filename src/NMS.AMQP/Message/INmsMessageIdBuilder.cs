namespace Apache.NMS.AMQP.Message
{
    public interface INmsMessageIdBuilder
    {
        object CreateMessageId(string producerId, long messageSequence);
    }
}