using System.Text;
using Apache.NMS.AMQP.Provider.Amqp;

namespace Apache.NMS.AMQP.Message
{
    public class DefaultMessageIdBuilder : INmsMessageIdBuilder
    {
        private readonly StringBuilder builder = new StringBuilder();
        private int idPrefixLength = -1;

        public object CreateMessageId(string producerId, long messageSequence)
        {
            if (idPrefixLength < 0)
            {
                Initialize(producerId);
            }
            
            builder.Length = idPrefixLength;
            builder.Append(messageSequence);
            
            return builder.ToString();
        }

        private void Initialize(string producerId)
        {
            if (!AmqpMessageIdHelper.HasMessageIdPrefix(producerId))
            {
                builder.Append(AmqpMessageIdHelper.NMS_ID_PREFIX);
            }
            builder.Append(producerId).Append("-");
            idPrefixLength = builder.Length;
        }
    }
}