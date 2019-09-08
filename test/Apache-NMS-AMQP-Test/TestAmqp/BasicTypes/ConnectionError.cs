using Amqp.Types;

namespace NMS.AMQP.Test.TestAmqp.BasicTypes
{
    public static class ConnectionError
    {
        public static readonly Symbol CONNECTION_FORCED = new Symbol("amqp:connection:forced");
    }
}