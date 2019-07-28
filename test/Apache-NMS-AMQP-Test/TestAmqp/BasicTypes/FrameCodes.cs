using Amqp.Framing;

namespace NMS.AMQP.Test.TestAmqp.BasicTypes
{
    public static class FrameCodes
    {
        public static readonly ulong TRANSFER = new Transfer().Descriptor.Code;
    }
}