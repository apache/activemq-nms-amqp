using Amqp.Types;

namespace NMS.AMQP.Test.TestAmqp.BasicTypes
{
    public static class TerminusExpiryPolicy
    {
        public static readonly Symbol LINK_DETACH = new Symbol("link-detach");
        public static readonly Symbol NEVER = new Symbol("never");
    }
}