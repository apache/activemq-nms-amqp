using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP
{
    public class NmsDurableTopicSubscriber : NmsMessageConsumer
    {
        public NmsDurableTopicSubscriber(Id consumerId, NmsSession session, IDestination destination, string selector, bool noLocal) : base(consumerId, session, destination, selector, noLocal)
        {
        }

        public NmsDurableTopicSubscriber(Id consumerId, NmsSession session, IDestination destination, string name, string selector, bool noLocal) : base(consumerId, session, destination, name, selector, noLocal)
        {
        }

        protected override bool IsDurableSubscription => true;
    }
}