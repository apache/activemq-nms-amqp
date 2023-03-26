using System;
using System.ComponentModel;
using Apache.NMS.Util;

namespace Apache.NMS.AMQP.Message
{
    internal class NmsMessagePropertiesInterceptor : PrimitiveMapInterceptor
    {
        private const string NMSX_GROUP_ID = "NMSXGroupId";
        private const string NMSX_GROUP_SEQ = "NMSXGroupSeq";
        private const string NMS_AMQP_ACK_TYPE = "NMS_AMQP_ACK_TYPE";

        private readonly NmsMessage nmsMessage;

        public NmsMessagePropertiesInterceptor(NmsMessage nmsMessage, bool isReadOnlyProperties) : base(nmsMessage, nmsMessage.Facade.Properties, isReadOnlyProperties)
        {
            this.nmsMessage = nmsMessage;
        }

        protected override object GetObjectProperty(string name)
        {
            CheckPropertyNameIsValid(name);
            
            switch (name)
            {
                case NMSX_GROUP_ID:
                    return this.nmsMessage.Facade.GroupId;
                case NMSX_GROUP_SEQ:
                    return this.nmsMessage.Facade.GroupSequence;
                case NMS_AMQP_ACK_TYPE:
                    return this.nmsMessage.NmsAcknowledgeCallback?.AcknowledgementType;
                default:
                    return base.GetObjectProperty(name);
            }
        }

        protected override void SetObjectProperty(string name, object value)
        {
            CheckPropertyNameIsValid(name);

            switch (name)
            {
                case NMSX_GROUP_ID when value is string groupId:
                    this.nmsMessage.Facade.GroupId = groupId;
                    break;
                case NMSX_GROUP_SEQ:
                    var groupSequence = Convert.ToUInt32(value);
                    this.nmsMessage.Facade.GroupSequence = groupSequence;
                    break;
                case NMS_AMQP_ACK_TYPE:
                    if (this.nmsMessage.NmsAcknowledgeCallback == null)
                    {
                        throw new NMSException($"Session Acknowledgement Mode does not allow setting: {NMS_AMQP_ACK_TYPE}");
                    }
                    var ackType = Convert.ToInt32(value);
                    this.nmsMessage.NmsAcknowledgeCallback.AcknowledgementType = (AckType) ackType;
                    break;
                default:
                    base.SetObjectProperty(name, value);
                    break;
            }
        }

        private static void CheckPropertyNameIsValid(string name)
        {
            if (name == null)
            {
                throw new ArgumentNullException(paramName: nameof(name), message: "Property name must not be null");
            }

            if (name.Length == 0)
            {
                throw new ArgumentException(message: "Property name must not be the empty string", paramName: nameof(name));
            }
        }
    }
}