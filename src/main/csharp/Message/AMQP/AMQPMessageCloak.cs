/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Apache.NMS;
using Apache.NMS.Util;
using Amqp;
using Amqp.Types;
using Amqp.Framing;

namespace NMS.AMQP.Message.AMQP
{
    using Util;
    using Util.Types.Map.AMQP;
    using Cloak;
    using Factory;
    using System.Reflection;

    class AMQPMessageCloak : IMessageCloak
    {
        private TimeSpan timeToLive;
        private IDestination replyTo;
        private bool redelivered = false;
        private string msgId;
        private IDestination destination;
        private string correlationId;
        private IPrimitiveMap properties;
        private MessagePropertyIntercepter propertyHelper;

        private Header messageHeader = null;
        private DeliveryAnnotations deliveryAnnontations = null;
        private MessageAnnotations messageAnnontations = null;
        private ApplicationProperties applicationProperties = null;
        private Properties messageProperties = null;

#pragma warning disable CS0414
        private Footer messageFooter = null;
#pragma warning restore CS0414

        private byte[] content;
        private bool readOnlyProperties = false;

        protected Amqp.Message message;
        protected readonly Connection connection;
        protected MessageConsumer consumer;

        internal AMQPMessageCloak(Connection c)
        {
            message = new Amqp.Message();
            connection = c;
            InitMessage();
        }

        internal AMQPMessageCloak(MessageConsumer c, Amqp.Message msg)
        {
            message = msg;
            consumer = c;
            connection = c.Session.Connection;
            InitMessage();
            InitDeliveryAnnotations();
        }


        #region Internal Properties

        internal Connection Connection { get { return connection; } }

        internal Amqp.Message AMQPMessage { get { return message; } }

        internal virtual byte JMSMessageType { get { return MessageSupport.JMS_TYPE_MSG; } }

        #endregion

        private void InitMessage()
        {
            InitMessageHeader();
            InitMessageProperties();
            SetMessageAnnotation(SymbolUtil.JMSX_OPT_MSG_TYPE, (sbyte)JMSMessageType);
        }

        protected virtual void CopyInto(IMessageCloak msg)
        {
            MessageTransformation.CopyNMSMessageProperties(this, msg);
            msg.AckHandler = this.AckHandler;
        }

        #region Protected Amqp.Message Initialize/Accessor

        protected void InitMessageHeader()
        {
            if (this.messageHeader == null && this.message.Header == null)
            {
                this.messageHeader = new Header();
                this.message.Header = this.messageHeader;
            }
            else if (this.messageHeader == null && this.message.Header != null)
            {
                this.messageHeader = this.message.Header;
            }
            else if (this.messageHeader != null && this.message.Header == null)
            {
                this.message.Header = this.messageHeader;
            }
        }

        protected void InitMessageProperties()
        {
            if (this.messageProperties == null && this.message.Properties == null)
            {
                this.messageProperties = new Properties();
                this.message.Properties = this.messageProperties;
            }
            else if (this.messageProperties == null && this.message.Properties != null)
            {
                this.messageProperties = this.message.Properties;
            }
            else if (this.messageProperties != null && this.message.Properties == null)
            {
                this.message.Properties = this.messageProperties;
            }
        }

        protected void InitApplicationProperties()
        {
            if (this.applicationProperties == null && this.message.ApplicationProperties == null)
            {
                this.applicationProperties = new ApplicationProperties();
                this.message.ApplicationProperties = this.applicationProperties;
            }
            else if (this.applicationProperties == null && this.message.ApplicationProperties != null)
            {
                this.applicationProperties = this.message.ApplicationProperties;
            }
            else if (this.applicationProperties != null && this.message.ApplicationProperties == null)
            {
                this.message.ApplicationProperties = this.applicationProperties;

            }
        }

        protected void InitDeliveryAnnotations()
        {
            if (this.deliveryAnnontations == null && this.message.DeliveryAnnotations == null)
            {
                this.deliveryAnnontations = new DeliveryAnnotations();
                this.message.DeliveryAnnotations = this.deliveryAnnontations;
            }
            else if (this.deliveryAnnontations == null && this.message.DeliveryAnnotations != null)
            {
                this.deliveryAnnontations = this.message.DeliveryAnnotations;
            }
            else if (this.deliveryAnnontations != null && this.message.DeliveryAnnotations == null)
            {
                this.message.DeliveryAnnotations = this.deliveryAnnontations;
            }
        }

        protected void InitMessageAnnontations()
        {
            if (this.messageAnnontations == null && this.message.MessageAnnotations == null)
            {
                this.messageAnnontations = new MessageAnnotations();
                this.message.MessageAnnotations = messageAnnontations;
            }
            else if (this.messageAnnontations == null && this.message.MessageAnnotations != null)
            {
                this.messageAnnontations = this.message.MessageAnnotations;
            }
            else if (this.messageAnnontations != null && this.message.MessageAnnotations == null)
            {
                this.message.MessageAnnotations = this.messageAnnontations;
            }
        }

        protected void SetDeliveryAnnotation(Symbol key, object value)
        {
            InitDeliveryAnnotations();
            this.deliveryAnnontations[key] = value;
        }

        protected void SetMessageAnnotation(Symbol key, object value)
        {
            InitMessageAnnontations();
            messageAnnontations[key] = value;
        }

        protected object GetMessageAnnotation(Symbol key)
        {
            InitMessageAnnontations();
            return messageAnnontations[key];
        }

        #endregion

        #region IMessageCloak Properties

        public bool IsReceived { get { return consumer != null; } }

        public virtual byte[] Content
        {
            get
            {
                return content;
            }

            set
            {
                content = value;
            }
        }

        public virtual bool IsBodyReadOnly { get; set; }

        public virtual bool IsPropertiesReadOnly
        {
            get
            {
                return (this.propertyHelper == null) ? readOnlyProperties : this.propertyHelper.ReadOnly;
            }
            set
            {
                if (this.propertyHelper != null)
                    this.propertyHelper.ReadOnly = value;
                readOnlyProperties = value;
            }
        }


        public string NMSCorrelationID
        {
            get
            {
                if ( null != this.correlationId)
                {
                    return this.correlationId;
                }
                object objId = this.messageProperties.GetCorrelationId();
                if (objId != null)
                {
                    // correlationId strings are returned as-is to the application, otherwise
                    // convert it to a NMSMessageId string 
                    if (objId is string)
                    {
                        this.correlationId = objId as string;
                    }
                    else
                    {
                        this.correlationId = MessageSupport.CreateNMSMessageId(objId);
                    }
                }

                return this.correlationId;
            }
            set
            {
                object objId = MessageSupport.CreateAMQPMessageId(value);
                this.messageProperties.SetCorrelationId(objId);
                this.correlationId = value;
            }
        }

        public MsgDeliveryMode NMSDeliveryMode
        {
            get
            {
                if (this.messageHeader.Durable)
                {
                    return MsgDeliveryMode.Persistent;
                }
                else
                {
                    return MsgDeliveryMode.NonPersistent;
                }

            }
            set
            {
                if (value.Equals(MsgDeliveryMode.Persistent))
                {
                    this.messageHeader.Durable = true;
                }
                else
                {
                    this.messageHeader.Durable = false;
                }
            }
        }

        public IDestination NMSDestination
        {
            get
            {
                if (destination == null && consumer != null)
                {
                    object typeObj = GetMessageAnnotation(SymbolUtil.JMSX_OPT_DEST);
                    if (typeObj != null)
                    {
                        byte type = Convert.ToByte(typeObj);
                        destination = MessageSupport.CreateDestinationFromMessage(Connection, messageProperties, type);
                        if(destination == null)
                        {
                            destination = consumer.Destination;
                        }
                    }
                }
                return destination;
            }
            set
            {
                string destString = null;
                IDestination dest = null;
                if (value != null) {
                    destString = UriUtil.GetAddress(value, Connection);
                    dest = value;
                }
                this.messageProperties.To = destString;
                SetMessageAnnotation(SymbolUtil.JMSX_OPT_DEST, MessageSupport.GetValueForDestination(dest));
                destination = dest;
            }
        }

        public string NMSMessageId
        {
            get
            {
                object objId = this.messageProperties.GetMessageId();
                if (this.msgId == null && objId != null)
                {
                    this.msgId = MessageSupport.CreateNMSMessageId(objId);
                }
                return this.msgId;
            }
            set
            {
                object msgId = MessageSupport.CreateAMQPMessageId(value);
                //Tracer.InfoFormat("Set message Id to <{0}>: {1}", msgId.GetType().Name, msgId.ToString());
                this.messageProperties.SetMessageId(msgId);
                this.msgId = value;
            }
        }

        public MsgPriority NMSPriority
        {
            get { return MessageSupport.GetPriorityFromValue(this.messageHeader.Priority); }
            set
            {
                this.messageHeader.Priority = MessageSupport.GetValueForPriority(value);
            }
        }

        public bool NMSRedelivered
        {
            get
            {
                if (this.messageHeader.DeliveryCount > 0)
                {
                    redelivered = true;
                }
                return redelivered;
            }
            set { redelivered = value; }
        }

        public IDestination NMSReplyTo
        {
            get
            {
                if (replyTo == null && IsReceived)
                {
                    object typeObj = GetMessageAnnotation(SymbolUtil.JMSX_OPT_REPLY_TO);
                    if (typeObj != null)
                    {
                        byte type = Convert.ToByte(typeObj);
                        replyTo = MessageSupport.CreateDestinationFromMessage(Connection, messageProperties, type, true);
                    }
                }
                return replyTo;
            }
            set
            {
                IDestination dest = null;
                string destString = null;
                if (value != null)
                {
                    destString = UriUtil.GetAddress(value, Connection);
                    dest = value;
                    SetMessageAnnotation(SymbolUtil.JMSX_OPT_REPLY_TO, MessageSupport.GetValueForDestination(dest));
                }
                this.messageProperties.ReplyTo = destString;

                replyTo = dest;
            }
        }

        public DateTime NMSTimestamp
        {
            get { return messageProperties.CreationTime; }
            set
            {
                messageProperties.CreationTime = value;
                if (NMSTimeToLive != null && NMSTimeToLive != TimeSpan.Zero)
                {
                    messageProperties.AbsoluteExpiryTime = value + timeToLive;
                }
            }
        }

        public TimeSpan NMSTimeToLive
        {
            get
            {
                if ( timeToLive != null)
                {
                    return timeToLive;
                }
                if (messageProperties.AbsoluteExpiryTime == DateTime.MinValue)
                {

                    timeToLive = TimeSpan.FromMilliseconds(Convert.ToDouble(this.messageHeader.Ttl));
                    return timeToLive;
                }
                else
                {
                    return messageProperties.AbsoluteExpiryTime - NMSTimestamp;
                }
            }
            set
            {
                timeToLive = value;
            }
        }

        public string NMSType
        {
            get { return this.messageProperties.Subject; }
            set { this.messageProperties.Subject = value; }
        }

        public IPrimitiveMap Properties
        {
            get
            {
                if (properties == null)
                {
                    InitApplicationProperties();
                    properties = new AMQPPrimitiveMap(this.applicationProperties);
                    propertyHelper = new MessagePropertyIntercepter(this, properties, readOnlyProperties);
                }
                return propertyHelper;
            }
        }

        public int DeliveryCount
        {
            get
            {
                return Convert.ToInt32(this.messageHeader.DeliveryCount);
            }

            set
            {
                this.messageHeader.DeliveryCount = Convert.ToUInt32(value);
            }
        }

        public int RedeliveryCount
        {
            get
            {
                return DeliveryCount - 1;
            }

            set
            {
                DeliveryCount = value + 1;
            }
        }

        public MessageAcknowledgementHandler AckHandler { get; set; }
        
        public void Acknowledge()
        {
            if (AckHandler != null)
            {
                if (connection.IsClosed)
                {
                    throw new IllegalStateException("Can not acknowledge Message on closed connection.");
                }
            
                AckHandler.Acknowledge();
                AckHandler = null;
            }
        }

        public virtual void ClearBody()
        {
            Content = null;
        }

        public virtual void ClearProperties()
        {
            if (properties != null)
            {
                propertyHelper.Clear();
            }
        }
        
        public virtual IMessageCloak Copy()
        {
            IMessageCloak copy = null;
            switch(JMSMessageType)
            {
                case MessageSupport.JMS_TYPE_MSG:
                    copy = new AMQPMessageCloak(connection);
                    break;
                case MessageSupport.JMS_TYPE_BYTE:
                    copy = new AMQPBytesMessageCloak(connection);
                    break;
                case MessageSupport.JMS_TYPE_TXT:
                    copy = new AMQPTextMessageCloak(connection);
                    break;
                case MessageSupport.JMS_TYPE_MAP:
                    copy = new AMQPMapMessageCloak(connection);
                    break;
                case MessageSupport.JMS_TYPE_STRM:
                    copy = new AMQPStreamMessageCloak(connection);
                    break;
                case MessageSupport.JMS_TYPE_OBJ:
                    copy = new AMQPObjectMessageCloak(connection, (this as AMQPObjectMessageCloak).Type);
                    break;
                default:
                    throw new NMSException("Fatal error Invalid JMS type.");
            }
            
            CopyInto(copy);
            return copy;
        }

        public object GetMessageAnnotation(string symbolKey)
        {
            Symbol sym = symbolKey;
            return GetMessageAnnotation(sym);
        }

        public void SetMessageAnnotation(string symbolKey, object value)
        {
            Symbol sym = symbolKey;
            SetMessageAnnotation(sym, value);
        }

        public object GetDeliveryAnnotation(string symbolKey)
        {
            Symbol sym = symbolKey;
            return GetDeliveryAnnotation(sym);
        }

        public void SetDeliveryAnnotation(string symbolKey, object value)
        {
            Symbol sym = symbolKey;
            SetDeliveryAnnotation(sym, value);
        }

        public string GetContentType()
        {
            return GetContentTypeSymbol();
        }

        public void SetContentType(string type)
        {
            SetContentType(new Symbol(type));
        }

        protected virtual Symbol GetContentTypeSymbol()
        {
            return this.messageProperties.ContentType;
        }

        protected virtual void SetContentType(Symbol type)
        {
            this.messageProperties.ContentType = type;
        }
        
        #endregion

        public override string ToString()
        {
            string result = string.Format("{0}:\n", this.GetType());
            result += string.Format("inner amqp message: \n{0}\n", AMQPMessageCloak.ToString(message));
            result += "NMS Fields = [\n";
            foreach (MemberInfo info in this.GetType().GetMembers())
            {
                if (info is PropertyInfo)
                {
                    PropertyInfo prop = info as PropertyInfo;
                    if (prop.GetGetMethod(true).IsPublic)
                    {
                        try
                        {
                            result += string.Format("{0} = {1},\n", prop.Name, prop.GetValue(this, null));
                        }catch(TargetInvocationException tie)
                        {
                            Tracer.InfoFormat("Failed to invoke Member field accessor: {0}, cause: {1}", prop.Name, tie);
                        }
                    }
                }
            }
            result = result.Substring(0, result.Length - 2) + "\n]";
            return result;
        }

        public static string ToString(Amqp.Message message)
        {
            if (message == null) return "null";
            string result = "Type="+ message.GetType().Name +":\n";
            
            if (message.Header != null)
            {
                result += "Message Header: " + message.Header.ToString() + "\n";
            }
            
            return result;
        }
    }
}
