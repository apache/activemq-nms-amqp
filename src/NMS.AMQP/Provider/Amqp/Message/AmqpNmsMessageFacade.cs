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
using System.Text;
using Amqp;
using Amqp.Framing;
using Amqp.Types;
using Apache.NMS.AMQP.Message;
using Apache.NMS.AMQP.Message.Facade;
using Apache.NMS.AMQP.Util;
using Apache.NMS.AMQP.Util.Types.Map.AMQP;

namespace Apache.NMS.AMQP.Provider.Amqp.Message
{
    public class AmqpNmsMessageFacade : INmsMessageFacade
    {
        private const int ABSOLUTE_EXPIRY_TIME_INDEX = 8;
        
        private TimeSpan? amqpTimeToLiveOverride;
        private IDestination destination;
        private IDestination replyTo;
        private IDestination consumerDestination;
        private IAmqpConnection connection;
        private DateTime? syntheticExpiration;
        public global::Amqp.Message Message { get; private set; }

        public int RedeliveryCount
        {
            get => Convert.ToInt32(Header.DeliveryCount);
            set => Header.DeliveryCount = Convert.ToUInt32(value);
        }

        public int DeliveryCount
        {
            get => RedeliveryCount + 1;
            set => RedeliveryCount = value - 1;
        }

        private IPrimitiveMap properties;

        public IPrimitiveMap Properties
        {
            get
            {
                if (properties == null)
                {
                    LazyCreateApplicationProperties();
                    properties = new AMQPPrimitiveMap(Message.ApplicationProperties);
                }

                return properties;
            }
        }

        public string NMSMessageId
        {
            get => Message.Properties == null ? null : AmqpMessageIdHelper.ToMessageIdString(Message.Properties.GetMessageId());
            set
            {
                object idObject = AmqpMessageIdHelper.ToIdObject(value);
                if (idObject != null)
                {
                    LazyCreateProperties();
                    Message.Properties.SetMessageId(idObject);
                }
                else
                    Message.Properties?.SetMessageId(null);
            }
        }

        public object ProviderMessageIdObject
        {
            get => Message.Properties?.GetMessageId();
            set
            {
                if (Message.Properties == null)
                {
                    if (value == null)
                    {
                        return;
                    }
                    LazyCreateProperties();
                }
                Message.Properties?.SetMessageId(value);
            }
        }

        public string NMSCorrelationID
        {
            get => Message.Properties == null ? null : AmqpMessageIdHelper.ToCorrelationIdString(Message.Properties.GetCorrelationId());
            set
            {
                object idObject = null;
                if (value != null)
                {
                    if (AmqpMessageIdHelper.HasMessageIdPrefix(value))
                    {
                        // JMSMessageID value, process it for possible type conversion
                        idObject = AmqpMessageIdHelper.ToIdObject(value);
                    }
                    else
                    {
                        idObject = value;
                    }
                }

                if (idObject != null)
                {
                    LazyCreateProperties();
                    Message.Properties.SetCorrelationId(idObject);
                }
                else
                    Message.Properties?.SetCorrelationId(null);
            }
        }

        public IDestination NMSDestination
        {
            get => destination ?? (destination = AmqpDestinationHelper.GetDestination(this, connection, consumerDestination));
            set
            {
                destination = value;
                string address = AmqpDestinationHelper.GetDestinationAddress(value, connection);
                if (address != null)
                {
                    LazyCreateProperties();
                    Message.Properties.To = address;
                }
                else if (Message.Properties != null)
                {
                    Message.Properties.To = null;
                }
            }
        }

        public IDestination NMSReplyTo
        {
            get => replyTo ?? (replyTo = AmqpDestinationHelper.GetReplyTo(this, connection, consumerDestination));
            set
            {
                replyTo = value;
                string address = AmqpDestinationHelper.GetDestinationAddress(value, connection);
                if (address != null)
                {
                    LazyCreateProperties();
                    Message.Properties.ReplyTo = address;
                }
                else if (Message.Properties != null)
                {
                    Message.Properties.ReplyTo = null;
                }
            }
        }

        public TimeSpan NMSTimeToLive
        {
            get => amqpTimeToLiveOverride ?? TimeSpan.FromMilliseconds(Header.Ttl);
            set => amqpTimeToLiveOverride = value;
        }
        
        public bool IsPersistent
        {
            get => Message.Header.Durable;
            set => Message.Header.Durable = value;
        }

        public MsgPriority NMSPriority
        {
            get => MessageSupport.GetPriorityFromValue(Header.Priority);
            set => Header.Priority = MessageSupport.GetValueForPriority(value);
        }

        public bool NMSRedelivered
        {
            get => RedeliveryCount > 0;
            set
            {
                if (value)
                {
                    if (!NMSRedelivered)
                        RedeliveryCount = 1;
                }
                else
                {
                    if (NMSRedelivered)
                        RedeliveryCount = 0;
                }
            }
        }

        public DateTime NMSTimestamp
        {
            get => Message.Properties?.CreationTime ?? DateTime.MinValue;
            set
            {
                if (value != default)
                {
                    LazyCreateProperties();
                    Message.Properties.CreationTime = value;
                }
                else if (Message.Properties != null)
                {
                    Message.Properties.CreationTime = default;
                }
            }
        }

        public DateTime? Expiration
        {
            get => Message.Properties?.HasField(ABSOLUTE_EXPIRY_TIME_INDEX) == true ? Message.Properties.AbsoluteExpiryTime : syntheticExpiration;
            set
            {
                if (value != null)
                {
                    LazyCreateProperties();
                    Message.Properties.AbsoluteExpiryTime = value.Value;
                }
                else
                {
                    Message.Properties?.ResetField(ABSOLUTE_EXPIRY_TIME_INDEX);
                }
            }
        }

        public string NMSType
        {
            get => Message.Properties?.Subject;
            set
            {
                if (value != null)
                {
                    LazyCreateProperties();
                    Message.Properties.Subject = value;
                }
                else if (Message.Properties != null)
                {
                    Message.Properties.Subject = value;
                }
            }
        }

        public Header Header => Message.Header;

        public string GroupId
        {
            get => Message.Properties?.GroupId;
            set
            {
                if (value != null)
                {
                    LazyCreateProperties();
                    Message.Properties.GroupId = value;
                }
                else if (Message.Properties != null)
                {
                    Message.Properties.GroupId = null;
                }
            }
        }

        public string ReplyToGroupId
        {
            get => Message.Properties?.ReplyToGroupId;
            set
            {
                if (value != null)
                {
                    LazyCreateProperties();
                    Message.Properties.ReplyToGroupId = value;
                }
                else if (Message.Properties != null)
                {
                    Message.Properties.ReplyToGroupId = null;
                }
            }
        }

        public uint GroupSequence
        {
            get => Message.Properties?.GroupSequence ?? (uint) 0;
            set
            {
                if (value != 0)
                {
                    LazyCreateProperties();
                    Message.Properties.GroupSequence = value;
                }
                else if (Message.Properties != null)
                {
                    Message.Properties.GroupSequence = value;
                }
            }
        }

        public string ToAddress => Message.Properties?.To;
        public string ReplyToAddress => Message.Properties?.ReplyTo;

        public string UserId
        {
            get
            {
                if (Message.Properties?.UserId != null && Message.Properties.UserId.Length > 0)
                {
                    return Encoding.UTF8.GetString(Message.Properties.UserId);
                }

                return null;
            }
            set
            {
                byte[] bytes = null;
                if (value != null)
                {
                    bytes = Encoding.UTF8.GetBytes(value);
                }

                if (bytes == null)
                {
                    if (Message.Properties != null)
                    {
                        Message.Properties.UserId = null;
                    }
                }
                else
                {
                    LazyCreateProperties();
                    Message.Properties.UserId = bytes;
                }
            }
        }

        public MessageAnnotations MessageAnnotations => Message.MessageAnnotations;
        public virtual sbyte? JmsMsgType => MessageSupport.JMS_TYPE_MSG;

        /// <summary>
        /// The annotation value for the JMS Message content type.  For a generic JMS message this
        /// value is omitted so we return null here, subclasses should override this to return the
        /// correct content type value for their payload.
        /// </summary>
        public Symbol ContentType
        {
            get => Message.Properties?.ContentType;
            set
            {
                if (value != null)
                {
                    LazyCreateProperties();
                    Message.Properties.ContentType = value;
                }
                else if (Message.Properties != null)
                {
                    Message.Properties.ContentType = null;
                }
            }
        }

        public virtual void OnSend(TimeSpan producerTtl)
        {
            if (amqpTimeToLiveOverride.HasValue)
                Header.Ttl = Convert.ToUInt32(amqpTimeToLiveOverride.Value.TotalMilliseconds);
            else if (producerTtl != NMSConstants.defaultTimeToLive)
                Header.Ttl = Convert.ToUInt32(producerTtl.TotalMilliseconds);
        }

        /// <summary>
        /// Initialize the state of this message for receive.
        /// </summary>
        public virtual void Initialize(IAmqpConsumer consumer, global::Amqp.Message message)
        {
            this.consumerDestination = consumer.Destination;
            this.connection = consumer.Connection;
            Message = message;

            InitializeBody();
            InitializeHeader();

            TimeSpan ttl = NMSTimeToLive;
            DateTime? absoluteExpiryTime = Expiration;
            if (absoluteExpiryTime == null && ttl != default)
            {
                syntheticExpiration = DateTime.UtcNow + ttl;
            }
        }

        protected virtual void InitializeBody()
        {
        }

        /// <summary>
        /// Initialize the state of this message for send.
        /// </summary>
        public virtual void Initialize(IAmqpConnection connection)
        {
            this.connection = connection;

            Message = new global::Amqp.Message();
            InitializeEmptyBody();
            InitializeHeader();
        }

        private void InitializeHeader()
        {
            if (Message.Header == null)
            {
                Message.Header = new Header { Durable = true };
            }
        }

        /// <summary>
        /// Used to indicate that a Message object should empty the body element and make
        /// any other internal updates to reflect the message now has no body value.
        /// </summary>
        protected virtual void InitializeEmptyBody()
        {
        }

        public virtual NmsMessage AsMessage()
        {
            return new NmsMessage(this);
        }

        public virtual void ClearBody()
        {
        }

        public virtual INmsMessageFacade Copy()
        {
            AmqpNmsMessageFacade copy = new AmqpNmsMessageFacade();
            CopyInto(copy);
            return copy;
        }

        protected void CopyInto(AmqpNmsMessageFacade target)
        {
            target.connection = connection;
            target.consumerDestination = consumerDestination;
            target.syntheticExpiration = syntheticExpiration;
            target.amqpTimeToLiveOverride = amqpTimeToLiveOverride;
            target.destination = destination;
            target.replyTo = replyTo;

            ByteBuffer buffer = Message.Encode();
            target.Message = global::Amqp.Message.Decode(buffer);
            target.InitializeHeader();
            target.InitializeBody();
        }

        public object GetMessageAnnotation(Symbol annotationName)
        {
            return Message.MessageAnnotations?[annotationName];
        }

        public bool MessageAnnotationExists(Symbol annotationName)
        {
            return MessageAnnotations != null && MessageAnnotations.Map.ContainsKey(annotationName);
        }

        public void SetMessageAnnotation(Symbol symbolKeyName, string value)
        {
            LazyCreateMessageAnnotations();
            MessageAnnotations.Map.Add(symbolKeyName, value);
        }

        private void LazyCreateMessageAnnotations()
        {
            if (Message.MessageAnnotations == null)
                Message.MessageAnnotations = new MessageAnnotations();
        }

        private void LazyCreateProperties()
        {
            if (Message.Properties == null)
                Message.Properties = new Properties();
        }

        private void LazyCreateApplicationProperties()
        {
            if (Message.ApplicationProperties == null)
                Message.ApplicationProperties = new ApplicationProperties();
        }
    }
}