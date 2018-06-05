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
using NMS.AMQP;
using NMS.AMQP.Util;

namespace NMS.AMQP.Message
{

    using Cloak;
    
    internal enum AckType
    {
        ACCEPTED = 0,
        REJECTED = 1,
        RELEASED = 2,
        MODIFIED_FAILED = 3,
        MODIFIED_FAILED_UNDELIVERABLE = 4,
    }

    /// <summary>
    /// NMS.AMQP.Message.Message is the root message class that implements the Apache.NMS.IMessage interface.
    /// NMS.AMQP.Message.Message uses the NMS.AMQP.Message.Cloak.IMessageCloak interface to detach from the underlying AMQP 1.0 engine.
    /// </summary>
    class Message : IMessage
    {

        public static readonly string MESSAGE_VENDOR_ACK_PROP = PropertyUtil.CreateProperty("ACK.TYPE", "AMQP");

        protected readonly IMessageCloak cloak;

//        private bool isReadOnlyMsgBody = false;
//        private bool isReadOnlyProperties = false;
        private IPrimitiveMap propertyHelper = null;

        #region Constructors

        internal Message(IMessageCloak message)
        {
            this.cloak = message;
        }
        
        #endregion
        
        #region Protected Methods

        protected void FailIfReadOnlyMsgBody()
        {
            if(IsReadOnly == true)
            {
                throw new MessageNotWriteableException("Message is in Read-Only mode.");
            }
        }

        protected void FailIfWriteOnlyMsgBody()
        {
            if (IsReadOnly == false)
            {
                throw new MessageNotReadableException("Message is in Write-Only mode.");
            }
        }

        #endregion

        #region Public Properties

        public virtual byte[] Content
        {
            get
            {
                return cloak.Content;
            }

            set
            {
                cloak.Content = value;
            }
        }

        public virtual bool IsReadOnly
        {
            get { return cloak.IsBodyReadOnly; }
            internal set { cloak.IsBodyReadOnly = value; }
        }

        public virtual bool IsReadOnlyProperties
        {
            get { return cloak.IsPropertiesReadOnly; }
            internal set { cloak.IsPropertiesReadOnly = value; }
        }

        #endregion

        #region IMessage Properties


        public string NMSCorrelationID
        {
            get { return cloak.NMSCorrelationID; }
            set { cloak.NMSCorrelationID = value; }
        }

        public MsgDeliveryMode NMSDeliveryMode
        {
            get { return cloak.NMSDeliveryMode; }
            set { cloak.NMSDeliveryMode = value; }
        }

        public IDestination NMSDestination
        {
            get { return cloak.NMSDestination; }
            set { cloak.NMSDestination = value; }
        }

        public string NMSMessageId
        {
            get { return cloak.NMSMessageId; }
            set { cloak.NMSMessageId = value; }
        }

        public MsgPriority NMSPriority
        {
            get { return cloak.NMSPriority; }
            set { cloak.NMSPriority = value; }
        }

        public bool NMSRedelivered
        {
            get { return cloak.NMSRedelivered; }
            set { cloak.NMSRedelivered = value; }
        }

        public IDestination NMSReplyTo
        {
            get { return cloak.NMSReplyTo; }
            set { cloak.NMSReplyTo = value; }
        }

        public DateTime NMSTimestamp
        {
            get { return cloak.NMSTimestamp; }
            set { cloak.NMSTimestamp = value; }
        }

        public TimeSpan NMSTimeToLive
        {
            get { return cloak.NMSTimeToLive; }
            set { cloak.NMSTimeToLive = value; }
        }

        public string NMSType
        {
            get { return cloak.NMSType; }
            set { cloak.NMSType = value; }
        }

        public IPrimitiveMap Properties
        {
            get
            {
                if(propertyHelper == null)
                {
                    propertyHelper = new NMSMessagePropertyInterceptor(this, cloak.Properties);
                }
                return propertyHelper;
            }
        }

        #endregion

        #region IMessage Methods

        public virtual void Acknowledge()
        {
            cloak.Acknowledge();
        }

        public virtual void ClearBody()
        {
            cloak.ClearBody();
        }

        public void ClearProperties()
        {
            propertyHelper.Clear();
        }

        #endregion

        #region Internal Methods

        internal IMessageCloak GetMessageCloak()
        {
            return cloak;
        }

        internal virtual Message Copy()
        {
            return new Message(this.cloak.Copy());
        }

        protected virtual void CopyInto(Message other)
        {

        }

        #endregion

        public override string ToString()
        {
            return base.ToString() + ":\n Impl Type: " + cloak.ToString();
        }

    }

    internal class MessageAcknowledgementHandler
    {
        
        private MessageConsumer consumer;
        private Session session;
        private Message message;
        
        private AckType? atype = null;

        public MessageAcknowledgementHandler(MessageConsumer mc, Message msg)
        {
            consumer = mc;
            session = consumer.Session;
            message = msg;
            
        }

        public AckType AcknowledgementType
        {
            get
            {
                return atype ?? MessageSupport.DEFAULT_ACK_TYPE;
            }
            set
            {
                atype = value;
            }
        }

        public void ClearAckType()
        {
            atype = null;
        }

        public bool IsAckTypeSet
        {
            get => atype != null;
        }

        public void Acknowledge()
        {
            
            if (session.AcknowledgementMode.Equals(AcknowledgementMode.IndividualAcknowledge))
            {
                consumer.AcknowledgeMessage(message, AcknowledgementType);
            }
            else // Session Ackmode  AcknowledgementMode.ClientAcknowledge
            {
                session.Acknowledge(AcknowledgementType);
            }
        }

    }

}
