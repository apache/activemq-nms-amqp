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
using Apache.NMS.Util;

namespace Apache.NMS.Amqp
{
    public delegate void AcknowledgeHandler(BaseMessage baseMessage);

    public class BaseMessage : IMessage, ICloneable
    {
        private PrimitiveMap propertiesMap = new PrimitiveMap();
        private IDestination destination;
        private string correlationId;
        private TimeSpan timeToLive;
        private string messageId;
        private MsgDeliveryMode deliveryMode = MsgDeliveryMode.NonPersistent;
        private MsgPriority priority = MsgPriority.Normal;
        private Destination replyTo;
        private byte[] content;
        private string type;
        private event AcknowledgeHandler Acknowledger;
        private DateTime timestamp = new DateTime();
        private bool readOnlyMsgBody = false;

        public BaseMessage() { }

        public BaseMessage(BaseMessage copy)
        {
            this.propertiesMap = copy.propertiesMap;
            this.destination = copy.destination;
            this.correlationId = copy.correlationId;
            this.timeToLive = copy.timeToLive;
            this.messageId = copy.messageId;
            this.deliveryMode = copy.deliveryMode;
            this.priority = copy.priority;
            this.replyTo = copy.replyTo;
            this.content = copy.content;
            this.type = copy.type;
            this.Acknowledger = copy.Acknowledger;
            this.timestamp = copy.timestamp;
            this.readOnlyMsgBody = copy.readOnlyMsgBody;
        }

        ///
        /// <summary>
        ///  Clone this object and return a new instance that the caller now owns.
        /// </summary>
        ///
        public virtual Object Clone()
        {
            return this.MemberwiseClone();
        }


        public override bool Equals(object obj)
        {
            // If parameter is null return false.
            if (obj == null)
            {
                return false;
            }

            // If parameter cannot be cast to BaseMessage return false.
            BaseMessage p = obj as BaseMessage;
            if ((System.Object)p == null)
            {
                return false;
            }

            if (propertiesMap == null ^ p.propertiesMap == null)
            {
                return false;
            }
            if (propertiesMap != null)
            {
                if (!propertiesMap.ToString().Equals(p.propertiesMap.ToString()))
                {
                    return false;
                }
            }

            if (destination == null ^ p.destination == null)
            {
                return false;
            }
            if (destination != null)
            {
                if (!destination.ToString().Equals(p.destination.ToString()))
                {
                    return false;
                }
            }

            if (correlationId == null ^ p.correlationId == null)
            {
                return false;
            }
            if (correlationId != null)
            {
                if (!correlationId.Equals(p.correlationId))
                {
                    return false;
                }
            }

            if (timeToLive == null ^ p.timeToLive == null)
            {
                return false;
            }
            if (timeToLive != null)
            {
                if (!timeToLive.ToString().Equals(p.timeToLive.ToString()))
                {
                    return false;
                }
            }

            if (messageId == null ^ p.messageId == null)
            {
                return false;
            }
            if (messageId != null)
            {
                if (!messageId.Equals(p.messageId))
                {
                    return false;
                }
            }

            if (deliveryMode != p.deliveryMode)
            {
                return false;
            }

            if (priority != p.priority)
            {
                return false;
            }

            if (replyTo == null ^ p.replyTo == null)
            {
                return false;
            }
            if (replyTo != null)
            {
                if (!replyTo.ToString().Equals(p.replyTo.ToString()))
                {
                    return false;
                }
            }

            if (content == null ^ p.content == null)
            {
                return false;
            }
            if (content != null)
            {
                if (!content.ToString().Equals(p.content.ToString()))
                {
                    return false;
                }
            }

            if (type == null ^ p.type == null)
            {
                return false;
            }
            if (type != null)
            {
                if (!type.Equals(p.type))
                {
                    return false;
                }
            }

            if (timestamp == null ^ p.timestamp == null)
            {
                return false;
            }
            if (timestamp != null)
            {
                if (!timestamp.ToString().Equals(p.timestamp.ToString()))
                {
                    return false;
                }
            }

            if (readOnlyMsgBody != p.readOnlyMsgBody)
            {
                return false;
            }

            return true;
        }

        public bool ReadOnlyBody
        {
            get { return readOnlyMsgBody; }
            set { readOnlyMsgBody = value; }
        }

        // IMessage interface

        public void Acknowledge()
        {
            if(null != Acknowledger)
            {
                Acknowledger(this);
            }
        }

        /// <summary>
        /// Clears out the message body. Clearing a message's body does not clear its header
        /// values or property entries.
        ///
        /// If this message body was read-only, calling this method leaves the message body in
        /// the same state as an empty body in a newly created message.
        /// </summary>
        public virtual void ClearBody()
        {
            this.Content = null;
            this.readOnlyMsgBody = false;
        }

        /// <summary>
        /// Clears a message's properties.
        ///
        /// The message's header fields and body are not cleared.
        /// </summary>
        public virtual void ClearProperties()
        {
            propertiesMap.Clear();
        }

        // Properties

        public IPrimitiveMap Properties
        {
            get { return propertiesMap; }
        }


        // NMS headers

        /// <summary>
        /// The correlation ID used to correlate messages with conversations or long running business processes
        /// </summary>
        public string NMSCorrelationID
        {
            get { return correlationId; }
            set { correlationId = value; }
        }

        /// <summary>
        /// The destination of the message
        /// </summary>
        public IDestination NMSDestination
        {
            get { return destination; }
            set { destination = value; }
        }

        /// <summary>
        /// The time in milliseconds that this message should expire in
        /// </summary>
        public TimeSpan NMSTimeToLive
        {
            get { return timeToLive; }
            set { timeToLive = value; }
        }

        /// <summary>
        /// The message ID which is set by the provider
        /// </summary>
        public string NMSMessageId
        {
            get { return messageId; }
            set { messageId = value; }
        }

        /// <summary>
        /// Whether or not this message is persistent
        /// </summary>
        public MsgDeliveryMode NMSDeliveryMode
        {
            get { return deliveryMode; }
            set { deliveryMode = value; }
        }

        /// <summary>
        /// The Priority on this message
        /// </summary>
        public MsgPriority NMSPriority
        {
            get { return priority; }
            set { priority = value; }
        }

        /// <summary>
        /// Returns true if this message has been redelivered to this or another consumer before being acknowledged successfully.
        /// </summary>
        public bool NMSRedelivered
        {
            get { return false; }
            set { }
        }


        /// <summary>
        /// The destination that the consumer of this message should send replies to
        /// </summary>
        public IDestination NMSReplyTo
        {
            get { return replyTo; }
            set { replyTo = (Destination) value; }
        }


        /// <summary>
        /// The timestamp the broker added to the message
        /// </summary>
        public DateTime NMSTimestamp
        {
            get { return timestamp; }
            set { timestamp = value; }
        }

        public byte[] Content
        {
            get { return content; }
            set { this.content = value; }
        }

        /// <summary>
        /// The type name of this message
        /// </summary>
        public string NMSType
        {
            get { return type; }
            set { type = value; }
        }


        public object GetObjectProperty(string name)
        {
            return null;
        }

        public void SetObjectProperty(string name, object value)
        {
        }

        protected void FailIfReadOnlyBody()
        {
            if(ReadOnlyBody == true)
            {
                throw new MessageNotWriteableException("Message is in Read-Only mode.");
            }
        }

        protected void FailIfWriteOnlyBody()
        {
            if( ReadOnlyBody == false )
            {
                throw new MessageNotReadableException("Message is in Write-Only mode.");
            }
        }
    }
}

