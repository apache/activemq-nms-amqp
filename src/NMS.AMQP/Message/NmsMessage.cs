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
using Apache.NMS.AMQP.Message.Facade;
using Apache.NMS.Util;

namespace Apache.NMS.AMQP.Message
{
    public class NmsMessage : IMessage
    {
        private MessagePropertyIntercepter properties;
        private bool readOnlyProperties;

        public NmsMessage(INmsMessageFacade facade)
        {
            Facade = facade;
        }

        public INmsMessageFacade Facade { get; }

        public IPrimitiveMap Properties => properties ?? (properties = new MessagePropertyIntercepter(this, Facade.Properties, IsReadOnlyProperties));

        public string NMSCorrelationID
        {
            get => Facade.NMSCorrelationID;
            set => Facade.NMSCorrelationID = value;
        }

        public IDestination NMSDestination
        {
            get => Facade.NMSDestination;
            set => Facade.NMSDestination = value;
        }

        public TimeSpan NMSTimeToLive
        {
            get => Facade.NMSTimeToLive;
            set => Facade.NMSTimeToLive = value;
        }

        public string NMSMessageId
        {
            get => Facade.NMSMessageId;
            set => Facade.NMSMessageId = value;
        }

        public MsgDeliveryMode NMSDeliveryMode
        {
            get => Facade.IsPersistent ? MsgDeliveryMode.Persistent : MsgDeliveryMode.NonPersistent;
            set
            {
                CheckReadOnly();
                switch (value)
                {
                    case MsgDeliveryMode.Persistent:
                        Facade.IsPersistent = true;
                        break;
                    case MsgDeliveryMode.NonPersistent:
                        Facade.IsPersistent = false;
                        break;
                    default:
                        throw new NMSException($"Invalid DeliveryMode specified: {value}");
                }
            }
        }

        public MsgPriority NMSPriority
        {
            get => Facade.NMSPriority;
            set => Facade.NMSPriority = value;
        }

        public bool NMSRedelivered
        {
            get => Facade.NMSRedelivered;
            set => Facade.NMSRedelivered = value;
        }

        public IDestination NMSReplyTo
        {
            get => Facade.NMSReplyTo;
            set => Facade.NMSReplyTo = value;
        }

        public DateTime NMSTimestamp
        {
            get => Facade.NMSTimestamp;
            set => Facade.NMSTimestamp = value;
        }

        public string NMSType
        {
            get => Facade.NMSType;
            set => Facade.NMSType = value;
        }

        public string NMSXGroupId
        {
            get => Facade.GroupId;
            set => Facade.GroupId = value;
        }

        public int NMSXGroupSeq
        {
            get => (int) Facade.GroupSequence;
            set => Facade.GroupSequence = (uint) value;
        }

        public NmsAcknowledgeCallback NmsAcknowledgeCallback { get; set; }

        public virtual bool IsReadOnly { get; set; }

        public bool IsReadOnlyBody { get; set; }

        public bool IsReadOnlyProperties
        {
            get => readOnlyProperties;
            set
            {
                readOnlyProperties = value;
                if (properties != null)
                {
                    properties.ReadOnly = value;
                }
            }
        }

        public void Acknowledge()
        {
            if (NmsAcknowledgeCallback != null)
            {
                try
                {
                    NmsAcknowledgeCallback.Acknowledge();
                    NmsAcknowledgeCallback = null;
                }
                catch (Exception e)
                {
                    throw NMSExceptionSupport.Create(e);
                }
            }
        }

        public virtual void ClearBody()
        {
            CheckReadOnly();
            IsReadOnlyBody = false;
            Facade.ClearBody();
        }

        public void ClearProperties()
        {
            CheckReadOnly();
            IsReadOnlyProperties = false;
            Properties.Clear();
        }

        public virtual void OnSend(TimeSpan producerTtl)
        {
            IsReadOnly = true;
            Facade.OnSend(producerTtl);
        }

        public void OnDispatch()
        {
            IsReadOnly = false;
            IsReadOnlyBody = true;
            IsReadOnlyProperties = true;
        }

        public override string ToString()
        {
            return $"NmsMessage {{ {Facade} }}";
        }

        protected bool Equals(NmsMessage other)
        {
            if (other.NMSMessageId == null)
                return false;

            return string.Equals(NMSMessageId, other.NMSMessageId);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((NmsMessage) obj);
        }

        public override int GetHashCode()
        {
            return (NMSMessageId != null ? NMSMessageId.GetHashCode() : base.GetHashCode());
        }

        //----- State validation methods -----------------------------------------//

        protected void CheckReadOnly()
        {
            if (IsReadOnly)
            {
                throw new MessageNotReadableException("Message is currently read-only");
            }
        }

        protected void CheckWriteOnlyBody()
        {
            if (!IsReadOnlyBody)
            {
                throw new MessageNotReadableException("Message body is write-only");
            }
        }

        protected void CheckReadOnlyBody()
        {
            if (IsReadOnly || IsReadOnlyBody)
            {
                throw new MessageNotWriteableException("Message body is read-only");
            }
        }

        public bool IsExpired()
        {
            DateTime? expireTime = Facade.Expiration;
            return expireTime != null && DateTime.UtcNow > expireTime;
        }

        public virtual NmsMessage Copy()
        {
            NmsMessage copy = new NmsMessage(Facade.Copy());
            CopyInto(copy);
            return copy;
        }

        protected void CopyInto(NmsMessage target)
        {
            target.IsReadOnly = IsReadOnly;
            target.IsReadOnlyBody = IsReadOnlyBody;
            target.IsReadOnlyProperties = IsReadOnlyProperties;
            target.NmsAcknowledgeCallback = NmsAcknowledgeCallback;
        }
    }
}