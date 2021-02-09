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
using Apache.NMS;
using Apache.NMS.AMQP.Message;
using NMS.AMQP.Test.Message.Facade;

namespace NMS.AMQP.Test.Message.Foreign
{
    public class ForeignNmsMessage : IMessage
    {
        readonly NmsMessage message = new TestMessageFactory().CreateMessage();

        public void Acknowledge()
        {
            message.Acknowledge();
        }

        public void ClearBody()
        {
            message.ClearBody();
        }

        public void ClearProperties()
        {
            message.ClearProperties();
        }

        public T Body<T>()
        {
            return message.Body<T>();
        }

        public bool IsBodyAssignableTo(Type type)
        {
            return message.IsBodyAssignableTo(type);
        }

        public IPrimitiveMap Properties => message.Properties;

        public string NMSCorrelationID
        {
            get => message.NMSCorrelationID;
            set => message.NMSCorrelationID = value;
        }

        public IDestination NMSDestination
        {
            get => message.NMSDestination;
            set => message.NMSDestination = value;
        }

        public TimeSpan NMSTimeToLive
        {
            get => message.NMSTimeToLive;
            set => message.NMSTimeToLive = value;
        }

        public string NMSMessageId
        {
            get => message.NMSMessageId;
            set => message.NMSMessageId = value;
        }

        public MsgDeliveryMode NMSDeliveryMode
        {
            get => message.NMSDeliveryMode;
            set => message.NMSDeliveryMode = value;
        }

        public MsgPriority NMSPriority
        {
            get => message.NMSPriority;
            set => message.NMSPriority = value;
        }

        public bool NMSRedelivered
        {
            get => message.NMSRedelivered;
            set => message.NMSRedelivered = value;
        }

        public IDestination NMSReplyTo
        {
            get => message.NMSReplyTo;
            set => message.NMSReplyTo = value;
        }

        public DateTime NMSTimestamp
        {
            get => message.NMSTimestamp;
            set => message.NMSTimestamp = value;
        }

        public string NMSType
        {
            get => message.NMSType;
            set => message.NMSType = value;
        }

        public DateTime NMSDeliveryTime
        {
            get => message.NMSDeliveryTime;
            set => message.NMSDeliveryTime = value;
        }
    }
}