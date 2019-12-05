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
using Apache.NMS.AMQP;
using Apache.NMS.AMQP.Message;
using Apache.NMS.AMQP.Message.Facade;
using Apache.NMS.Util;

namespace NMS.AMQP.Test.Message.Facade
{
    [Serializable]
    public class NmsTestMessageFacade : INmsMessageFacade
    {
        public virtual NmsMessage AsMessage()
        {
            return new NmsMessage(this);
        }

        public virtual void ClearBody()
        {
        }

        public int DeliveryCount { get; set; }

        public int RedeliveryCount { get; set; }

        public void OnSend(TimeSpan producerTtl)
        {
            
        }

        public string NMSMessageId
        {
            get => ProviderMessageIdObject as string;
            set => ProviderMessageIdObject = value;
        }

        public IPrimitiveMap Properties { get; } = new PrimitiveMap();
        public string NMSCorrelationID { get; set; }
        public IDestination NMSDestination { get; set; }
        public TimeSpan NMSTimeToLive { get; set; }
        public MsgPriority NMSPriority { get; set; }
        public bool NMSRedelivered { get; set; }
        public IDestination NMSReplyTo { get; set; }
        public DateTime NMSTimestamp { get; set; }
        public string NMSType { get; set; }
        public string GroupId { get; set; }
        public uint GroupSequence { get; set; }
        public DateTime? Expiration { get; set; }
        public sbyte? JmsMsgType { get; }
        public bool IsPersistent { get; set; }
        public object ProviderMessageIdObject { get; set; }

        public INmsMessageFacade Copy()
        {
            return null;
        }
    }
}