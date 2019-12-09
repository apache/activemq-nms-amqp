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

namespace Apache.NMS.AMQP.Message.Facade
{
    public interface INmsMessageFacade
    {
        NmsMessage AsMessage();
        void ClearBody();
        int DeliveryCount { get; set; }
        int RedeliveryCount { get; set; }
        void OnSend(TimeSpan producerTtl);
        string NMSMessageId { get; set; }
        IPrimitiveMap Properties { get; }
        string NMSCorrelationID { get; set; }
        IDestination NMSDestination { get; set; }
        TimeSpan NMSTimeToLive { get; set; }
        MsgPriority NMSPriority { get; set; }
        bool NMSRedelivered { get; set; }
        IDestination NMSReplyTo { get; set; }
        DateTime NMSTimestamp { get; set; }
        string NMSType { get; set; }
        string GroupId { get; set; }
        uint GroupSequence { get; set; }
        DateTime? Expiration { get; set; }
        sbyte? JmsMsgType { get; }
        
        /// <summary>
        /// True if this message is tagged as being persistent
        /// </summary>
        bool IsPersistent { get; set; }

        /// <summary>
        /// Gets and sets the underlying providers message ID object for this message if one exists, null otherwise.
        /// In the case the returned value is a string, it is not defined whether the NMS mandated 'ID:' prefix will be present.
        /// </summary>
        object ProviderMessageIdObject { get; set; }

        INmsMessageFacade Copy();
    }
}