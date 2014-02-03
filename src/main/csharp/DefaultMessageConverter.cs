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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Apache.NMS.Util;
using Org.Apache.Qpid.Messaging;

namespace Apache.NMS.Amqp
{
    public enum NMSMessageType
    {
        BaseMessage,
        TextMessage,
        BytesMessage,
        ObjectMessage,
        MapMessage,
        StreamMessage
    }

    public class DefaultMessageConverter : IMessageConverter
    {
        #region IMessageConverter Members
        // NMS Message                       AMQP Message
        // ================================  =================
        // string          NMSCorrelationID  string     CorrelationId
        // MsgDeliveryMode NMSDeliveryMode   bool       Durable
        // IDestination    NMSDestination
        // string          MNSMessageId      string     MessageId
        // MsgPriority     NMSPriority       byte       Priority
        // bool            NMSRedelivered    bool       Redelivered
        // IDestination    NMSReplyTo        Address    ReplyTo
        // DateTime        NMSTimestamp
        // TimeSpan        NMSTimeToLive     Duration   Ttl
        // string          NMSType           string     ContentType
        // IPrimitiveMap   Properties        Dictionary Properties
        //                                   string     Subject
        //                                   string     UserId
        //
        public virtual Message ToAmqpMessage(IMessage message)
        {
            Message amqpMessage = CreateAmqpMessage(message);

            if (null != message.NMSCorrelationID)
            {
                amqpMessage.CorrelationId = message.NMSCorrelationID;
            }
            amqpMessage.Durable = (message.NMSDeliveryMode == MsgDeliveryMode.Persistent);
            if (null != message.NMSMessageId)
            {
                amqpMessage.MessageId = message.NMSMessageId;
            }
            amqpMessage.Priority = ToAmqpMessagePriority(message.NMSPriority);
            amqpMessage.Redelivered = message.NMSRedelivered;
            if (null != message.NMSReplyTo)
            {
                amqpMessage.ReplyTo = ToAmqpAddress(message.NMSReplyTo);
            }

            if (message.NMSTimeToLive != TimeSpan.Zero)
            {
                amqpMessage.Ttl = ToQpidDuration(message.NMSTimeToLive);
            }

            if (null != message.NMSType)
            {
                amqpMessage.ContentType = message.NMSType;
            }

            amqpMessage.Properties = FromNmsPrimitiveMap(message.Properties);

            // TODO: NMSDestination, Amqp.Subect, Amqp.UserId
            return amqpMessage;
        }

        //
        public virtual IMessage ToNmsMessage(Message message)
        {
            BaseMessage answer = CreateNmsMessage(message);

            try
            {
                answer.NMSCorrelationID = message.CorrelationId;
                answer.NMSDeliveryMode = (message.Durable ? MsgDeliveryMode.Persistent : MsgDeliveryMode.NonPersistent);
                answer.NMSMessageId = message.MessageId;
                answer.NMSPriority = ToNmsPriority(message.Priority);
                answer.NMSRedelivered = message.Redelivered;
                answer.NMSReplyTo = ToNmsDestination(message.ReplyTo);
                answer.NMSTimeToLive = ToNMSTimespan(message.Ttl);
                answer.NMSType = message.ContentType;
                SetNmsPrimitiveMap(answer.Properties, message.Properties);

                // TODO: NMSDestination, NMSTimestamp, Properties
            }
            catch (InvalidOperationException)
            {
            }

            return answer;
        }
        #endregion

        #region MessagePriority Methods
        //
        private static byte ToAmqpMessagePriority(MsgPriority msgPriority)
        {
            return (byte)msgPriority;
        }

        //
        private static MsgPriority ToNmsPriority(byte qpidMsgPriority)
        {
            if (qpidMsgPriority > (byte)MsgPriority.Highest)
            {
                return MsgPriority.Highest;
            }
            return (MsgPriority)qpidMsgPriority;
        }
        #endregion

        #region Duration Methods
        //
        private static Duration ToQpidDuration(TimeSpan timespan)
        {
            if (timespan.TotalMilliseconds <= 0)
            {
                Duration result = DurationConstants.IMMEDIATE;
                return result;
            }
            else if (timespan.TotalMilliseconds > (Double)DurationConstants.FORVER.Milliseconds)
            {
                Duration result = DurationConstants.FORVER;
                return result;
            }
            else
            {
                Duration result = new Duration((UInt64)timespan.TotalMilliseconds);
                return result;
            }
        }

        //
        private static TimeSpan ToNMSTimespan(Duration duration)
        {
            if (duration.Milliseconds > Int64.MaxValue)
            {
                TimeSpan result = new TimeSpan(Int64.MaxValue);
                return result;
            }
            else
            {
                TimeSpan result = new TimeSpan((Int64)duration.Milliseconds);
                return result;
            }
        }
        #endregion

        #region MessageBody Conversion Methods
        protected virtual Message CreateAmqpMessage(IMessage message)
        {
            if (message is TextMessage)
            {
                TextMessage textMessage = message as TextMessage;
                Message result = new Message(textMessage.Text);
                return result;
            }
            else if (message is BytesMessage)
            {
                BytesMessage bytesMessage = message as BytesMessage;
                Message result = new Message(bytesMessage.Content, 0, bytesMessage.Content.Length);
                return result;
            }
            else if (message is ObjectMessage)
            {
                ObjectMessage objectMessage = message as ObjectMessage;
                Message result = new Message(objectMessage.Body);
                return result;
            }
            else if (message is MapMessage)
            {
                MapMessage mapMessage = message as MapMessage;
                PrimitiveMap mapBody = mapMessage.Body as PrimitiveMap;
                Dictionary<string, object> dict = FromNmsPrimitiveMap(mapBody);

                Message result = new Message(dict);
                return result;
            }
            else if (message is StreamMessage)
            {
                StreamMessage streamMessage = message as StreamMessage;
                Message result = new Message(streamMessage.Content, 0, streamMessage.Content.Length);
                return result;
            }
            else if (message is BaseMessage)
            {
                Message result = new Message();
                return result;
            }
            else
            {
                throw new Exception("unhandled message type");
            }
        }

        protected virtual BaseMessage CreateNmsMessage(Message message)
        {
            BaseMessage result = null;

            if ("amqp/map" == message.ContentType)
            {
                Dictionary<string, object> dict = new Dictionary<string,object>();
                message.GetContent(dict);
                PrimitiveMap bodyMap = new PrimitiveMap();
                SetNmsPrimitiveMap(bodyMap, dict);
                MapMessage mapMessage = new MapMessage();
                mapMessage.Body = bodyMap;
                result = mapMessage;
            }
            else if ("amqp/list" == message.ContentType)
            {
                // TODO: Return list message 
            }
            else
            {
                TextMessage textMessage = new TextMessage();
                textMessage.Text = message.GetContent();
                result = textMessage;
            }

            return result;
        }
        #endregion

        #region Address/Destination Conversion Methods
        public Address ToAmqpAddress(IDestination destination)
        {
            if (null == destination)
            {
                return null;
            }

            return new Address((destination as Destination).Path);
        }

        protected virtual IDestination ToNmsDestination(Address destinationQueue)
        {
            if (null == destinationQueue)
            {
                return null;
            }

            return new Queue(destinationQueue.ToString());
        }
        #endregion

        #region PrimitiveMap Conversion Methods

        //
        public void SetNmsPrimitiveMap(IPrimitiveMap map, Dictionary<string, object> dict)
        {
            
            // TODO: lock?
            map.Clear();
            foreach (System.Collections.Generic.KeyValuePair
                    <string, object> kvp in dict)
            {
                map[kvp.Key] = kvp.Value;
            }
        }

        //
        public Dictionary<string, object> FromNmsPrimitiveMap(IPrimitiveMap pm)
        {
            Dictionary<string, object> dict = new Dictionary<string,object>();

            // TODO: lock?
            ICollection keys = pm.Keys;
            foreach (object key in keys)
            {
                dict.Add(key.ToString(), pm[key.ToString()]);
            }
            return dict;
        }
        #endregion
    }
}
