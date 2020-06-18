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
using System.Threading.Tasks;
using Apache.NMS.AMQP.Message;
using Apache.NMS.Util;

namespace Apache.NMS.AMQP
{
    public class NmsProducer : INMSProducer
    {
        
        private readonly ISession session;
        private readonly NmsMessageProducer producer;
        
        // Message Headers
        private String correlationId;
        private String type;
        private IDestination replyTo;

        // Message Properties
        private readonly IPrimitiveMap messageProperties = new PrimitiveMap();

        /**
         * Create a new JMSProducer instance.
         *
         * The producer is backed by the given Session object and uses the shared MessageProducer
         * instance to send all of its messages.
         *
         * @param session
         *      The Session that created this JMSProducer
         * @param producer
         *      The shared MessageProducer owned by the parent Session.
         */
        public NmsProducer(ISession session, NmsMessageProducer producer) {
            this.session = session;
            this.producer = producer;
        }

        
        public void Dispose()
        {
            producer.Dispose();
        }

        public INMSProducer Send(IDestination destination, IMessage message)  {

            if (message == null) {
                throw new MessageFormatException("Message must not be null");
            }
            
            NmsMessageTransformation.CopyMap(messageProperties, message.Properties);
            
            if (correlationId != null) {
                message.NMSCorrelationID = correlationId;
            }
            if (type != null) {
                message.NMSType = type;
            }
            if (replyTo != null) {
                message.NMSReplyTo = replyTo;
            }
            
            producer.Send(destination, message);
            
            return this;
        }

        public INMSProducer Send(IDestination destination, string body)
        {
            return Send(destination, CreateTextMessage(body));
        }

        public INMSProducer Send(IDestination destination, IPrimitiveMap body)
        {
            IMapMessage message = CreateMapMessage();
            NmsMessageTransformation.CopyMap(body, message.Body);
            return Send(destination, message);
        }

        public INMSProducer Send(IDestination destination, byte[] body)
        {
            return Send(destination, CreateBytesMessage(body));
        }

        public INMSProducer Send(IDestination destination, object body)
        {
            return Send(destination, CreateObjectMessage(body));
        }

        public async Task<INMSProducer> SendAsync(IDestination destination, IMessage message)
        {
            if (message == null) {
                throw new MessageFormatException("Message must not be null");
            }

            NmsMessageTransformation.CopyMap(messageProperties, message.Properties);
            
            if (correlationId != null) {
                message.NMSCorrelationID = correlationId;
            }
            if (type != null) {
                message.NMSType = type;
            }
            if (replyTo != null) {
                message.NMSReplyTo = replyTo;
            }

            await producer.SendAsync(destination, message);
            return this;
        }

        public Task<INMSProducer> SendAsync(IDestination destination, string body)
        {
            return SendAsync(destination, CreateTextMessage(body));
        }

        public Task<INMSProducer> SendAsync(IDestination destination, IPrimitiveMap body)
        {
            IMapMessage message = CreateMapMessage();
            NmsMessageTransformation.CopyMap(body, message.Body);
            return SendAsync(destination, message);
        }

        public Task<INMSProducer> SendAsync(IDestination destination, byte[] body)
        {
            return SendAsync(destination, CreateBytesMessage(body));
        }

        public Task<INMSProducer> SendAsync(IDestination destination, object body)
        {
            return SendAsync(destination, CreateObjectMessage(body));
        }

        public INMSProducer ClearProperties()
        {
            messageProperties.Clear();
            return this;
        }


        public IMessage CreateMessage()
        {
            return session.CreateMessage();
        }

        public ITextMessage CreateTextMessage()
        {
            return session.CreateTextMessage();
        }

        public ITextMessage CreateTextMessage(string text)
        {
            return session.CreateTextMessage(text);
        }

        public IMapMessage CreateMapMessage()
        {
            return session.CreateMapMessage();
        }

        public IObjectMessage CreateObjectMessage(object body)
        {
            return session.CreateObjectMessage(body);
        }

        public IBytesMessage CreateBytesMessage()
        {
            return session.CreateBytesMessage();
        }

        public IBytesMessage CreateBytesMessage(byte[] body)
        {
            return session.CreateBytesMessage(body);
        }

        public IStreamMessage CreateStreamMessage()
        {
            return session.CreateStreamMessage();
        }

        public void Close()
        {
            producer.Close();
        }


        public string NMSCorrelationID
        {
            get => correlationId;
            set => correlationId = value;
        }
        
        public INMSProducer SetNMSCorrelationID(string correlationID)
        {
            NMSCorrelationID = correlationID;
            return this;
        }


        public IDestination NMSReplyTo
        {
            get => replyTo;
            set => replyTo = value;
        }

        public INMSProducer SetNMSReplyTo(IDestination replyTo)
        {
            NMSReplyTo = replyTo;
            return this;
        }
        
        public string NMSType
        {
            get => type;
            set => type = value;
        }

        public INMSProducer SetNMSType(string type)
        {
            NMSType = type;
            return this;
        }

        public MsgDeliveryMode DeliveryMode
        {
            get => producer.DeliveryMode;
            set => producer.DeliveryMode = value;
        }
        
        public INMSProducer SetDeliveryMode(MsgDeliveryMode deliveryMode)
        {
            DeliveryMode = deliveryMode;
            return this;
        }

        public TimeSpan TimeToLive
        {
            get => producer.TimeToLive;
            set => producer.TimeToLive = value;
        }

        public INMSProducer SetTimeToLive(TimeSpan timeToLive)
        {
            TimeToLive = timeToLive;
            return this;
        }

        public TimeSpan RequestTimeout
        {
            get => producer.RequestTimeout;
            set => producer.RequestTimeout = value;
        }

        public MsgPriority Priority
        {
            get => producer.Priority;
            set => producer.Priority = value;
        }
        
        public INMSProducer SetPriority(MsgPriority priority)
        {
            Priority = priority;
            return this;
        }

        public bool DisableMessageID
        {
            get => producer.DisableMessageID;
            set => producer.DisableMessageID = value;
        }
        
        public INMSProducer SetDisableMessageID(bool value)
        {
            DisableMessageID = value;
            return this;
        }

        public bool DisableMessageTimestamp
        {
            get => producer.DisableMessageTimestamp;
            set => producer.DisableMessageTimestamp = value;
        }

        public INMSProducer SetDisableMessageTimestamp(bool value)
        {
            DisableMessageTimestamp = value;
            return this;
        }

        public TimeSpan DeliveryDelay
        {
            get => producer.DeliveryDelay;
            set => producer.DeliveryDelay = value;
        }
        
        public INMSProducer SetDeliveryDelay(TimeSpan deliveryDelay)
        {
            DeliveryDelay = deliveryDelay;
            return this;
        }
        
        public IPrimitiveMap Properties => messageProperties;

        public INMSProducer SetProperty(string name, bool value)
        {
            messageProperties.SetBool(name, value);
            return this;
        }

        public INMSProducer SetProperty(string name, byte value)
        {
            messageProperties.SetByte(name, value);
            return this;
        }

        public INMSProducer SetProperty(string name, double value)
        {
            messageProperties.SetDouble(name, value);
            return this;
        }

        public INMSProducer SetProperty(string name, float value)
        {
            messageProperties.SetFloat(name, value);
            return this;
        }

        public INMSProducer SetProperty(string name, int value)
        {
            messageProperties.SetInt(name, value);
            return this;
        }

        public INMSProducer SetProperty(string name, long value)
        {
            messageProperties.SetLong(name, value);
            return this;
        }

        public INMSProducer SetProperty(string name, short value)
        {
            messageProperties.SetShort(name, value);
            return this;
        }

        public INMSProducer SetProperty(string name, char value)
        {
            messageProperties.SetChar(name, value);
            return this;
        }

        public INMSProducer SetProperty(string name, string value)
        {
            messageProperties.SetString(name, value);
            return this;
        }

        public INMSProducer SetProperty(string name, byte[] value)
        {
            messageProperties.SetBytes(name, value);
            return this;
        }

        public INMSProducer SetProperty(string name, IList value)
        {
            messageProperties.SetList(name, value);
            return this;
        }

        public INMSProducer SetProperty(string name, IDictionary value)
        {
            messageProperties.SetDictionary(name, value);
            return this;
        }

        public ProducerTransformerDelegate ProducerTransformer
        {
            get => producer.ProducerTransformer; 
            set => producer.ProducerTransformer = value;
        }

    }
}