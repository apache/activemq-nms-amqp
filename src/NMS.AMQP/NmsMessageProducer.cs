﻿/*
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
using System.Threading.Tasks;
using Apache.NMS.AMQP.Message;
using Apache.NMS.AMQP.Meta;
using Apache.NMS.AMQP.Provider;
using Apache.NMS.AMQP.Util;
using Apache.NMS.AMQP.Util.Synchronization;

namespace Apache.NMS.AMQP
{
    public class NmsMessageProducer : IMessageProducer
    {
        private readonly NmsSession session;
        private readonly AtomicBool closed = new AtomicBool();
        private readonly AtomicLong messageSequence = new AtomicLong();

        private Exception failureCause;
        private TimeSpan deliveryDelay = TimeSpan.Zero;
        private MsgDeliveryMode deliveryMode = MsgDeliveryMode.Persistent;
        private TimeSpan timeToLive = NMSConstants.defaultTimeToLive;
        private TimeSpan requestTimeout;
        private MsgPriority priority = NMSConstants.defaultPriority - 1;
        private bool disableMessageId;
        private bool disableMessageTimestamp;

        internal NmsMessageProducer(NmsProducerId producerId, NmsSession session, IDestination destination)
        {
            this.session = session;
            Info = new NmsProducerInfo(producerId)
            {
                Destination = destination
            };
        }

        internal async  Task Init()
        {
            await session.Connection.CreateResource(Info).Await();

            session.Add(this);
        }
        
        public NmsProducerId ProducerId => Info.Id;
        public NmsProducerInfo Info { get; }
        public INmsMessageIdBuilder MessageIdBuilder { get; } = new DefaultMessageIdBuilder();

        public void Dispose()
        {
            try
            {
                Close();
            }
            catch (Exception ex)
            {
                Tracer.DebugFormat("Caught exception while disposing {0} {1}. Exception {2}", GetType().Name, Info, ex);
            }
        }

        public void Send(IMessage message)
        {
            Send(message, DeliveryMode, Priority, TimeToLive);
        }

        public void Send(IMessage message, MsgDeliveryMode deliveryMode, MsgPriority priority, TimeSpan timeToLive)
        {
            Send(Info.Destination, message, deliveryMode, priority, timeToLive);
        }

        public void Send(IDestination destination, IMessage message)
        {
            Send(destination, message, DeliveryMode, Priority, TimeToLive);
        }

        public void Send(IDestination destination, IMessage message, MsgDeliveryMode deliveryMode, MsgPriority priority, TimeSpan timeToLive)
        {
            CheckClosed();
            session.Send(this, destination, message, deliveryMode, priority, timeToLive, DisableMessageID, DisableMessageTimestamp, deliveryDelay);
        }

        public Task SendAsync(IMessage message)
        {
            return SendAsync(Info.Destination, message, deliveryMode, priority, timeToLive);
        }

        public Task SendAsync(IMessage message, MsgDeliveryMode deliveryMode, MsgPriority priority, TimeSpan timeToLive)
        {
            return SendAsync(Info.Destination, message, deliveryMode, priority, timeToLive);
        }

        public Task SendAsync(IDestination destination, IMessage message)
        {
            return SendAsync(destination, message, deliveryMode, priority, timeToLive);
        }

        public Task SendAsync(IDestination destination, IMessage message, MsgDeliveryMode deliveryMode, MsgPriority priority,
            TimeSpan timeToLive)
        {
            CheckClosed();
            return session.SendAsync(this, destination, message, deliveryMode, priority, timeToLive, DisableMessageID, DisableMessageTimestamp, deliveryDelay);
        }

        public void Close()
        {
            CloseAsync().GetAsyncResult();
        }

        public async Task CloseAsync()
        {
            if (closed)
                return;

            Shutdown();
            await session.Connection.DestroyResource(Info).Await();
        }

        public IMessage CreateMessage()
        {
            CheckClosed();
            return session.CreateMessage();
        }

        public async Task<IMessage> CreateMessageAsync()
        {
            CheckClosed();
            return await session.CreateMessageAsync();
        }

        public ITextMessage CreateTextMessage()
        {
            CheckClosed();
            return session.CreateTextMessage();
        }

        public async Task<ITextMessage> CreateTextMessageAsync()
        {
            CheckClosed();
            return await session.CreateTextMessageAsync();
        }

        public ITextMessage CreateTextMessage(string text)
        {
            CheckClosed();
            return session.CreateTextMessage(text);
        }

        public async Task<ITextMessage> CreateTextMessageAsync(string text)
        {
            CheckClosed();
            return await session.CreateTextMessageAsync(text);
        }

        public IMapMessage CreateMapMessage()
        {
            CheckClosed();
            return session.CreateMapMessage();
        }

        public async Task<IMapMessage> CreateMapMessageAsync()
        {
            CheckClosed();
            return await session.CreateMapMessageAsync();
        }

        public IObjectMessage CreateObjectMessage(object body)
        {
            CheckClosed();
            return session.CreateObjectMessage(body);
        }

        public async Task<IObjectMessage> CreateObjectMessageAsync(object body)
        {
            CheckClosed();
            return await session.CreateObjectMessageAsync(body);
        }

        public IBytesMessage CreateBytesMessage()
        {
            CheckClosed();
            return session.CreateBytesMessage();
        }

        public async Task<IBytesMessage> CreateBytesMessageAsync()
        {
            CheckClosed();
            return await session.CreateBytesMessageAsync();
        }

        public IBytesMessage CreateBytesMessage(byte[] body)
        {
            CheckClosed();
            return session.CreateBytesMessage(body);
        }

        public async Task<IBytesMessage> CreateBytesMessageAsync(byte[] body)
        {
            CheckClosed();
            return await session.CreateBytesMessageAsync(body);
        }

        public IStreamMessage CreateStreamMessage()
        {
            CheckClosed();
            return session.CreateStreamMessage();
        }

        public async Task<IStreamMessage> CreateStreamMessageAsync()
        {
            CheckClosed();
            return await session.CreateStreamMessageAsync();
        }

        public ProducerTransformerDelegate ProducerTransformer { get; set; }

        public MsgDeliveryMode DeliveryMode
        {
            get
            {
                CheckClosed();
                return deliveryMode;
            }
            set
            {
                CheckClosed();
                deliveryMode = value;
            }
        }

        public TimeSpan TimeToLive
        {
            get
            {
                CheckClosed();
                return timeToLive;
            }
            set
            {
                CheckClosed();
                timeToLive = value;
            }
        }

        public TimeSpan RequestTimeout
        {
            get
            {
                CheckClosed();
                return requestTimeout;
            }
            set
            {
                CheckClosed();
                requestTimeout = value;
            }
        }

        public MsgPriority Priority
        {
            get
            {
                CheckClosed();
                return priority;
            }
            set
            {
                CheckClosed();
                priority = value;
            }
        }

        public bool DisableMessageID
        {
            get
            {
                CheckClosed();
                return disableMessageId;
            }
            set
            {
                CheckClosed();
                disableMessageId = value;
            }
        }

        public bool DisableMessageTimestamp
        {
            get
            {
                CheckClosed();
                return disableMessageTimestamp;
            }
            set
            {
                CheckClosed();
                disableMessageTimestamp = value;
            }
        }

        public TimeSpan DeliveryDelay
        {
            get
            {
                CheckClosed();
                return deliveryDelay;
            }
            set
            {
                if (!session.Connection.ConnectionInfo.DelayedDeliverySupported)
                {
                    throw new NotSupportedException("Delayed Delivery is not supported");
                }
                
                CheckClosed();
                deliveryDelay = value;
            }
        }
        
        public Task OnConnectionRecovery(IProvider provider)
        {
            return provider.CreateResource(Info);
        }

        private void CheckClosed()
        {
            if (!closed) return;

            if (failureCause == null)
                throw new IllegalStateException("The MessageProducer is closed");
            else
                throw new IllegalStateException("The MessageProducer was closed due to an unrecoverable error.", failureCause);
        }

        public void Shutdown(Exception error = null)
        {
            if (closed.CompareAndSet(false, true))
            {
                failureCause = error;
                session.Remove(this);
            }
        }

        /// <summary>
        /// Returns the next logical sequence for a Message sent from this Producer.
        /// </summary>
        public long GetNextMessageSequence()
        {
            return messageSequence.IncrementAndGet();
        }
    }
}