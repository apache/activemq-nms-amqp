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
using System.Threading;
using Apache.NMS.Util;
using Org.Apache.Qpid.Messaging;

namespace Apache.NMS.Amqp
{
    /// <summary>
    /// An object capable of sending messages to some destination
    /// </summary>
    public class MessageProducer : IMessageProducer
    {
        /// <summary>
        /// Private object used for synchronization, instead of public "this"
        /// </summary>
        private readonly object myLock = new object();

        private readonly Session session;
        private readonly int id; 
        private Destination destination;

        //private long messageCounter;
        private MsgDeliveryMode deliveryMode;
        private TimeSpan timeToLive;
        private MsgPriority priority;
        private bool disableMessageID;
        private bool disableMessageTimestamp;

        //private IMessageConverter messageConverter;

        private readonly Atomic<bool> started = new Atomic<bool>(false);
        private Org.Apache.Qpid.Messaging.Sender qpidSender = null;

        private ProducerTransformerDelegate producerTransformer;
        public ProducerTransformerDelegate ProducerTransformer
        {
            get { return this.producerTransformer; }
            set { this.producerTransformer = value; }
        }

        public MessageProducer(Session session, int producerId, Destination destination)
        {
            this.session = session;
            this.id = producerId;
            this.destination = destination;
        }

        #region IStartable Methods
        public void Start()
        {
            // Don't try creating session if connection not yet up
            if (!session.IsStarted)
            {
                throw new SessionClosedException();
            }

            if (started.CompareAndSet(false, true))
            {
                try
                {
                    // Create qpid sender
                    Tracer.DebugFormat("Start Producer Id = " + ProducerId.ToString()); 
                    if (qpidSender == null)
                    {
                        qpidSender = session.CreateQpidSender(destination.Address);
                    }
                }
                catch (Org.Apache.Qpid.Messaging.QpidException e)
                {
                    throw new NMSException("Failed to create Qpid Sender : " + e.Message);
                }
            }
        }

        public bool IsStarted
        {
            get { return started.Value; }
        }
        #endregion

        #region IStoppable Methods
        public void Stop()
        {
            if (started.CompareAndSet(true, false))
            {
                try
                {
                    Tracer.DebugFormat("Stop  Producer Id = " + ProducerId);
                    qpidSender.Close();
                    qpidSender.Dispose();
                    qpidSender = null;
                }
                catch (Org.Apache.Qpid.Messaging.QpidException e)
                {
                    throw new NMSException("Failed to close session with Id " + ProducerId.ToString() + " : " + e.Message);
                }
            }
        }
        #endregion

        public void Send(IMessage message)
        {
            Send(Destination, message);
        }

        public void Send(IMessage message, MsgDeliveryMode deliveryMode, MsgPriority priority, TimeSpan timeToLive)
        {
            Send(Destination, message, deliveryMode, priority, timeToLive);
        }

        public void Send(IDestination destination, IMessage message)
        {
            Send(destination, message, DeliveryMode, Priority, TimeToLive);
        }

        public void Send(IDestination destination, IMessage message, MsgDeliveryMode deliveryMode, MsgPriority priority, TimeSpan timeToLive)
        {
            try
            {
                if (this.ProducerTransformer != null)
                {
                    IMessage transformed = this.ProducerTransformer(this.session, this, message);
                    if (transformed != null)
                    {
                        message = transformed;
                    }
                }

                message.NMSDeliveryMode = deliveryMode;
                message.NMSTimeToLive = timeToLive;
                message.NMSPriority = priority;
                if (!DisableMessageTimestamp)
                {
                    message.NMSTimestamp = DateTime.UtcNow;
                }

                if (!DisableMessageID)
                {
                    // TODO: message.NMSMessageId =
                }

                // Convert the Message into a Amqp message
                Message msg = session.MessageConverter.ToAmqpMessage(message);

                qpidSender.Send(msg);
            }
            catch (Exception e)
            {
                throw new NMSException(e.Message + ": " /* TODO: + dest */, e);
            }
        }

        public void Close()
        {
            Stop();
        }

        public void Dispose()
        {
            Close();
        }

        public IMessage CreateMessage()
        {
            return session.CreateMessage();
        }

        public ITextMessage CreateTextMessage()
        {
            return session.CreateTextMessage();
        }

        public ITextMessage CreateTextMessage(String text)
        {
            return session.CreateTextMessage(text);
        }

        public IMapMessage CreateMapMessage()
        {
            return session.CreateMapMessage();
        }

        public IObjectMessage CreateObjectMessage(Object body)
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

        public MsgDeliveryMode DeliveryMode
        {
            get { return deliveryMode; }
            set { deliveryMode = value; }
        }

        public TimeSpan TimeToLive
        {
            get { return timeToLive; }
            set { timeToLive = value; }
        }

        /// <summary>
        /// The default timeout for network requests.
        /// </summary>
        public TimeSpan RequestTimeout
        {
            get { return NMSConstants.defaultRequestTimeout; }
            set { }
        }

        public IDestination Destination
        {
            get { return destination; }
            set { destination = (Destination) value; }
        }

        public MsgPriority Priority
        {
            get { return priority; }
            set { priority = value; }
        }

        public bool DisableMessageID
        {
            get { return disableMessageID; }
            set { disableMessageID = value; }
        }

        public bool DisableMessageTimestamp
        {
            get { return disableMessageTimestamp; }
            set { disableMessageTimestamp = value; }
        }

        public int ProducerId
        {
            get { return id; }
        }
    }
}
