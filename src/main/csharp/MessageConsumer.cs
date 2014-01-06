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
    /// An object capable of receiving messages from some destination
    /// </summary>
    public class MessageConsumer : IMessageConsumer
    {
        /// <summary>
        /// Private object used for synchronization, instead of public "this"
        /// </summary>
        private readonly object myLock = new object();

        protected TimeSpan zeroTimeout = new TimeSpan(0);

        private readonly Session session;
        private readonly int id;
        private readonly Destination destination;
        private readonly AcknowledgementMode acknowledgementMode;
        private event MessageListener listener;
        private int listenerCount = 0;
        private Thread asyncDeliveryThread = null;
        private AutoResetEvent pause = new AutoResetEvent(false);
        private Atomic<bool> asyncDelivery = new Atomic<bool>(false);

        private readonly Atomic<bool> started = new Atomic<bool>(false); 
        private Org.Apache.Qpid.Messaging.Receiver qpidReceiver = null;

        private ConsumerTransformerDelegate consumerTransformer;
        public ConsumerTransformerDelegate ConsumerTransformer
        {
            get { return this.consumerTransformer; }
            set { this.consumerTransformer = value; }
        }

        public MessageConsumer(Session session, int consumerId, Destination dest, AcknowledgementMode acknowledgementMode)
        {
            this.session = session;
            this.id = consumerId;
            this.destination = dest;
            this.acknowledgementMode = acknowledgementMode;
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
                    Tracer.DebugFormat("Start Consumer Id = " + ConsumerId.ToString());
                    if (qpidReceiver == null)
                    {
                        qpidReceiver = session.CreateQpidReceiver(destination.ToString());
                    }
                }
                catch (Org.Apache.Qpid.Messaging.QpidException e)
                {
                    throw new NMSException("Failed to create Qpid Receiver : " + e.Message);
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
                    qpidReceiver.Dispose();
                    qpidReceiver = null;
                }
                catch (Org.Apache.Qpid.Messaging.QpidException e)
                {
                    throw new NMSException("Failed to close session with Id " + ConsumerId.ToString() + " : " + e.Message);
                }
            }
        }
        #endregion

        public event MessageListener Listener
        {
            add
            {
                listener += value;
                listenerCount++;
                StartAsyncDelivery();
            }

            remove
            {
                if(listenerCount > 0)
                {
                    listener -= value;
                    listenerCount--;
                }

                if(0 == listenerCount)
                {
                    StopAsyncDelivery();
                }
            }
        }

        public IMessage Receive()
        {
            IMessage nmsMessage = null;

            Message qpidMessage = new Message();
            if (qpidReceiver.Fetch(ref qpidMessage))
            {
                nmsMessage = session.MessageConverter.ToNmsMessage(qpidMessage);
            }
            return nmsMessage;
        }

        public IMessage Receive(TimeSpan timeout)
        {
            IMessage nmsMessage = null;

            // TODO: Receive a message

            return nmsMessage;
        }

        public IMessage ReceiveNoWait()
        {
            IMessage nmsMessage = null;

            // TODO: Receive a message

            return nmsMessage;
        }

        public void Dispose()
        {
            Close();
        }

        public void Close()
        {
            StopAsyncDelivery();
        }

        protected virtual void StopAsyncDelivery()
        {
            if(asyncDelivery.CompareAndSet(true, false))
            {
                if(null != asyncDeliveryThread)
                {
                    Tracer.Info("Stopping async delivery thread.");
                    pause.Set();
                    if(!asyncDeliveryThread.Join(10000))
                    {
                        Tracer.Info("Aborting async delivery thread.");
                        asyncDeliveryThread.Abort();
                    }

                    asyncDeliveryThread = null;
                    Tracer.Info("Async delivery thread stopped.");
                }
            }
        }

        protected virtual void StartAsyncDelivery()
        {
            if(asyncDelivery.CompareAndSet(false, true))
            {
                asyncDeliveryThread = new Thread(new ThreadStart(DispatchLoop));
                asyncDeliveryThread.Name = "Message Consumer Dispatch: " + "TODO: unique name";
                asyncDeliveryThread.IsBackground = true;
                asyncDeliveryThread.Start();
            }
        }

        protected virtual void DispatchLoop()
        {
            Tracer.Info("Starting dispatcher thread consumer: " + this);
            while(asyncDelivery.Value)
            {
                try
                {
                    IMessage message = Receive();
                    if(asyncDelivery.Value && message != null)
                    {
                        try
                        {
                            listener(message);
                        }
                        catch(Exception e)
                        {
                            HandleAsyncException(e);
                        }
                    }
                }
                catch(ThreadAbortException ex)
                {
                    Tracer.InfoFormat("Thread abort received in thread: {0} : {1}", this, ex.Message);
                    break;
                }
                catch(Exception ex)
                {
                    Tracer.ErrorFormat("Exception while receiving message in thread: {0} : {1}", this, ex.Message);
                }
            }
            Tracer.Info("Stopping dispatcher thread consumer: " + this);
        }

        protected virtual void HandleAsyncException(Exception e)
        {
            session.Connection.HandleException(e);
        }

        protected virtual IMessage ToNmsMessage(Message message)
        {
            if(message == null)
            {
                return null;
            }

            IMessage converted = session.MessageConverter.ToNmsMessage(message);

            if(this.ConsumerTransformer != null)
            {
                IMessage newMessage = ConsumerTransformer(this.session, this, converted);
                if(newMessage != null)
                {
                    converted = newMessage;
                }
            }

            return converted;
        }

        public int ConsumerId
        {
            get { return id; }
        }
    }
}
