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
using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP
{
    public class NmsContext : INMSContext
    {
        private readonly object syncRoot = new object();

        private readonly NmsConnection connection;
        private readonly AtomicLong connectionRefCount;
        public AcknowledgementMode AcknowledgementMode { get; }

        private NmsSession session;
        private NmsMessageProducer sharedProducer;
        private bool autoStart = true;

        public NmsContext(NmsConnection connection, AcknowledgementMode acknowledgementMode)
        {
            this.connection = connection;
            this.AcknowledgementMode = acknowledgementMode;
            this.connectionRefCount = new AtomicLong(1);
        }

        private NmsContext(NmsConnection connection, AcknowledgementMode acknowledgementMode,
            AtomicLong connectionRefCount)
        {
            this.connection = connection;
            this.AcknowledgementMode = acknowledgementMode;
            this.connectionRefCount = connectionRefCount;
        }

        public void Dispose()
        {
            connection.Dispose();
        }

        public void Start()
        {
            connection.Start();
        }

        public bool IsStarted { get => connection.IsStarted; }
        
        public void Stop()
        {
            connection.Stop();
        }

        public INMSContext CreateContext(AcknowledgementMode acknowledgementMode)
        {
            if (connectionRefCount.Get() == 0) {
                throw new IllegalStateException("The Connection is closed");
            }
            
            connectionRefCount.IncrementAndGet();
            return new NmsContext(connection, acknowledgementMode, connectionRefCount);
        }

        public INMSProducer CreateProducer()
        {
            if (sharedProducer == null) {
                lock (syncRoot) {
                    if (sharedProducer == null) {
                        sharedProducer = (NmsMessageProducer) GetSession().CreateProducer();
                    }
                }
            }
            return new NmsProducer(GetSession(), sharedProducer);
        }


        public INMSConsumer CreateConsumer(IDestination destination)
        {
            return StartIfNeeded(new NmsConsumer(GetSession(), (NmsMessageConsumer) GetSession().CreateConsumer(destination)));
        }

        public INMSConsumer CreateConsumer(IDestination destination, string selector)
        {
            return StartIfNeeded(new NmsConsumer(GetSession(), (NmsMessageConsumer) GetSession().CreateConsumer(destination, selector)));
        }

        public INMSConsumer CreateConsumer(IDestination destination, string selector, bool noLocal)
        {
            return StartIfNeeded(new NmsConsumer(GetSession(), (NmsMessageConsumer) GetSession().CreateConsumer(destination, selector, noLocal)));
        }

        public INMSConsumer CreateDurableConsumer(ITopic destination, string subscriptionName)
        {
            return StartIfNeeded(new NmsConsumer(GetSession(), (NmsMessageConsumer) GetSession().CreateDurableConsumer(destination, subscriptionName)));
        }

        public INMSConsumer CreateDurableConsumer(ITopic destination, string subscriptionName, string selector)
        {
            return StartIfNeeded(new NmsConsumer(GetSession(), (NmsMessageConsumer) GetSession().CreateDurableConsumer(destination, subscriptionName, selector)));
        }

        public INMSConsumer CreateDurableConsumer(ITopic destination, string subscriptionName, string selector, bool noLocal)
        {
            return StartIfNeeded(new NmsConsumer(GetSession(), (NmsMessageConsumer) GetSession().CreateDurableConsumer(destination, subscriptionName, selector, noLocal)));
        }

        public INMSConsumer CreateSharedConsumer(ITopic destination, string subscriptionName)
        {
            return StartIfNeeded(new NmsConsumer(GetSession(), (NmsMessageConsumer) GetSession().CreateSharedConsumer(destination, subscriptionName)));
        }

        public INMSConsumer CreateSharedConsumer(ITopic destination, string subscriptionName, string selector)
        {
            return StartIfNeeded(new NmsConsumer(GetSession(), (NmsMessageConsumer) GetSession().CreateSharedConsumer(destination, subscriptionName, selector)));
        }

        public INMSConsumer CreateSharedDurableConsumer(ITopic destination, string subscriptionName)
        {
            return StartIfNeeded(new NmsConsumer(GetSession(), (NmsMessageConsumer) GetSession().CreateSharedDurableConsumer(destination, subscriptionName)));
        }

        public INMSConsumer CreateSharedDurableConsumer(ITopic destination, string subscriptionName, string selector)
        {
            return StartIfNeeded(new NmsConsumer(GetSession(), (NmsMessageConsumer) GetSession().CreateSharedDurableConsumer(destination, subscriptionName, selector)));
        }

        public void Unsubscribe(string name)
        {
            GetSession().Unsubscribe(name);
        }

        public IQueueBrowser CreateBrowser(IQueue queue)
        {
            return GetSession().CreateBrowser(queue);
        }

        public IQueueBrowser CreateBrowser(IQueue queue, string selector)
        {
            return GetSession().CreateBrowser(queue, selector);
        }

        public IQueue GetQueue(string name)
        {
            return GetSession().GetQueue(name);
        }

        public ITopic GetTopic(string name)
        {
            return GetSession().GetTopic(name);
        }

        public ITemporaryQueue CreateTemporaryQueue()
        {
            return GetSession().CreateTemporaryQueue();
        }

        public ITemporaryTopic CreateTemporaryTopic()
        {
            return GetSession().CreateTemporaryTopic();
        }

        public IMessage CreateMessage()
        {
            return GetSession().CreateMessage();
        }

        public ITextMessage CreateTextMessage()
        {
            return GetSession().CreateTextMessage();
        }

        public ITextMessage CreateTextMessage(string text)
        {
            return GetSession().CreateTextMessage(text);
        }

        public IMapMessage CreateMapMessage()
        {
            return GetSession().CreateMapMessage();
        }

        public IObjectMessage CreateObjectMessage(object body)
        {
            return GetSession().CreateObjectMessage(body);
        }

        public IBytesMessage CreateBytesMessage()
        {
            return GetSession().CreateBytesMessage();
        }

        public IBytesMessage CreateBytesMessage(byte[] body)
        {
            return GetSession().CreateBytesMessage(body);
        }

        public IStreamMessage CreateStreamMessage()
        {
            return GetSession().CreateStreamMessage();
        }

        public void Close()
        {
            NMSException failure = null;

            try
            {
                session?.Close();
            } catch (NMSException jmse)
            {
                failure = jmse;
            }

            if (connectionRefCount.DecrementAndGet() == 0) {
                try {
                    connection.Close();
                } catch (NMSException jmse) {
                    if (failure == null)
                    {
                        failure = jmse;
                    }
                }
            }

            if (failure != null) {
                throw failure;
            }
        }

        public void Recover()
        {
            GetSession().Recover();
        }

        public void Acknowledge()
        {
            GetSession().Acknowledge();
        }

        public void Commit()
        {
            GetSession().Commit();
        }

        public void Rollback()
        {
            GetSession().Rollback();
        }

        public void PurgeTempDestinations()
        {
            connection.PurgeTempDestinations();
        }
        
        
        private NmsSession GetSession() {
            if (session == null) {
                lock (syncRoot) {
                    if (session == null)
                    {
                        session = (NmsSession) connection.CreateSession(AcknowledgementMode);
                    }
                }
            }
            return session;
        }
        
        private NmsConsumer StartIfNeeded(NmsConsumer consumer) {
            if (autoStart) {
                connection.Start();
            }
            return consumer;
        }
        

        public ConsumerTransformerDelegate ConsumerTransformer { get => session.ConsumerTransformer; set => session.ConsumerTransformer = value; }
        
        public ProducerTransformerDelegate ProducerTransformer { get => session.ProducerTransformer; set => session.ProducerTransformer = value; }
        
        public TimeSpan RequestTimeout { get => session.RequestTimeout; set => session.RequestTimeout = value; }
        
        public bool Transacted => session.Transacted;
        
        public string ClientId { get => connection.ClientId; set => connection.ClientId = value; }
        
        public bool AutoStart { get => autoStart; set => autoStart = value; }
        
        public event SessionTxEventDelegate TransactionStartedListener;
        
        public event SessionTxEventDelegate TransactionCommittedListener;
        
        public event SessionTxEventDelegate TransactionRolledBackListener;
        
        public event ExceptionListener ExceptionListener;
        
        public event ConnectionInterruptedListener ConnectionInterruptedListener;
        
        public event ConnectionResumedListener ConnectionResumedListener;
    }
}