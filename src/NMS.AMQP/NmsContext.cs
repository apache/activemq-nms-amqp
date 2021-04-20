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
using System.Threading.Tasks;
using Apache.NMS.AMQP.Util;
using Apache.NMS.AMQP.Util.Synchronization;

namespace Apache.NMS.AMQP
{
    public class NmsContext : INMSContext
    {
        private readonly NmsSynchronizationMonitor syncRoot = new NmsSynchronizationMonitor();

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

        public Task StartAsync()
        {
            return connection.StartAsync();
        }

        public bool IsStarted { get => connection.IsStarted; }
        
        public void Stop()
        {
            connection.Stop();
        }

        public Task StopAsync()
        {
            return connection.StopAsync();
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
            if (sharedProducer == null)
            {
                using(syncRoot.Lock())
                {
                    if (sharedProducer == null)
                    {
                        sharedProducer = (NmsMessageProducer) GetSession().CreateProducer();
                    }
                }
            }
            return new NmsProducer(GetSession(), sharedProducer);
        }

        public async Task<INMSProducer> CreateProducerAsync()
        {
            if (sharedProducer == null)
            {
                using (await syncRoot.LockAsync().Await())
                {
                    if (sharedProducer == null)
                    {
                        sharedProducer = (NmsMessageProducer) await (await GetSessionAsync().Await()).CreateProducerAsync().Await();
                    }
                }
            }
            return new NmsProducer(await GetSessionAsync(), sharedProducer);
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

        public async Task<INMSConsumer> CreateConsumerAsync(IDestination destination)
        {
            return await StartIfNeededAsync(new NmsConsumer(await GetSessionAsync().Await(), (NmsMessageConsumer) await (await GetSessionAsync()).CreateConsumerAsync(destination)));
        }

        public async Task<INMSConsumer> CreateConsumerAsync(IDestination destination, string selector)
        {
            return await StartIfNeededAsync(new NmsConsumer(await GetSessionAsync().Await(), (NmsMessageConsumer) await (await GetSessionAsync()).CreateConsumerAsync(destination, selector)));
        }

        public async Task<INMSConsumer> CreateConsumerAsync(IDestination destination, string selector, bool noLocal)
        {
            return await StartIfNeededAsync(new NmsConsumer(await GetSessionAsync(), (NmsMessageConsumer) await (await GetSessionAsync()).CreateConsumerAsync(destination, selector, noLocal)));
        }

        public async Task<INMSConsumer> CreateDurableConsumerAsync(ITopic destination, string subscriptionName)
        {
            return await StartIfNeededAsync(new NmsConsumer(await GetSessionAsync(), (NmsMessageConsumer) await (await GetSessionAsync()).CreateDurableConsumerAsync(destination, subscriptionName)));
        }

        public async Task<INMSConsumer> CreateDurableConsumerAsync(ITopic destination, string subscriptionName, string selector)
        {
            return await StartIfNeededAsync(new NmsConsumer(await GetSessionAsync(), (NmsMessageConsumer) await (await GetSessionAsync()).CreateDurableConsumerAsync(destination, subscriptionName, selector)));
        }

        public async Task<INMSConsumer> CreateDurableConsumerAsync(ITopic destination, string subscriptionName, string selector, bool noLocal)
        {
            return await StartIfNeededAsync(new NmsConsumer(await GetSessionAsync(), (NmsMessageConsumer) await (await GetSessionAsync()).CreateDurableConsumerAsync(destination, subscriptionName, selector, noLocal)));
        }

        public async Task<INMSConsumer> CreateSharedConsumerAsync(ITopic destination, string subscriptionName)
        {
            return await StartIfNeededAsync(new NmsConsumer(await GetSessionAsync(), (NmsMessageConsumer) await (await GetSessionAsync()).CreateSharedConsumerAsync(destination, subscriptionName)));
        }
        
        public async Task<INMSConsumer> CreateSharedConsumerAsync(ITopic destination, string subscriptionName, string selector)
        {
            return await StartIfNeededAsync(new NmsConsumer(await GetSessionAsync(), (NmsMessageConsumer) await (await GetSessionAsync()).CreateSharedConsumerAsync(destination, subscriptionName, selector)));
        }

        public async Task<INMSConsumer> CreateSharedDurableConsumerAsync(ITopic destination, string subscriptionName)
        {
            return await StartIfNeededAsync(new NmsConsumer(await GetSessionAsync(), (NmsMessageConsumer) await (await GetSessionAsync()).CreateSharedDurableConsumerAsync(destination, subscriptionName)));
        }

        public async Task<INMSConsumer> CreateSharedDurableConsumerAsync(ITopic destination, string subscriptionName, string selector)
        {
            return await StartIfNeededAsync(new NmsConsumer(await GetSessionAsync(), (NmsMessageConsumer) await (await GetSessionAsync()).CreateSharedDurableConsumerAsync(destination, subscriptionName, selector)));
        }

        public void Unsubscribe(string name)
        {
            GetSession().Unsubscribe(name);
        }

        public async Task UnsubscribeAsync(string name)
        {
            await (await GetSessionAsync().Await()).UnsubscribeAsync(name).Await();
        }

        public IQueueBrowser CreateBrowser(IQueue queue)
        {
            return GetSession().CreateBrowser(queue);
        }

        public async Task<IQueueBrowser> CreateBrowserAsync(IQueue queue)
        {
            return await (await GetSessionAsync().Await()).CreateBrowserAsync(queue).Await();
        }

        public IQueueBrowser CreateBrowser(IQueue queue, string selector)
        {
            return GetSession().CreateBrowser(queue, selector);
        }

        public async Task<IQueueBrowser> CreateBrowserAsync(IQueue queue, string selector)
        {
            return await (await GetSessionAsync().Await()).CreateBrowserAsync(queue, selector).Await();
        }

        public IQueue GetQueue(string name)
        {
            return GetSession().GetQueue(name);
        }

        public async Task<IQueue> GetQueueAsync(string name)
        {
            return await (await GetSessionAsync().Await()).GetQueueAsync(name).Await();
        }

        public ITopic GetTopic(string name)
        {
            return GetSession().GetTopic(name);
        }

        public async Task<ITopic> GetTopicAsync(string name)
        {
            return await (await GetSessionAsync().Await()).GetTopicAsync(name).Await();
        }

        public ITemporaryQueue CreateTemporaryQueue()
        {
            return GetSession().CreateTemporaryQueue();
        }

        public async Task<ITemporaryQueue> CreateTemporaryQueueAsync()
        {
            return await (await GetSessionAsync().Await()).CreateTemporaryQueueAsync().Await();
        }

        public ITemporaryTopic CreateTemporaryTopic()
        {
            return GetSession().CreateTemporaryTopic();
        }

        public async Task<ITemporaryTopic> CreateTemporaryTopicAsync()
        {
            return await (await GetSessionAsync().Await()).CreateTemporaryTopicAsync().Await();
        }

        public IMessage CreateMessage()
        {
            return GetSession().CreateMessage();
        }

        public async Task<IMessage> CreateMessageAsync()
        {
            return await (await GetSessionAsync().Await()).CreateMessageAsync().Await();
        }

        public ITextMessage CreateTextMessage()
        {
            return GetSession().CreateTextMessage();
        }

        public async Task<ITextMessage> CreateTextMessageAsync()
        {
            return await (await GetSessionAsync().Await()).CreateTextMessageAsync().Await();
        }

        public ITextMessage CreateTextMessage(string text)
        {
            return GetSession().CreateTextMessage(text);
        }

        public async Task<ITextMessage> CreateTextMessageAsync(string text)
        {
            return await (await GetSessionAsync().Await()).CreateTextMessageAsync(text).Await();
        }

        public IMapMessage CreateMapMessage()
        {
            return GetSession().CreateMapMessage();
        }

        public async Task<IMapMessage> CreateMapMessageAsync()
        {
            return await (await GetSessionAsync().Await()).CreateMapMessageAsync().Await();
        }

        public IObjectMessage CreateObjectMessage(object body)
        {
            return GetSession().CreateObjectMessage(body);
        }

        public async Task<IObjectMessage> CreateObjectMessageAsync(object body)
        {
            return await (await GetSessionAsync().Await()).CreateObjectMessageAsync(body).Await();
        }

        public IBytesMessage CreateBytesMessage()
        {
            return GetSession().CreateBytesMessage();
        }

        public async Task<IBytesMessage> CreateBytesMessageAsync()
        {
            return await (await GetSessionAsync().Await()).CreateBytesMessageAsync().Await();
        }

        public IBytesMessage CreateBytesMessage(byte[] body)
        {
            return GetSession().CreateBytesMessage(body);
        }

        public async Task<IBytesMessage> CreateBytesMessageAsync(byte[] body)
        {
            return await (await GetSessionAsync().Await()).CreateBytesMessageAsync(body).Await();
        }

        public IStreamMessage CreateStreamMessage()
        {
            return GetSession().CreateStreamMessage();
        }

        public async Task<IStreamMessage> CreateStreamMessageAsync()
        {
            return await (await GetSessionAsync().Await()).CreateStreamMessageAsync().Await();
        }

        public void Close()
        {
            CloseInternal(true).GetAsyncResult();
        }

        public Task CloseAsync()
        {
            return CloseInternal(false);
        }

        public async Task CloseInternal(bool sync)
        {
            NMSException failure = null;

            try
            {
                if (sync)
                    session?.Close();
                else
                    await (session?.CloseAsync() ?? Task.CompletedTask).Await();
            } catch (NMSException jmse)
            {
                failure = jmse;
            }

            if (connectionRefCount.DecrementAndGet() == 0) {
                try
                {
                    if (sync)
                        connection.Close();
                    else
                        await connection.CloseAsync().Await();
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

        public async Task RecoverAsync()
        {
            await (await GetSessionAsync().Await()).RecoverAsync().Await();
        }

        public void Acknowledge()
        {
            GetSession().Acknowledge();
        }

        public async Task AcknowledgeAsync()
        {
            await (await GetSessionAsync().Await()).AcknowledgeAsync().Await();
        }

        public void Commit()
        {
            GetSession().Commit();
        }

        public async Task CommitAsync()
        {
            await (await GetSessionAsync().Await()).CommitAsync().Await();
        }

        public void Rollback()
        {
            GetSession().Rollback();
        }

        public async Task RollbackAsync()
        {
            await (await GetSessionAsync().Await()).RollbackAsync().Await();
        }

        public void PurgeTempDestinations()
        {
            connection.PurgeTempDestinations();
        }
        
        
        private NmsSession GetSession() {
            if (session == null)
            {
                using( syncRoot.Lock())
                {
                    if (session == null)
                    {
                        session = (NmsSession) connection.CreateSession(AcknowledgementMode);
                    }
                }
            }
            return session;
        }

        private async Task<NmsSession> GetSessionAsync()
        {
            if (session == null)
            {
                using(await syncRoot.LockAsync().Await())
                {
                    if (session == null)
                    {
                        session = (NmsSession) await connection.CreateSessionAsync(AcknowledgementMode).Await();
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
        
        private async Task<NmsConsumer> StartIfNeededAsync(NmsConsumer consumer) {
            if (autoStart) {
                await connection.StartAsync().Await();
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