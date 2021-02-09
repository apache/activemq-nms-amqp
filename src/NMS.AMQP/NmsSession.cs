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
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.NMS.AMQP.Message;
using Apache.NMS.AMQP.Meta;
using Apache.NMS.AMQP.Provider;
using Apache.NMS.AMQP.Util;
using Apache.NMS.AMQP.Util.Synchronization;

namespace Apache.NMS.AMQP
{
    public class NmsSession : ISession
    {
        private readonly ConcurrentDictionary<NmsConsumerId, NmsMessageConsumer> consumers = new ConcurrentDictionary<NmsConsumerId, NmsMessageConsumer>();
        private readonly ConcurrentDictionary<NmsProducerId, NmsMessageProducer> producers = new ConcurrentDictionary<NmsProducerId, NmsMessageProducer>();

        private readonly AtomicLong consumerIdGenerator = new AtomicLong();
        private readonly AtomicLong producerIdGenerator = new AtomicLong();
        private readonly AtomicBool closed = new AtomicBool();
        private readonly AtomicBool started = new AtomicBool();

        public NmsSessionInfo SessionInfo { get; }
        public NmsConnection Connection { get; }
        
        private SessionDispatcher dispatcher;
        private Exception failureCause;
        private readonly AcknowledgementMode acknowledgementMode;

        public NmsSession(NmsConnection connection, NmsSessionId sessionId, AcknowledgementMode acknowledgementMode)
        {
            Connection = connection;
            this.acknowledgementMode = acknowledgementMode;
            SessionInfo = new NmsSessionInfo(sessionId)
            {
                AcknowledgementMode = acknowledgementMode
            };

            if (AcknowledgementMode == AcknowledgementMode.Transactional)
                TransactionContext = new NmsLocalTransactionContext(this);
            else
                TransactionContext = new NmsNoTxTransactionContext(this);
        }

        public bool IsStarted => started;

        internal async Task Begin()
        {
            await Connection.CreateResource(SessionInfo).Await();

            try
            {
                // We always keep an open TX if transacted so start now.
                await TransactionContext.Begin().Await();
            }
            catch (Exception)
            {
                // failed, close the AMQP session before we throw
                try
                {
                    await Connection.DestroyResource(SessionInfo).Await();
                }
                catch (Exception)
                {
                    // Ignore, throw original error
                }
                throw;
            }
        }

        public void Close()
        {
            CheckIsOnDeliveryExecutionFlow();

            if (!closed)
            {
                Shutdown();
                Connection.DestroyResource(SessionInfo).GetAsyncResult();
            }
        }

        public async Task CloseAsync()
        {
            CheckIsOnDeliveryExecutionFlow();

            if (!closed)
            {
                await ShutdownAsync().Await();
                await Connection.DestroyResource(SessionInfo).Await();
            }
        }

        public void Dispose()
        {
            try
            {
                Close();
            }
            catch (Exception ex)
            {
                Tracer.DebugFormat("Caught exception while disposing {0} {1}. Exception {2}", this.GetType().Name, this.SessionInfo.Id, ex);
            }
        }

        public IMessageProducer CreateProducer()
        {
            return CreateProducer(null);
        }

        public Task<IMessageProducer> CreateProducerAsync()
        {
            return CreateProducerAsync(null);
        }

        public IMessageProducer CreateProducer(IDestination destination)
        {
            return CreateProducerAsync(destination).GetAsyncResult();
        }

        public async Task<IMessageProducer> CreateProducerAsync(IDestination destination)
        {
            var producer = new NmsMessageProducer(GetNextProducerId(), this, destination);
            await producer.Init().Await();
            return producer;
        }
        
        private NmsProducerId GetNextProducerId()
        {
            return new NmsProducerId(SessionInfo.Id, producerIdGenerator.IncrementAndGet());
        }

        public IMessageConsumer CreateConsumer(IDestination destination)
        {
            return CreateConsumer(destination, null);
        }

        public Task<IMessageConsumer> CreateConsumerAsync(IDestination destination)
        {
            return CreateConsumerAsync(destination, null);
        }

        public IMessageConsumer CreateConsumer(IDestination destination, string selector)
        {
            return CreateConsumer(destination, selector, false);
        }

        public Task<IMessageConsumer> CreateConsumerAsync(IDestination destination, string selector)
        {
            return CreateConsumerAsync(destination, selector, false);
        }

        public IMessageConsumer CreateConsumer(IDestination destination, string selector, bool noLocal)
        {
            return CreateConsumerAsync(destination, selector, noLocal).GetAsyncResult();
        }

        public async Task<IMessageConsumer> CreateConsumerAsync(IDestination destination, string selector, bool noLocal)
        {
            CheckClosed();

            NmsMessageConsumer messageConsumer = new NmsMessageConsumer(GetNextConsumerId(), this, destination, selector, noLocal);
            await messageConsumer.Init().Await();

            return messageConsumer;
        }

        public IMessageConsumer CreateDurableConsumer(ITopic destination, string name)
        {
            return CreateDurableConsumer(destination, name, null, false);
        }

        public Task<IMessageConsumer> CreateDurableConsumerAsync(ITopic destination, string name)
        {
            return CreateDurableConsumerAsync(destination, name, null, false);
        }

        public IMessageConsumer CreateDurableConsumer(ITopic destination, string name, string selector)
        {
            return CreateDurableConsumer(destination, name, selector, false);
        }

        public Task<IMessageConsumer> CreateDurableConsumerAsync(ITopic destination, string name, string selector)
        {
            return CreateDurableConsumerAsync(destination, name, selector, false);
        }

        public IMessageConsumer CreateDurableConsumer(ITopic destination, string name, string selector, bool noLocal)
        {
            return CreateDurableConsumerAsync(destination, name, selector, noLocal).GetAsyncResult();
        }

        public async Task<IMessageConsumer> CreateDurableConsumerAsync(ITopic destination, string name, string selector, bool noLocal)
        {
            CheckClosed();

            NmsMessageConsumer messageConsumer = new NmsDurableMessageConsumer(GetNextConsumerId(), this, destination, name, selector, noLocal);
            await messageConsumer.Init().Await();

            return messageConsumer;
        }

        public IMessageConsumer CreateSharedConsumer(ITopic destination, string name)
        {
            return CreateSharedConsumer(destination, name, null);
        }

        public Task<IMessageConsumer> CreateSharedConsumerAsync(ITopic destination, string name)
        {
            return CreateSharedConsumerAsync(destination, name, null);
        }

        public IMessageConsumer CreateSharedConsumer(ITopic destination, string name, string selector)
        {
            return CreateSharedConsumerAsync(destination, name, selector).GetAsyncResult();
        }

        public async Task<IMessageConsumer> CreateSharedConsumerAsync(ITopic destination, string name, string selector)
        {
            CheckClosed();

            NmsMessageConsumer messageConsumer = new NmsSharedMessageConsumer(GetNextConsumerId(), this, destination, name, selector, false);
            await messageConsumer.Init().Await();
            
            return messageConsumer;
        }

        public IMessageConsumer CreateSharedDurableConsumer(ITopic destination, string name)
        {
            return CreateSharedDurableConsumer(destination, name, null);
        }

        public Task<IMessageConsumer> CreateSharedDurableConsumerAsync(ITopic destination, string name)
        {
            return CreateSharedDurableConsumerAsync(destination, name, null);
        }

        public IMessageConsumer CreateSharedDurableConsumer(ITopic destination, string name, string selector)
        {
            return CreateSharedDurableConsumerAsync(destination, name, selector).GetAsyncResult();
        }

        public async Task<IMessageConsumer> CreateSharedDurableConsumerAsync(ITopic destination, string name, string selector)
        {
            CheckClosed();

            NmsMessageConsumer messageConsumer = new NmsSharedDurableMessageConsumer(GetNextConsumerId(), this, destination, name, selector, false);
            await messageConsumer.Init().Await();//.GetAwaiter().GetResult();
            
            return messageConsumer;
        }

        public NmsConsumerId GetNextConsumerId()
        {
            return new NmsConsumerId(SessionInfo.Id, consumerIdGenerator.IncrementAndGet());
        }

        public void DeleteDurableConsumer(string name)
        {
            Unsubscribe(name);
        }

        public void Unsubscribe(string name)
        {
            UnsubscribeAsync(name).GetAsyncResult();
        }

        public async Task UnsubscribeAsync(string name)
        {
            CheckClosed();

            await Connection.UnsubscribeAsync(name).Await();
        }

        public Task<IQueueBrowser> CreateBrowserAsync(IQueue queue)
        {
            return Task.FromResult(CreateBrowser(queue));
        }
       
        public Task<IQueueBrowser> CreateBrowserAsync(IQueue queue, string selector)
        {
            return Task.FromResult(CreateBrowser(queue, selector));
        }
        
        public IQueueBrowser CreateBrowser(IQueue queue)
        {
            return CreateBrowser(queue, null);
        }

       
        public IQueueBrowser CreateBrowser(IQueue queue, string selector)
        {
            CheckClosed();

            return new NmsQueueBrowser(this, queue, selector);
        }

        public IQueue GetQueue(string name)
        {
            CheckClosed();

            return new NmsQueue(name);
        }

        public Task<IQueue> GetQueueAsync(string name)
        {
            return Task.FromResult(GetQueue(name));
        }

        public ITopic GetTopic(string name)
        {
            CheckClosed();

            return new NmsTopic(name);
        }

        public Task<ITopic> GetTopicAsync(string name)
        {
            return Task.FromResult(GetTopic(name));
        }

        public ITemporaryQueue CreateTemporaryQueue()
        {
            return CreateTemporaryQueueAsync().GetAsyncResult();
        }

        public async Task<ITemporaryQueue> CreateTemporaryQueueAsync()
        {
            CheckClosed();

            return await Connection.CreateTemporaryQueueAsync().Await();
        }

        public ITemporaryTopic CreateTemporaryTopic()
        {
            return CreateTemporaryTopicAsync().GetAsyncResult();
        }

        public async Task<ITemporaryTopic> CreateTemporaryTopicAsync()
        {
            CheckClosed();

            return await Connection.CreateTemporaryTopicAsync().Await();
        }

        public void DeleteDestination(IDestination destination)
        {
            DeleteDestinationAsync(destination).GetAsyncResult();
        }

        public async Task DeleteDestinationAsync(IDestination destination)
        {
            CheckClosed();

            if (destination == null)
                return;

            if (destination is ITemporaryQueue temporaryQueue)
                await temporaryQueue.DeleteAsync().Await();
            else if (destination is ITemporaryTopic temporaryTopic)
                await temporaryTopic.DeleteAsync().Await();
            else
                throw new NotSupportedException("AMQP can not delete a Queue or Topic destination.");
        }

        public IMessage CreateMessage()
        {
            CheckClosed();

            return Connection.MessageFactory.CreateMessage();
        }

        public Task<IMessage> CreateMessageAsync()
        {
            return Task.FromResult(CreateMessage());
        }

        public ITextMessage CreateTextMessage()
        {
            CheckClosed();

            return Connection.MessageFactory.CreateTextMessage();
        }

        public Task<ITextMessage> CreateTextMessageAsync()
        {
            return Task.FromResult(CreateTextMessage());
        }

        public ITextMessage CreateTextMessage(string text)
        {
            CheckClosed();

            return Connection.MessageFactory.CreateTextMessage(text);
        }

        public Task<ITextMessage> CreateTextMessageAsync(string text)
        {
            return Task.FromResult(CreateTextMessage(text));
        }

        public IMapMessage CreateMapMessage()
        {
            CheckClosed();

            return Connection.MessageFactory.CreateMapMessage();
        }

        public Task<IMapMessage> CreateMapMessageAsync()
        {
            return Task.FromResult(CreateMapMessage());
        }

        public IObjectMessage CreateObjectMessage(object body)
        {
            CheckClosed();

            return Connection.MessageFactory.CreateObjectMessage(body);
        }

        public Task<IObjectMessage> CreateObjectMessageAsync(object body)
        {
            return Task.FromResult(CreateObjectMessage(body));
        }

        public IBytesMessage CreateBytesMessage()
        {
            CheckClosed();

            return Connection.MessageFactory.CreateBytesMessage();
        }

        public Task<IBytesMessage> CreateBytesMessageAsync()
        {
            return Task.FromResult(CreateBytesMessage());
        }

        public IBytesMessage CreateBytesMessage(byte[] body)
        {
            CheckClosed();

            return Connection.MessageFactory.CreateBytesMessage(body);
        }

        public Task<IBytesMessage> CreateBytesMessageAsync(byte[] body)
        {
            return Task.FromResult(CreateBytesMessage(body));
        }

        public IStreamMessage CreateStreamMessage()
        {
            CheckClosed();

            return Connection.MessageFactory.CreateStreamMessage();
        }
        
        public Task<IStreamMessage> CreateStreamMessageAsync()
        {
            return Task.FromResult(CreateStreamMessage());
        }

        public void Recover()
        {
            RecoverAsync().GetAsyncResult();
        }

        public async Task RecoverAsync()
        {
            CheckClosed();

            bool wasStarted = IsStarted;
            Stop();
            
            await Connection.Recover(SessionInfo.Id).Await();

            if (wasStarted) 
                Start();
        }

        public void Acknowledge()
        {
            AcknowledgeAsync().GetAsyncResult();
        }
        
        public async Task AcknowledgeAsync()
        {
            if (acknowledgementMode == AcknowledgementMode.ClientAcknowledge) {
                await AcknowledgeAsync(AckType.ACCEPTED).Await();
            }
        }

        public void Commit()
        {
            CheckClosed();

            TransactionContext.Commit().GetAsyncResult();
        }

        public async Task CommitAsync()
        {
            CheckClosed();

            await TransactionContext.Commit().Await();
        }

        public void Rollback()
        {
            RollbackAsync().GetAsyncResult();
        }

        public async Task RollbackAsync()
        {
            CheckClosed();
            
            // Stop processing any new messages that arrive
            try
            {
                foreach (NmsMessageConsumer consumer in consumers.Values)
                {
                    await consumer.SuspendForRollbackAsync().Await();
                }
            }
            finally
            {
                await TransactionContext.Rollback().Await(); //.GetAsyncResult();    
            }
            
            // Currently some consumers won't get suspended and some won't restart
            // after a failed rollback.
            foreach (NmsMessageConsumer consumer in consumers.Values)
            {
                await consumer.ResumeAfterRollbackAsync().Await();
            }
        }

        private void CheckClosed()
        {
            if (closed)
            {
                if (failureCause == null)
                    throw new IllegalStateException("The Session is closed");
                else
                    throw new IllegalStateException("The Session was closed due to an unrecoverable error.", failureCause);
            }
        }

        public ConsumerTransformerDelegate ConsumerTransformer { get; set; }
        public ProducerTransformerDelegate ProducerTransformer { get; set; }
        public TimeSpan RequestTimeout { get; set; }
        public bool Transacted => AcknowledgementMode == AcknowledgementMode.Transactional;

        public AcknowledgementMode AcknowledgementMode
        {
            get
            {
                CheckClosed();
                return acknowledgementMode;
            }
        }
        public bool IsClosed => closed;

        internal INmsTransactionContext TransactionContext { get; }

        public event SessionTxEventDelegate TransactionStartedListener
        {
            add => this.TransactionContext.TransactionStartedListener += value;
            remove => this.TransactionContext.TransactionStartedListener -= value;
        }
        public event SessionTxEventDelegate TransactionCommittedListener
        {
            add => this.TransactionContext.TransactionCommittedListener += value;
            remove => this.TransactionContext.TransactionCommittedListener -= value;            
        }
        public event SessionTxEventDelegate TransactionRolledBackListener
        {
            add => this.TransactionContext.TransactionRolledBackListener += value;
            remove => this.TransactionContext.TransactionRolledBackListener -= value;      
        }

        public void OnInboundMessage(InboundMessageDispatch envelope)
        {
            if (consumers.TryGetValue(envelope.ConsumerInfo.Id, out NmsMessageConsumer messageConsumer))
            {
                messageConsumer.OnInboundMessage(envelope);
            }
            else
            {
                Tracer.Error($"Could not dispatch message {envelope.Message.NMSMessageId} because consumer {envelope.ConsumerId} not found.");
            }
        }

        public Task AcknowledgeAsync(AckType ackType)
        {
            return Connection.Acknowledge(SessionInfo.Id, ackType);
        }

        public Task AcknowledgeAsync(AckType ackType, InboundMessageDispatch envelope)
        {
            return TransactionContext.Acknowledge(envelope, ackType);
        }

        public Task AcknowledgeIndividualAsync(AckType ackType, InboundMessageDispatch envelope)
        {
            if (Transacted)
            {
                throw new IllegalStateException("Message acknowledge called inside a transacted Session");
            }

            return Connection.Acknowledge(envelope, ackType); //.GetAsyncResult();
        }

        public void Send(NmsMessageProducer producer, IDestination destination, IMessage original,
            MsgDeliveryMode deliveryMode,
            MsgPriority priority, TimeSpan timeToLive, bool disableMessageId, bool disableMessageTimestamp, TimeSpan deliveryDelay)
        {

            SendAsync(producer, destination, original, deliveryMode, priority, timeToLive, disableMessageId,
                disableMessageTimestamp, deliveryDelay).GetAsyncResult();
            
        }

        public Task SendAsync(NmsMessageProducer producer, IDestination destination, IMessage original, MsgDeliveryMode deliveryMode,
            MsgPriority priority, TimeSpan timeToLive, bool disableMessageId, bool disableMessageTimestamp, TimeSpan deliveryDelay)
        {
            if (destination == null)
                throw new InvalidDestinationException("Destination must not be null");
            
            if (original == null)
                throw new MessageFormatException("Message must not be null");

            NmsMessage outbound = null;
            original.NMSDeliveryMode = deliveryMode;
            original.NMSPriority = priority;
            original.NMSRedelivered = false;
            original.NMSDestination = destination;

            bool isNmsMessage = original is NmsMessage;

            DateTime timeStamp = DateTime.UtcNow;

            bool hasTTL = timeToLive > TimeSpan.Zero;
            bool hasDelay = deliveryDelay > TimeSpan.Zero;

            if (!disableMessageTimestamp)
            {
                original.NMSTimestamp = timeStamp;
            }
            else
                original.NMSTimestamp = DateTime.MinValue;
            
            long messageSequence = producer.GetNextMessageSequence();
            object messageId = null;
            if (!disableMessageId)
            {
                messageId = producer.MessageIdBuilder.CreateMessageId(producer.ProducerId.ToString(), messageSequence);
            }

            if (isNmsMessage)
                outbound = (NmsMessage) original;
            else
                outbound = NmsMessageTransformation.TransformMessage(Connection.MessageFactory, original);
            
            // Set the message ID
            outbound.Facade.ProviderMessageIdObject = messageId;
            if (!isNmsMessage)
            {
                // If the original was a foreign message, we still need to update it
                // with the properly encoded Message ID String, get it from the one
                // we transformed from now that it is set.
                original.NMSMessageId = outbound.NMSMessageId;
            }

            if (hasDelay)
            {
                outbound.Facade.DeliveryTime = timeStamp + deliveryDelay;
            }

            if (hasTTL)
                outbound.Facade.Expiration = timeStamp + timeToLive;
            else
                outbound.Facade.Expiration = null;

            outbound.OnSend(timeToLive);

            bool fireAndForget = deliveryMode == MsgDeliveryMode.NonPersistent;

            return TransactionContext.Send(new OutboundMessageDispatch
            {
                Message = outbound,
                ProducerId = producer.Info.Id,
                ProducerInfo = producer.Info,
                FireAndForget = fireAndForget
            });
        }

        internal void EnqueueForDispatch(NmsMessageConsumer.MessageDeliveryTask task)
        {
            if (Tracer.IsDebugEnabled)
            {
                Tracer.Debug("Message enqueued for dispatch.");
            }
            
            dispatcher?.Post(task);
        }

        public NmsMessageConsumer ConsumerClosed(NmsConsumerId consumerId, Exception cause)
        {
            Tracer.Info($"A NMS MessageConsumer has been closed: {consumerId}");

            if (consumers.TryGetValue(consumerId, out NmsMessageConsumer consumer))
            {
                if (consumer.HasMessageListener())
                {
                    Connection.OnAsyncException(cause);
                }
            }

            try
            {
                consumer?.Shutdown(cause);
            }
            catch (Exception error)
            {
                Tracer.DebugFormat("Ignoring exception thrown during cleanup of closed consumer", error);
            }

            return consumer;
        }

        public NmsMessageProducer ProducerClosed(NmsProducerId producerId, Exception cause)
        {
            Tracer.Info($"A NmsMessageProducer has been closed: {producerId}. Cause: {cause}");

            if (producers.TryGetValue(producerId, out NmsMessageProducer producer))
            {
                try
                {
                    producer.Shutdown(cause);
                }
                catch (Exception error)
                {
                    Tracer.DebugFormat("Ignoring exception thrown during cleanup of closed producer", error);
                }
            }
            else
            {
                if (Tracer.IsDebugEnabled)
                {
                    Tracer.Debug($"NmsMessageProducer: {producerId} not found in session {this.SessionInfo.Id}.");
                }

            }
            
            return producer;
        }

        public void Add(NmsMessageConsumer messageConsumer)
        {
            consumers.TryAdd(messageConsumer.Info.Id, messageConsumer);
        }
        
        public void Add(NmsMessageProducer messageProducer)
        {
            producers.TryAdd(messageProducer.Info.Id, messageProducer);
        }

        public void Remove(NmsMessageConsumer messageConsumer)
        {
            consumers.TryRemove(messageConsumer.Info.Id, out _);
        }

        public void Remove(NmsMessageProducer messageProducer)
        {
            producers.TryRemove(messageProducer.Info.Id, out _);
        }

        public void Shutdown(NMSException exception = null)
        {
            ShutdownAsync(exception).GetAsyncResult();
        }

        public async Task ShutdownAsync(NMSException exception = null)
        {
            if (closed.CompareAndSet(false, true))
            {
                failureCause = exception;
                Stop();
                
                try
                {
                    foreach (NmsMessageConsumer consumer in consumers.Values.ToArray())
                        consumer.Shutdown(exception);

                    foreach (NmsMessageProducer producer in producers.Values.ToArray()) 
                        producer.Shutdown(exception);

                    await TransactionContext.Shutdown().Await();
                }
                finally
                {
                    Connection.RemoveSession(SessionInfo);
                }
            }
        }
        
        public void Start()
        {
            if (started.CompareAndSet(false, true))
            {
                dispatcher = new SessionDispatcher();

                foreach (NmsMessageConsumer consumer in consumers.Values.ToArray())
                {
                    consumer.Start();
                }
            }
        }

        internal void CheckIsOnDeliveryExecutionFlow()
        {
            if (dispatcher != null && dispatcher.IsOnDeliveryExecutionFlow())
            {
                throw new IllegalStateException("Illegal invocation from MessageListener callback");
            }
        }

        public async Task OnConnectionRecovery(IProvider provider)
        {
            await provider.CreateResource(SessionInfo).Await();

            foreach (NmsMessageConsumer consumer in consumers.Values)
            {
                await consumer.OnConnectionRecovery(provider).Await();
            }

            foreach (NmsMessageProducer producer in producers.Values)
            {
                await producer.OnConnectionRecovery(provider).Await();
            }
        }

        public async Task OnConnectionRecovered(IProvider provider)
        {
            foreach (NmsMessageConsumer consumer in consumers.Values)
            {
                await consumer.OnConnectionRecovered(provider).Await();
            }
        }

        public void Stop()
        {
            if (started.CompareAndSet(true, false))
            {
                dispatcher.Stop();
                dispatcher = null;

                foreach (NmsMessageConsumer consumer in consumers.Values)
                {
                    consumer.Stop();
                }
            }
        }

        public bool IsDestinationInUse(NmsTemporaryDestination destination)
        {
            foreach (var consumer in consumers.Values.ToArray())
            {
                if (consumer.IsDestinationInUse(destination))
                {
                    return true;
                }
            }

            return false;
        }

        public void OnConnectionInterrupted()
        {
            TransactionContext.OnConnectionInterrupted();

            foreach (NmsMessageConsumer consumer in consumers.Values)
            {
                consumer.OnConnectionInterrupted();
            }
        }

        internal bool IsIndividualAcknowledge() => acknowledgementMode == AcknowledgementMode.IndividualAcknowledge;
    }
}