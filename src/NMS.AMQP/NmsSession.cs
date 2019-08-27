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

namespace Apache.NMS.AMQP
{
    public class NmsSession : ISession
    {
        private readonly ConcurrentDictionary<Id, NmsMessageConsumer> consumers = new ConcurrentDictionary<Id, NmsMessageConsumer>();
        private readonly ConcurrentDictionary<Id, NmsMessageProducer> producers = new ConcurrentDictionary<Id, NmsMessageProducer>();

        private readonly NestedIdGenerator consumerIdGenerator;
        private readonly NestedIdGenerator producerIdGenerator;
        private readonly AtomicBool closed = new AtomicBool();
        private readonly AtomicBool started = new AtomicBool();

        public SessionInfo SessionInfo { get; }
        public NmsConnection Connection { get; }
        
        private SessionDispatcher dispatcher;
        private Exception failureCause;
        private readonly AcknowledgementMode acknowledgementMode;

        public NmsSession(NmsConnection connection, Id sessionId, AcknowledgementMode acknowledgementMode)
        {
            Connection = connection;
            this.acknowledgementMode = acknowledgementMode;
            SessionInfo = new SessionInfo(sessionId)
            {
                AcknowledgementMode = acknowledgementMode
            };
            consumerIdGenerator = new NestedIdGenerator("ID:consumer", SessionInfo.Id, true);
            producerIdGenerator = new NestedIdGenerator("ID:producer", SessionInfo.Id, true);

            if (AcknowledgementMode == AcknowledgementMode.Transactional)
                TransactionContext = new NmsLocalTransactionContext(this);
            else
                TransactionContext = new NmsNoTxTransactionContext(this);
        }

        public bool IsStarted => started;

        internal async Task Begin()
        {
            await Connection.CreateResource(SessionInfo).ConfigureAwait(false);
            await TransactionContext.Begin().ConfigureAwait(false);
        }

        public void Close()
        {
            CheckIsOnDeliveryThread();

            if (!closed)
            {
                Shutdown();
                Connection.DestroyResource(SessionInfo);
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

        public IMessageProducer CreateProducer(IDestination destination)
        {
            NmsMessageProducer messageProducer = new NmsMessageProducer(producerIdGenerator.GenerateId(), this, destination);
            messageProducer.Init().ConfigureAwait(false).GetAwaiter().GetResult();
            producers.TryAdd(messageProducer.Info.Id, messageProducer);
            return messageProducer;
        }

        public IMessageConsumer CreateConsumer(IDestination destination)
        {
            return CreateConsumer(destination, null);
        }

        public IMessageConsumer CreateConsumer(IDestination destination, string selector)
        {
            return CreateConsumer(destination, selector, false);
        }

        public IMessageConsumer CreateConsumer(IDestination destination, string selector, bool noLocal)
        {
            CheckClosed();

            NmsMessageConsumer messageConsumer = new NmsMessageConsumer(consumerIdGenerator.GenerateId(), this, destination, selector, noLocal);
            messageConsumer.Init().ConfigureAwait(false).GetAwaiter().GetResult();

            consumers.TryAdd(messageConsumer.Info.Id, messageConsumer);
            
            return messageConsumer;
        }

        public IMessageConsumer CreateDurableConsumer(ITopic destination, string name, string selector, bool noLocal)
        {
            CheckClosed();

            NmsMessageConsumer messageConsumer = new NmsMessageConsumer(consumerIdGenerator.GenerateId(), this, destination, name, selector, noLocal);
            messageConsumer.Info.IsDurable = true;
            messageConsumer.Init().ConfigureAwait(false).GetAwaiter().GetResult();
            consumers.TryAdd(messageConsumer.Info.Id, messageConsumer);

            return messageConsumer;
        }

        public void DeleteDurableConsumer(string name)
        {
            CheckClosed();

            Connection.Unsubscribe(name);
        }

        public IQueueBrowser CreateBrowser(IQueue queue)
        {
            throw new NotImplementedException();
        }

        public IQueueBrowser CreateBrowser(IQueue queue, string selector)
        {
            throw new NotImplementedException();
        }

        public IQueue GetQueue(string name)
        {
            CheckClosed();

            return new NmsQueue(name);
        }

        public ITopic GetTopic(string name)
        {
            CheckClosed();

            return new NmsTopic(name);
        }

        public ITemporaryQueue CreateTemporaryQueue()
        {
            CheckClosed();

            return Connection.CreateTemporaryQueue();
        }

        public ITemporaryTopic CreateTemporaryTopic()
        {
            CheckClosed();

            return Connection.CreateTemporaryTopic();
        }

        public void DeleteDestination(IDestination destination)
        {
            CheckClosed();

            if (destination == null)
                return;

            if (destination is ITemporaryQueue temporaryQueue)
                temporaryQueue.Delete();
            else if (destination is ITemporaryTopic temporaryTopic)
                temporaryTopic.Delete();
            else
                throw new NotSupportedException("AMQP can not delete a Queue or Topic destination.");
        }

        public IMessage CreateMessage()
        {
            CheckClosed();

            return Connection.MessageFactory.CreateMessage();
        }

        public ITextMessage CreateTextMessage()
        {
            CheckClosed();

            return Connection.MessageFactory.CreateTextMessage();
        }

        public ITextMessage CreateTextMessage(string text)
        {
            CheckClosed();

            return Connection.MessageFactory.CreateTextMessage(text);
        }

        public IMapMessage CreateMapMessage()
        {
            CheckClosed();

            return Connection.MessageFactory.CreateMapMessage();
        }

        public IObjectMessage CreateObjectMessage(object body)
        {
            CheckClosed();

            return Connection.MessageFactory.CreateObjectMessage(body);
        }

        public IBytesMessage CreateBytesMessage()
        {
            CheckClosed();

            return Connection.MessageFactory.CreateBytesMessage();
        }

        public IBytesMessage CreateBytesMessage(byte[] body)
        {
            CheckClosed();

            return Connection.MessageFactory.CreateBytesMessage(body);
        }

        public IStreamMessage CreateStreamMessage()
        {
            CheckClosed();

            return Connection.MessageFactory.CreateStreamMessage();
        }

        public void Recover()
        {
            CheckClosed();

            bool wasStarted = IsStarted;
            Stop();
            
            Connection.Recover(SessionInfo.Id).ConfigureAwait(false).GetAwaiter().GetResult();

            if (wasStarted) 
                Start();
        }

        public void Commit()
        {
            CheckClosed();

            TransactionContext.Commit().ConfigureAwait(false).GetAwaiter().GetResult();
        }

        public void Rollback()
        {
            CheckClosed();
            
            // Stop processing any new messages that arrive
            try
            {
                foreach (NmsMessageConsumer consumer in consumers.Values)
                {
                    consumer.SuspendForRollback();
                }
            }
            finally
            {
                TransactionContext.Rollback().ConfigureAwait(false).GetAwaiter().GetResult();    
            }
            
            // Currently some consumers won't get suspended and some won't restart
            // after a failed rollback.
            foreach (NmsMessageConsumer consumer in consumers.Values)
            {
                consumer.ResumeAfterRollback();
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
        }

        public void Acknowledge(AckType ackType)
        {
            Connection.Acknowledge(SessionInfo.Id, ackType).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        public void Acknowledge(AckType ackType, InboundMessageDispatch envelope)
        {
            TransactionContext.Acknowledge(envelope, ackType).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        public void AcknowledgeIndividual(AckType ackType, InboundMessageDispatch envelope)
        {
            if (Transacted)
            {
                throw new IllegalStateException("Message acknowledge called inside a transacted Session");
            }

            Connection.Acknowledge(envelope, ackType).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        public void Send(NmsMessageProducer producer, IDestination destination, IMessage original, MsgDeliveryMode deliveryMode,
            MsgPriority priority, TimeSpan timeToLive, bool disableMessageId, bool disableMessageTimestamp)
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

            if (!disableMessageTimestamp)
                original.NMSTimestamp = timeStamp;
            else
                original.NMSTimestamp = DateTime.MinValue;

            if (!disableMessageId)
                original.NMSMessageId = producer.MessageIdGenerator.GenerateId().ToString();
            else
                original.NMSMessageId = null;

            if (isNmsMessage)
                outbound = (NmsMessage) original;
            else
                outbound = NmsMessageTransformation.TransformMessage(Connection.MessageFactory, original);

            if (hasTTL)
                outbound.Facade.Expiration = timeStamp + timeToLive;
            else
                outbound.Facade.Expiration = null;

            outbound.OnSend(timeToLive);

            bool sync = deliveryMode == MsgDeliveryMode.Persistent;

            TransactionContext.Send(new OutboundMessageDispatch
            {
                Message = outbound,
                ProducerId = producer.Info.Id,
                ProducerInfo = producer.Info,
                SendAsync = !sync
            }).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        internal void EnqueueForDispatch(NmsMessageConsumer.MessageDeliveryTask task)
        {
            dispatcher?.Post(task);
        }

        public NmsMessageConsumer ConsumerClosed(Id consumerId, Exception cause)
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

        public NmsMessageProducer ProducerClosed(Id producerId, Exception cause)
        {
            Tracer.Info($"A NMS MessageProducer has been closed: {producerId}");

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
            
            return producer;
        }

        public void Remove(NmsMessageConsumer messageConsumer)
        {
            consumers.TryRemove(messageConsumer.Info.Id, out messageConsumer);
        }

        public void Remove(NmsMessageProducer messageProducer)
        {
            producers.TryRemove(messageProducer.Info.Id, out messageProducer);
        }

        public void Shutdown(NMSException exception = null)
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

                    TransactionContext.Shutdown().ConfigureAwait(false).GetAwaiter().GetResult();
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

        internal void CheckIsOnDeliveryThread()
        {
            if (dispatcher != null && dispatcher.IsOnDeliveryThread())
            {
                throw new IllegalStateException("Illegal invocation from MessageListener callback");
            }
        }

        public async Task OnConnectionRecovery(IProvider provider)
        {
            await provider.CreateResource(SessionInfo).ConfigureAwait(false);

            foreach (NmsMessageConsumer consumer in consumers.Values)
            {
                await consumer.OnConnectionRecovery(provider).ConfigureAwait(false);
            }

            foreach (NmsMessageProducer producer in producers.Values)
            {
                await producer.OnConnectionRecovery(provider).ConfigureAwait(false);
            }
        }

        public async Task OnConnectionRecovered(IProvider provider)
        {
            foreach (NmsMessageConsumer consumer in consumers.Values)
            {
                await consumer.OnConnectionRecovered(provider).ConfigureAwait(false);
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
    }
}