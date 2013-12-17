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
using System.Threading;
using Org.Apache.Qpid.Messaging;

namespace Apache.NMS.Amqp
{
    /// <summary>
    /// Amqp provider of ISession
    /// </summary>
    public class Session : ISession
    {
        /// <summary>
        /// Private object used for synchronization, instead of public "this"
        /// </summary>
        private readonly object myLock = new object();

        private readonly IDictionary consumers = Hashtable.Synchronized(new Hashtable());
        private readonly IDictionary producers = Hashtable.Synchronized(new Hashtable());

        private Connection connection;
        private AcknowledgementMode acknowledgementMode;
        private IMessageConverter messageConverter;
        private readonly int id;

        private int consumerCounter;
        private int producerCounter;
        private long nextDeliveryId;
        private long lastDeliveredSequenceId;
        protected bool disposed = false;
        protected bool closed = false;
        protected bool closing = false;
        private TimeSpan disposeStopTimeout = TimeSpan.FromMilliseconds(30000);
        private TimeSpan closeStopTimeout = TimeSpan.FromMilliseconds(Timeout.Infinite);
        private TimeSpan requestTimeout;

        public Session(Connection connection, int sessionId, AcknowledgementMode acknowledgementMode)
        {
            this.connection = connection;
            this.acknowledgementMode = acknowledgementMode;
            MessageConverter = connection.MessageConverter;
            id = sessionId;
            if (this.acknowledgementMode == AcknowledgementMode.Transactional)
            {
                // TODO: transactions
                throw new NotSupportedException("Transactions are not supported by Qpid/Amqp");
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected void Dispose(bool disposing)
        {
            if (this.disposed)
            {
                return;
            }

            try
            {
                // Force a Stop when we are Disposing vs a Normal Close.
                Close();
            }
            catch
            {
                // Ignore network errors.
            }

            this.disposed = true;
        }

        public virtual void Close()
        {
            if (!this.closed)
            {
                try
                {
                    Tracer.InfoFormat("Closing The Session with Id {0}", SessionId);
                    DoClose();
                    Tracer.InfoFormat("Closed The Session with Id {0}", SessionId);
                }
                catch (Exception ex)
                {
                    Tracer.ErrorFormat("Error closing Session with id {0} : {1}", SessionId, ex);
                }
            }
        }

        internal void DoClose()
        {
            Shutdown();
        }

        internal void Shutdown()
        {
            //Tracer.InfoFormat("Executing Shutdown on Session with Id {0}", this.info.SessionId);

            if (this.closed)
            {
                return;
            }

            lock (myLock)
            {
                if (this.closed || this.closing)
                {
                    return;
                }

                try
                {
                    this.closing = true;

                    // Stop all message deliveries from this Session
                    lock (consumers.SyncRoot)
                    {
                        foreach (MessageConsumer consumer in consumers.Values)
                        {
                            consumer.Shutdown();
                        }
                    }
                    consumers.Clear();

                    lock (producers.SyncRoot)
                    {
                        foreach (MessageProducer producer in producers.Values)
                        {
                            producer.Shutdown();
                        }
                    }
                    producers.Clear();

                    Connection.RemoveSession(this);
                }
                catch (Exception ex)
                {
                    Tracer.ErrorFormat("Error closing Session with Id {0} : {1}", SessionId, ex);
                }
                finally
                {
                    this.closed = true;
                    this.closing = false;
                }
            }
        }

        public IMessageProducer CreateProducer()
        {
            return CreateProducer(null);
        }

        public IMessageProducer CreateProducer(IDestination destination)
        {
            MessageProducer producer = null;
            try
            {
                Destination dest = null;
                if (destination != null)
                {
                    dest.Path = destination.ToString();
                }
                producer = DoCreateMessageProducer(dest);

                this.AddProducer(producer);
            }
            catch (Exception)
            {
                if (producer != null)
                {
                    this.RemoveProducer(producer.ProducerId);
                    producer.Close();
                }

                throw;
            }

            return producer;
        }

        internal virtual MessageProducer DoCreateMessageProducer(Destination destination)
        {
            return new MessageProducer(this, GetNextProducerId(), destination);
        }

        public IMessageConsumer CreateConsumer(IDestination destination)
        {
            return CreateConsumer(destination, null, false);
        }

        public IMessageConsumer CreateConsumer(IDestination destination, string selector)
        {
            return CreateConsumer(destination, selector, false);
        }

        public IMessageConsumer CreateConsumer(IDestination destination, string selector, bool noLocal)
        {
            if (destination == null)
            {
                throw new InvalidDestinationException("Cannot create a Consumer with a Null destination");
            }

            MessageConsumer consumer = null;

            try
            {
                Destination dest = null;
                if (destination != null)
                {
                    dest.Path = destination.ToString();
                }
                consumer = DoCreateMessageConsumer(GetNextConsumerId(), dest, acknowledgementMode);

                consumer.ConsumerTransformer = this.ConsumerTransformer;

                this.AddConsumer(consumer);

                if (this.Connection.IsStarted)
                {
                    consumer.Start();
                }
            }
            catch (Exception)
            {
                if (consumer != null)
                {
                    this.RemoveConsumer(consumer);
                    consumer.Close();
                }

                throw;
            }

            return consumer;
        }


        public IMessageConsumer CreateDurableConsumer(ITopic destination, string name, string selector, bool noLocal)
        {
            throw new NotSupportedException("TODO: Durable Consumer");
        }

        internal virtual MessageConsumer DoCreateMessageConsumer(int id, Destination destination, AcknowledgementMode mode)
        {
            return new MessageConsumer(this, id, destination, mode);
        }

        public void DeleteDurableConsumer(string name)
        {
            throw new NotSupportedException("TODO: Durable Consumer");
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
            return new Queue(name);
        }

        public ITopic GetTopic(string name)
        {
            throw new NotSupportedException("TODO: Topic");
        }

        public ITemporaryQueue CreateTemporaryQueue()
        {
            throw new NotSupportedException("TODO: Temp queue");
        }

        public ITemporaryTopic CreateTemporaryTopic()
        {
            throw new NotSupportedException("TODO: Temp topic");
        }

        /// <summary>
        /// Delete a destination (Queue, Topic, Temp Queue, Temp Topic).
        /// </summary>
        public void DeleteDestination(IDestination destination)
        {
            // TODO: Implement if possible.  If not possible, then change exception to NotSupportedException().
            throw new NotImplementedException();
        }

        public IMessage CreateMessage()
        {
            BaseMessage answer = new BaseMessage();
            return answer;
        }


        public ITextMessage CreateTextMessage()
        {
            TextMessage answer = new TextMessage();
            return answer;
        }

        public ITextMessage CreateTextMessage(string text)
        {
            TextMessage answer = new TextMessage(text);
            return answer;
        }

        public IMapMessage CreateMapMessage()
        {
            return new MapMessage();
        }

        public IBytesMessage CreateBytesMessage()
        {
            return new BytesMessage();
        }

        public IBytesMessage CreateBytesMessage(byte[] body)
        {
            BytesMessage answer = new BytesMessage();
            answer.Content = body;
            return answer;
        }

        public IStreamMessage CreateStreamMessage()
        {
            return new StreamMessage();
        }

        public IObjectMessage CreateObjectMessage(Object body)
        {
            ObjectMessage answer = new ObjectMessage();
            answer.Body = body;
            return answer;
        }

        public void Commit()
        {
            throw new NotSupportedException("Transactions not supported by Qpid/Amqp");
        }

        public void Rollback()
        {
            throw new NotSupportedException("Transactions not supported by Qpid/Amqp");
        }

        public void Recover()
        {
            throw new NotSupportedException("Transactions not supported by Qpid/Amqp");
        }

        // Properties
        public Connection Connection
        {
            get { return connection; }
        }

        /// <summary>
        /// The default timeout for network requests.
        /// </summary>
        public TimeSpan RequestTimeout
        {
            get { return NMSConstants.defaultRequestTimeout; }
            set { }
        }

        public IMessageConverter MessageConverter
        {
            get { return messageConverter; }
            set { messageConverter = value; }
        }

        public bool Transacted
        {
            get { return acknowledgementMode == AcknowledgementMode.Transactional; }
        }

        public AcknowledgementMode AcknowledgementMode
        {
            get { throw new NotImplementedException(); }
        }

        private ConsumerTransformerDelegate consumerTransformer;
        public ConsumerTransformerDelegate ConsumerTransformer
        {
            get { return this.consumerTransformer; }
            set { this.consumerTransformer = value; }
        }

        private ProducerTransformerDelegate producerTransformer;
        public ProducerTransformerDelegate ProducerTransformer
        {
            get { return this.producerTransformer; }
            set { this.producerTransformer = value; }
        }

        public void AddConsumer(MessageConsumer consumer)
        {
            if (!this.closing)
            {
                // Registered with Connection before we register at the broker.
                consumers[consumer.ConsumerId] = consumer;
            }
        }

        public void RemoveConsumer(MessageConsumer consumer)
        {
            if (!this.closing)
            {
                consumers.Remove(consumer.ConsumerId);
            }
        }

        public void AddProducer(MessageProducer producer)
        {
            if (!this.closing)
            {
                this.producers[producer.ProducerId] = producer;
            }
        }

        public void RemoveProducer(int objectId)
        {
            if (!this.closing)
            {
                producers.Remove(objectId);
            }
        }

        public int GetNextConsumerId()
        {
            return Interlocked.Increment(ref consumerCounter);
        }

        public int GetNextProducerId()
        {
            return Interlocked.Increment(ref producerCounter);
        }

        public int SessionId
        {
            get { return id; }
        }
        
        #region Transaction State Events

        public event SessionTxEventDelegate TransactionStartedListener;
        public event SessionTxEventDelegate TransactionCommittedListener;
        public event SessionTxEventDelegate TransactionRolledBackListener;

        #endregion

    }
}
