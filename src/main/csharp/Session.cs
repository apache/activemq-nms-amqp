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
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Reflection;
using Apache.NMS;
using Apache.NMS.Util;
using Amqp.Framing;
using Apache.NMS.AMQP.Util;
using Apache.NMS.AMQP.Message;
using Apache.NMS.AMQP.Message.Factory;


namespace Apache.NMS.AMQP
{
    
    enum SessionState
    {
        UNKNOWN,
        INITIAL,
        BEGINSENT,
        OPENED,
        ENDSENT,
        CLOSED
    }

    /// <summary>
    /// Apache.NMS.AMQP.Session facilitates management and creates the underlying Amqp.Session protocol engine object.
    /// Apache.NMS.AMQP.Session is also a Factory for Apache.NMS.AMQP.MessageProcuder, Apache.NMS.AMQP.MessageConsumer, Apache.NMS.AMQP.Message.Message,
    /// Apache.NMS.AMQP.Destination, etc.
    /// </summary>
    class Session : NMSResource<SessionInfo>, ISession
    {

        private Connection connection;
        private Amqp.ISession impl;
        private Dictionary<string, MessageConsumer> consumers;
        private Dictionary<string, MessageProducer> producers;
        private SessionInfo sessInfo;
        private CountDownLatch responseLatch;
        private Atomic<SessionState> state = new Atomic<SessionState>(SessionState.INITIAL);
        private DispatchExecutor dispatcher;
        private readonly IdGenerator prodIdGen;
        private readonly IdGenerator consIdGen;
        private bool recovered = false;

        #region Constructor
        
        internal Session(Connection conn)
        {
            consumers = new Dictionary<string, MessageConsumer>();
            producers = new Dictionary<string, MessageProducer>();
            sessInfo = new SessionInfo(conn.SessionIdGenerator.GenerateId());
            Info = sessInfo;
            dispatcher = new DispatchExecutor();
            this.Configure(conn);
            prodIdGen = new NestedIdGenerator("ID:producer", this.sessInfo.Id, true);
            consIdGen = new NestedIdGenerator("ID:consumer", this.sessInfo.Id, true);
            
        }
        
        #endregion

        #region Internal Properties

        internal Connection Connection { get => this.connection;  }

        internal IdGenerator ProducerIdGenerator
        {
            get { return prodIdGen; }
        }

        internal IdGenerator ConsumerIdGenerator
        {
            get { return consIdGen; }
        }

        internal Amqp.ISession InnerSession { get { return this.impl; } }
        
        internal string SessionId
        {
            get
            {
                return sessInfo.sessionId;
            }
        }

        internal StringDictionary Properties
        {
            get { return PropertyUtil.GetProperties(this.sessInfo); }
        }
        
        internal DispatchExecutor Dispatcher {  get { return dispatcher; } }

        private object ThisProducerLock { get { return producers; } }
        private object ThisConsumerLock { get { return consumers; } }

        internal bool IsClosed { get => this.state.Value.Equals(SessionState.CLOSED); }

        #endregion

        #region Internal/Private Methods

        internal void Configure(Connection conn)
        {
            this.connection = conn;
            
            PropertyUtil.SetProperties(this.sessInfo, conn.Properties);
            this.RequestTimeout = conn.RequestTimeout;
            sessInfo.maxHandle = conn.MaxChannel;
            
        }
        
        internal void OnException(Exception e)
        {
            Connection.OnException(e);
        }

        internal bool IsDestinationInUse(IDestination destination)
        {
            MessageConsumer[] messageConsumers = consumers.Values.ToArray();
            foreach(MessageConsumer consumer in messageConsumers)
            {
                if (consumer.IsUsingDestination(destination))
                {
                    return true;
                }
            }
            return false;
        }

        internal bool ContainsSubscriptionName(string name)
        {
            MessageConsumer[] messageConsumers = consumers.Values.ToArray();
            foreach (MessageConsumer consumer in messageConsumers)
            {
                if (consumer.HasSubscription(name))
                {
                    return true;
                }
            }
            return false;
        }

        internal void Remove (MessageLink link)
        {
            if (link is MessageConsumer)
            {
                lock (ThisConsumerLock)
                {
                    consumers.Remove(link.Id.ToString());
                }
            }
            else if (link is MessageProducer)
            {
                lock (ThisProducerLock)
                {
                    producers.Remove(link.Id.ToString());
                }
            }
        }

        internal bool IsRecovered { get => recovered; }

        internal void ClearRecovered() { recovered = false; }

        internal void Acknowledge(AckType ackType)
        {
            MessageConsumer[] consumers = null;
            lock(ThisConsumerLock)
            {
                consumers = this.consumers.Values.ToArray();
            }
            foreach(MessageConsumer mc in consumers)
            {
                mc.Acknowledge(ackType);
            }
        }

        internal void Begin()
        {
            if (Connection.IsConnected && this.state.CompareAndSet(SessionState.INITIAL, SessionState.BEGINSENT))
            {
                this.responseLatch = new CountDownLatch(1);
                this.impl = new Amqp.Session(Connection.InnerConnection as Amqp.Connection, this.CreateBeginFrame(), this.OnBeginResp);
                impl.AddClosedCallback(OnInternalClosed);
                SessionState finishedState = SessionState.UNKNOWN;
                try
                {
                    bool received = true;
                    if(sessInfo.requestTimeout <= 0)
                    {
                        this.responseLatch.await();
                    }
                    else
                    {
                        received = this.responseLatch.await(TimeSpan.FromMilliseconds(sessInfo.requestTimeout));
                    }
                        
                        
                    if (received && this.impl.Error == null)
                    {
                        finishedState = SessionState.OPENED;
                    }
                    else
                    {
                        finishedState = SessionState.INITIAL;
                        if (!received)
                        {
                            Tracer.InfoFormat("Session {0} Begin timeout in {1}ms", sessInfo.nextOutgoingId, sessInfo.requestTimeout);
                            throw ExceptionSupport.GetTimeoutException(this.impl, "Performative Begin Timeout while waiting for response.");
                        }
                        else 
                        {
                            Tracer.InfoFormat("Session {0} Begin error: {1}", sessInfo.nextOutgoingId, this.impl.Error);
                            throw ExceptionSupport.GetException(this.impl, "Performative Begin Error.");
                        }
                    }
                }
                finally
                {
                    this.responseLatch = null;
                    this.state.GetAndSet(finishedState);
                    if(finishedState != SessionState.OPENED && this.impl != null && !this.impl.IsClosed)
                    {
                        this.impl.Close();
                    }

                }
            }
        }

        protected void End()
        {
            Tracer.InfoFormat("End(Session {0}): Dispatcher {1}", SessionId, Dispatcher.Name);
            if (this.impl != null && this.state.CompareAndSet(SessionState.OPENED, SessionState.ENDSENT))
            {
                this.Stop();

                this.dispatcher?.Close();

                try
                {
                    if (!this.impl.IsClosed)
                    {
                        this.impl.Close(TimeSpan.FromMilliseconds(this.sessInfo.closeTimeout), null);
                    }
                }
                catch (TimeoutException tex)
                {
                    throw ExceptionSupport.GetTimeoutException(this.impl,
                        "Timeout for Amqp Session end for Session {0}. Cause {1}",
                        this.Id, tex.Message);
                }
                catch (Amqp.AmqpException amqpEx)
                {
                    throw ExceptionSupport.Wrap(amqpEx,
                        "Failed to end Amqp Session for Session {0}", this.Id);
                }
                finally
                {
                    if (this.state.CompareAndSet(SessionState.ENDSENT, SessionState.CLOSED))
                    {
                        this.Connection.Remove(this);
                    }
                    this.impl = null;
                    this.dispatcher = null;
                }
            }
        }

        private Begin CreateBeginFrame()
        {
            Begin begin = new Begin();
            
            begin.HandleMax = this.sessInfo.maxHandle;
            begin.IncomingWindow = this.sessInfo.incomingWindow;
            begin.OutgoingWindow = this.sessInfo.outgoingWindow;
            begin.NextOutgoingId = this.sessInfo.nextOutgoingId;

            return begin;
        }

        private void OnBeginResp(Amqp.ISession session, Begin resp)
        {
            Tracer.DebugFormat("Received Begin for Session {0}, Response: {1}", session, resp);
            
            this.sessInfo.remoteChannel = resp.RemoteChannel;
            this.responseLatch.countDown();
            
        }
        
        private void OnInternalClosed(Amqp.IAmqpObject sender, Error error)
        {
            string name = null;
            Connection parent = null;
            Session self = null;
            CountDownLatch latch = null;
            try
            {
                self = this;
                parent = this.Connection;
                name = this.Id.ToString();
                latch = this.responseLatch;
                if (self.state.CompareAndSet(SessionState.OPENED, SessionState.CLOSED))
                {
                    // unexpected close or parent close.
                    parent.Remove(self);
                    if (IsStarted)
                    {
                        // unexpected close
                        this.Shutdown();
                    }
                    else
                    {
                        // parent close
                        self.dispatcher?.Shutdown();
                    }
                }
                else if (self.state.CompareAndSet(SessionState.ENDSENT, SessionState.CLOSED))
                {
                    // application close.
                    // cleanup parent reference
                    parent.Remove(self);
                    // non-blocking close for event dispatch executor.
                    self.dispatcher?.Shutdown();
                }
            }
            catch (Exception ex)
            {
                Tracer.DebugFormat("Caught Exception during Amqp Session close for NMS Session{0}. Exception {1}", 
                    name != null ? (" " + name) : "", ex);
            }

            if (error != null)
            {
                Tracer.WarnFormat("Session{0} Unexpectedly closed with error: {1}", name != null ? (" " + name) : "", error);
                latch?.countDown();
            }

        }

        private IMessageConsumer DoCreateConsumer(IDestination destination, string name, string selector, bool noLocal)
        {
            this.ThrowIfClosed();
            if (destination.IsTemporary)
            {
                if(!(destination is TemporaryDestination))
                {
                    throw new InvalidDestinationException(
                        String.Format("Cannot create consumer with temporary Destination from another provider."));
                }
                else if(!this.Connection.Equals((destination as TemporaryDestination).Connection))
                {
                    throw new InvalidDestinationException(
                        String.Format("Temporary Destiantion {0} does not belong to connection {1}", 
                        destination.ToString(), this.Connection.ClientId));
                }
            }
            MessageConsumer consumer = new MessageConsumer(this, destination, name, selector, noLocal);
            try
            {
                consumer.Attach();
            }
            catch (NMSException) { throw; }
            catch (Exception ex)
            {
                throw ExceptionSupport.Wrap(ex, "Failed to establish link for Consumer {0} with destination {1}.", consumer.ConsumerId, destination.ToString());
            }

            lock (ThisConsumerLock)
            {
                consumers.Add(consumer.ConsumerId.ToString(), consumer);
            }
            if (IsStarted)
            {
                consumer.Start();
            }

            return consumer;
        }

        #endregion

        #region NMSResource Methods

        protected override void ThrowIfClosed()
        {
            if (state.Value.Equals(SessionState.CLOSED))
            {
                throw new IllegalStateException("Invalid Operation on Closed session.");
            }
        }

        protected override void StartResource()
        {
            this.Begin();

            // start dispatch thread.
            dispatcher.Start();

            // start all producers and consumers here
            lock (ThisProducerLock)
            {
                foreach (MessageProducer p in producers.Values)
                {
                    p.Start();
                }
            }
            lock (ThisConsumerLock)
            {
                foreach (MessageConsumer c in consumers.Values)
                {
                    c.Start();
                }
            }
            
        }

        protected override void StopResource()
        {
            // stop all producers and consumers here
            
            lock (ThisProducerLock)
            {
                foreach (MessageProducer p in producers.Values)
                {
                    p.Stop();
                }
            }
            lock (ThisConsumerLock)
            {
                foreach (MessageConsumer c in consumers.Values)
                {
                    c.Stop();
                }
            }
            dispatcher.Stop();
        }

        #endregion

        #region ISession Property Fields

        /// <summary>
        /// See <seealso cref="ISession.AcknowledgementMode"/>.
        /// <para>
        /// Throws <see cref="NotImplementedException"/> for <see cref="AcknowledgementMode.Transactional"/>.
        /// </para>
        /// </summary>
        public AcknowledgementMode AcknowledgementMode
        {
            get
            {
                return sessInfo.ackMode;
            }
            internal set
            {
                if(value.Equals(AcknowledgementMode.Transactional))
                {
                    throw new NotImplementedException("Amqp Provider does not Implement Transactinal AcknoledgementMode.");
                }
                else
                {
                    sessInfo.ackMode = value;
                }
            }
        }

        /// <summary>
        /// Not Implemented, throws <see cref="NotImplementedException"/>.
        /// </summary>
        public ConsumerTransformerDelegate ConsumerTransformer
        {
            get
            {
                throw new NotImplementedException();
            }

            set
            {
                throw new NotImplementedException();
            }
        }

        /// <summary>
        /// Not Implemented, throws <see cref="NotImplementedException"/>.
        /// </summary>
        public ProducerTransformerDelegate ProducerTransformer
        {
            get
            {
                throw new NotImplementedException();
            }

            set
            {
                throw new NotImplementedException();
            }
        }

        public TimeSpan RequestTimeout
        {
            get
            {
                return TimeSpan.FromMilliseconds(this.sessInfo.requestTimeout);
            }

            set
            {
                sessInfo.requestTimeout = Convert.ToInt64(value.TotalMilliseconds);
            }
        }

        public bool Transacted
        {
            get
            {
                return false;
            }
        }

        #endregion

        #region ISession Events

        /// <summary>
        /// Not Implemented, throws <see cref="NotImplementedException"/>.
        /// </summary>
        public event SessionTxEventDelegate TransactionCommittedListener
        {
            add => throw new NotImplementedException("AMQP Provider does not implement transactions.");
            remove => throw new NotImplementedException("AMQP Provider does not implement transactions.");
        }

        /// <summary>
        /// Not Implemented, throws <see cref="NotImplementedException"/>.
        /// </summary>
        public event SessionTxEventDelegate TransactionRolledBackListener
        {
            add => throw new NotImplementedException("AMQP Provider does not implement transactions.");
            remove => throw new NotImplementedException("AMQP Provider does not implement transactions.");
        }

        /// <summary>
        /// Not Implemented, throws <see cref="NotImplementedException"/>.
        /// </summary>
        public event SessionTxEventDelegate TransactionStartedListener
        {
            add => throw new NotImplementedException("AMQP Provider does not implement transactions.");
            remove => throw new NotImplementedException("AMQP Provider does not implement transactions.");
        }

        #endregion

        #region ISession Methods
        
        public IQueueBrowser CreateBrowser(IQueue queue)
        {
            throw new NotImplementedException();
        }

        public IQueueBrowser CreateBrowser(IQueue queue, string selector)
        {
            throw new NotImplementedException();
        }

        public IBytesMessage CreateBytesMessage()
        {
            ThrowIfClosed();
            return MessageFactory<ConnectionInfo>.Instance(Connection).CreateBytesMessage();
        }

        public IBytesMessage CreateBytesMessage(byte[] body)
        {
            IBytesMessage msg = CreateBytesMessage();
            msg.WriteBytes(body);
            return msg;
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
            return DoCreateConsumer(destination, null, selector, noLocal);
        }

        public IMessageConsumer CreateDurableConsumer(ITopic destination, string name, string selector, bool noLocal)
        {
            this.ThrowIfClosed();
            if (Connection.ContainsSubscriptionName(name))
            {
                throw new NMSException(string.Format("The subscription name {0} must be unique to a client's Id {1}", name, Connection?.ClientId), NMSErrorCode.INTERNAL_ERROR);
            }
            return DoCreateConsumer(destination, name, selector, noLocal);
        }
        
        public IMapMessage CreateMapMessage()
        {
            this.ThrowIfClosed();
            return Connection.MessageFactory.CreateMapMessage();
        }

        public IMessage CreateMessage()
        {
            this.ThrowIfClosed();
            return Connection.MessageFactory.CreateMessage();
        }

        public IObjectMessage CreateObjectMessage(object body)
        {
            this.ThrowIfClosed();
            return Connection.MessageFactory.CreateObjectMessage(body);
        }

        /// <summary>
        /// Creates an Anonymous Producer. This is equivalent to calling <see cref="ISession"/>.CreateProducer(null).
        /// </summary>
        /// <returns>An Anonymous <see cref="IMessageProducer"/>.</returns>
        public IMessageProducer CreateProducer()
        {
            return CreateProducer(null);
        }

        /// <summary>
        /// See <seealso cref="ISession.CreateProducer(IDestination)"/>.
        /// </summary>
        /// <param name="destination">The destination to create the producer on. Can be null for anonymous producers.</param>
        /// <returns><see cref="IMessageProducer"/> to send messages on.</returns>
        public IMessageProducer CreateProducer(IDestination destination)
        {
            ThrowIfClosed();
            if(destination == null && !Connection.IsAnonymousRelay)
            {
                throw new NotImplementedException("Anonymous producers are only supported with Anonymous-Relay-Node Connections.");
            }
            MessageProducer prod = new MessageProducer(this, destination);
            try
            {
                prod.Attach();
            }
            catch(NMSException) { throw; }
            catch(Exception ex)
            {
                throw ExceptionSupport.Wrap(ex, "Failed to Establish link for producer {0} with Destination {1}", prod.ProducerId, destination?.ToString() ?? "Anonymous");
            }
            lock (ThisProducerLock)
            {
                //Todo Fix adding multiple producers
                producers.Add(prod.ProducerId.ToString(), prod);
            }
            if (IsStarted)
            {
                prod.Start();
            }
            return prod;
        }

        public IStreamMessage CreateStreamMessage()
        {
            this.ThrowIfClosed();
            return Connection.MessageFactory.CreateStreamMessage();
        }

        public ITemporaryQueue CreateTemporaryQueue()
        {
            this.ThrowIfClosed();
            return Connection.CreateTemporaryQueue();
        }

        public ITemporaryTopic CreateTemporaryTopic()
        {
            this.ThrowIfClosed();
            return Connection.CreateTemporaryTopic();
        }

        public ITextMessage CreateTextMessage()
        {
            ThrowIfClosed();
            return Connection.MessageFactory.CreateTextMessage();
        }

        public ITextMessage CreateTextMessage(string text)
        {
            ITextMessage msg = CreateTextMessage();
            msg.Text = text;
            return msg;
        }

        /// <summary>
        /// Delete a destination (Temp Queue, Temp Topic). 
        /// This is equivalent to calling the <see cref="ITemporaryTopic"/> or <see cref="ITemporaryQueue"/> delete method.
        /// Queue, Topic, destinations are not supported for deletion.
        /// Destinations of type Queue or Topic will throw <see cref="NotSupportedException"/>.
        /// </summary>
        /// <param name="destination">The destination to delete.</param>
        public void DeleteDestination(IDestination destination)
        {
            this.ThrowIfClosed();
            if (destination == null)
            {
                return;
            }
            if (destination is TemporaryDestination)
            {
                (destination as TemporaryDestination).Delete();
            }
            else if(destination is ITemporaryQueue)
            {
                (destination as ITemporaryQueue).Delete();
            }
            else if(destination is ITemporaryTopic)
            {
                (destination as ITemporaryTopic).Delete();
            }
            else
            {
                throw new NotSupportedException("AMQP can not delete a Queue or Topic destination.");
            }
        }

        public void DeleteDurableConsumer(string name)
        {
            this.ThrowIfClosed();
            this.Connection.Unsubscribe(name);
        }

        public IQueue GetQueue(string name)
        {
            this.ThrowIfClosed();
            return new Queue(Connection, name);
        }

        public ITopic GetTopic(string name)
        {
            this.ThrowIfClosed();
            return new Topic(Connection, name);
        }

        public void Commit()
        {
            throw new NotImplementedException();
        }

        public void Recover()
        {
            this.ThrowIfClosed();
            recovered = true;
            MessageConsumer[] consumers = this.consumers.Values.ToArray();
            foreach(MessageConsumer mc in consumers)
            {
                mc.Recover();
            }
        }

        public void Rollback()
        {
            throw new NotImplementedException();
        }

        #endregion

        #region IDisposable Methods

        internal void CheckOnDispatchThread()
        {
            if (!IsClosed && Dispatcher != null && Dispatcher.IsOnDispatchThread)
            {
                throw new IllegalStateException("Session " + SessionId + " can not closed From MessageListener.");
            }
        }

        internal void Shutdown()
        {
            // stop all producers and consumers here

            lock (ThisProducerLock)
            {
                foreach (MessageProducer p in producers.Values)
                {
                    p.Shutdown();
                }
            }
            lock (ThisConsumerLock)
            {
                foreach (MessageConsumer c in consumers.Values)
                {
                    c.Shutdown();
                }
            }
            dispatcher?.Shutdown();
        }

        protected virtual void Dispose(bool disposing)
        {
            if (IsClosed) return;
            if (disposing)
            {
                CheckOnDispatchThread();
                try
                {
                    this.End();
                }
                catch (Exception ex)
                {
                    NMSException nmse = ExceptionSupport.Wrap(ex,"Amqp Session end failure for NMS Session {0}", this.Id);
                    Tracer.DebugFormat("Caught Exception while closing Session {0}. Exception {1}", this.Id, nmse);
                }
                
            }
        }

        public void Close()
        {
            Dispose(true);
            if (IsClosed)
            {
                GC.SuppressFinalize(this);
            }
        }

        public void Dispose()
        {
            try
            {
                this.Close();
            }
            catch (Exception ex)
            {
                Tracer.DebugFormat("Caught exception while disposing {0} {1}. Exception {2}", this.GetType().Name, this.Id, ex);
            }
        }

        #endregion


    }

    #region SessionInfo Class

    internal class SessionInfo : ResourceInfo
    {
        private static readonly uint DEFAULT_INCOMING_WINDOW;
        private static readonly uint DEFAULT_OUTGOING_WINDOW;

        static SessionInfo()
        {
            DEFAULT_INCOMING_WINDOW = 1024 * 10 - 1;
            DEFAULT_OUTGOING_WINDOW = uint.MaxValue - 2u;
        }
        
        internal SessionInfo(Id sessionId) : base(sessionId)
        {
            ulong endId = (ulong)sessionId.GetLastComponent(typeof(ulong));
            nextOutgoingId = Convert.ToUInt16(endId);
        }
        
        public string sessionId { get { return Id.ToString(); } }

        public AcknowledgementMode ackMode { get; set; }
        public ushort remoteChannel { get; internal set; }
        public uint nextOutgoingId { get; internal set; }
        public uint incomingWindow { get; set; } = DEFAULT_INCOMING_WINDOW;
        public uint outgoingWindow { get; set; } = DEFAULT_OUTGOING_WINDOW;
        public uint maxHandle { get; set; }
        public bool isTransacted { get => false;  set { } }
        public long requestTimeout { get; set; }
        public int closeTimeout { get; set; }
        public long sendTimeout { get; set; }

        public override string ToString()
        {
            string result = "";
            result += "sessInfo = [\n";
            foreach (MemberInfo info in this.GetType().GetMembers())
            {
                if (info is PropertyInfo)
                {
                    PropertyInfo prop = info as PropertyInfo;
                    if (prop.GetGetMethod(true).IsPublic)
                    {
                        result += string.Format("{0} = {1},\n", prop.Name, prop.GetValue(this, null));
                    }
                }
            }
            result = result.Substring(0, result.Length - 2) + "\n]";
            return result;
        }
    }

    #endregion

}
