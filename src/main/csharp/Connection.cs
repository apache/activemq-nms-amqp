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
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.NMS;
using Amqp;
using Amqp.Framing;
using Apache.NMS.AMQP.Util;
using Apache.NMS.AMQP.Transport;
using System.Reflection;
using Apache.NMS.Util;

namespace Apache.NMS.AMQP
{
    using Message.Factory;
    enum ConnectionState
    {
        UNKNOWN = -1,
        INITIAL = 0,
        CONNECTING = 1,
        CONNECTED = 2,
        CLOSING = 3,
        CLOSED = 4,

    }

    /// <summary>
    /// Apache.NMS.AMQP.Connection facilitates management and creates the underlying Amqp.Connection protocol engine object.
    /// Apache.NMS.AMQP.Connection is also the NMS.AMQP.Session Factory.
    /// </summary>
    class Connection : NMSResource<ConnectionInfo>, Apache.NMS.IConnection
    {
        public static readonly string MESSAGE_OBJECT_SERIALIZATION_PROP = PropertyUtil.CreateProperty("Message.Serialization", ConnectionFactory.ConnectionPropertyPrefix);
        
        private IRedeliveryPolicy redeliveryPolicy;
        private Amqp.IConnection impl;
        private ProviderCreateConnection implCreate;
        private ConnectionInfo connInfo;
        private readonly IdGenerator clientIdGenerator;
        private Atomic<bool> clientIdCanSet = new Atomic<bool>(true);
        private Atomic<bool> closing = new Atomic<bool>(false);
        private Atomic<ConnectionState> state = new Atomic<ConnectionState>(ConnectionState.INITIAL);
        private CountDownLatch latch;
        private ConcurrentDictionary<Id, Session> sessions = new ConcurrentDictionary<Id, Session>();
        private IdGenerator sesIdGen = null;
        private IdGenerator tempTopicIdGen = null;
        private IdGenerator tempQueueIdGen = null;
        private StringDictionary properties;
        private TemporaryLinkCache temporaryLinks = null;
        private IProviderTransportContext transportContext = null;
        private DispatchExecutor exceptionExecutor = null;

        #region Contructor

        internal Connection(Uri addr, IdGenerator clientIdGenerator)
        {
            connInfo = new ConnectionInfo();
            connInfo.remoteHost = addr;
            Info = connInfo;
            this.clientIdGenerator = clientIdGenerator;
            latch = new CountDownLatch(1);
            temporaryLinks = new TemporaryLinkCache(this);
        }
        
        #endregion

        #region Internal Properties

        internal Amqp.IConnection InnerConnection { get { return this.impl; } }

        internal IdGenerator SessionIdGenerator
        {
            get
            {
                IdGenerator sig = sesIdGen;
                lock (this)
                {
                    if (sig == null)
                    {
                        sig = new NestedIdGenerator("ID:ses", connInfo.Id, true);
                        sesIdGen = sig;
                    }
                }
                return sig;
            }
        }

        internal IdGenerator TemporaryTopicGenerator
        {
            get
            {
                IdGenerator ttg = tempTopicIdGen;
                lock (this)
                {
                    if (ttg == null)
                    {
                        ttg = new NestedIdGenerator("ID:nms-temp-topic", Info.Id, true);
                        tempTopicIdGen = ttg;
                    }
                }
                return ttg;
            }
        }

        internal IdGenerator TemporaryQueueGenerator
        {
            get
            {
                IdGenerator tqg = tempQueueIdGen;
                lock (this)
                {
                    if (tqg == null)
                    {
                        tqg = new NestedIdGenerator("ID:nms-temp-queue", Info.Id, true);
                        tempQueueIdGen = tqg;
                    }
                }
                return tqg;
            }
        }

        internal bool IsConnected
        {
            get
            {
                return this.state.Value.Equals(ConnectionState.CONNECTED);
            }
        }

        internal bool IsClosed
        {
            get
            {
                return this.state.Value.Equals(ConnectionState.CLOSED);
            }
        }

        internal ushort MaxChannel
        {
            get { return connInfo.channelMax; }
        }

        internal MessageTransformation TransformFactory
        {
            get
            {
                return MessageFactory<ConnectionInfo>.Instance(this).GetTransformFactory();
            }
        }

        internal IMessageFactory MessageFactory
        {
            get
            {
                return MessageFactory<ConnectionInfo>.Instance(this);
            }
        }

        internal string TopicPrefix
        {
            get { return connInfo.TopicPrefix; }
        }

        internal string QueuePrefix
        {
            get { return connInfo.QueuePrefix; }
        }

        internal bool IsAnonymousRelay
        {
            get { return connInfo.IsAnonymousRelay; }
        }

        internal bool IsDelayedDelivery
        {
            get { return connInfo.IsDelayedDelivery; }
        }
        
        #endregion

        #region Internal Methods

        internal ITemporaryTopic CreateTemporaryTopic()
        {
            TemporaryTopic temporaryTopic = new TemporaryTopic(this);

            CreateTemporaryLink(temporaryTopic);

            return temporaryTopic;
        }

        internal ITemporaryQueue CreateTemporaryQueue()
        {
            TemporaryQueue temporaryQueue = new TemporaryQueue(this);

            CreateTemporaryLink(temporaryQueue);

            return temporaryQueue;
        }

        private void CreateTemporaryLink(TemporaryDestination temporaryDestination)
        {
            TemporaryLink link = new TemporaryLink(temporaryLinks.Session, temporaryDestination);

            link.Attach();

            temporaryLinks.AddLink(temporaryDestination, link);

        }

        /// <summary>
        /// Unsubscribes Durable Consumers on the connection
        /// </summary>
        /// <param name="name">The subscription name.</param>
        internal void Unsubscribe(string name)
        {
            // check for any active consumers on the subscription name.
            foreach (Session session in GetSessions())
            {
                if (session.ContainsSubscriptionName(name))
                {
                    throw new IllegalStateException("Cannot unsubscribe from Durable Consumer while consuming messages.");
                }
            }
            // unsubscribe using an instance of RemoveSubscriptionLink.
            RemoveSubscriptionLink removeLink = new RemoveSubscriptionLink(this.temporaryLinks.Session, name);
            removeLink.Unsubscribe();
        }

        internal bool ContainsSubscriptionName(string name)
        {
            foreach (Session session in GetSessions())
            {
                if (session.ContainsSubscriptionName(name))
                {
                    return true;
                }
            }
            return false;
        }

        internal void Configure(ConnectionFactory cf)
        {
            Amqp.ConnectionFactory cfImpl = cf.Factory as Amqp.ConnectionFactory;
            
            // get properties from connection factory
            StringDictionary properties = cf.ConnectionProperties;

            // apply connection properties to connection factory and connection info.
            PropertyUtil.SetProperties(cfImpl.AMQP, properties, ConnectionFactory.ConnectionPropertyPrefix);
            PropertyUtil.SetProperties(connInfo, properties, ConnectionFactory.ConnectionPropertyPrefix);
            
            // create copy of transport context
            this.transportContext = cf.Context.Copy();

            // Store raw properties for future objects
            this.properties = PropertyUtil.Clone(properties);
            
            // Create Connection builder delegate.
            this.implCreate = this.transportContext.CreateConnectionBuilder();
        }

        internal StringDictionary Properties
        {
            get { return PropertyUtil.Merge(this.properties, PropertyUtil.GetProperties(this.connInfo)); }
        }

        internal void Remove(TemporaryDestination destination)
        {
            temporaryLinks.RemoveLink(destination);
        }

        internal void DestroyTemporaryDestination(TemporaryDestination destination)
        {
            ThrowIfClosed();
            foreach(Session session in GetSessions())
            {
                if (session.IsDestinationInUse(destination))
                {
                    throw new IllegalStateException("Cannot delete Temporary Destination, {0}, while consuming messages.");
                }
            }
            try
            {
                TemporaryLink link = temporaryLinks.RemoveLink(destination);
                if(link != null && !link.IsClosed)
                {
                    link.Close();
                }
            }
            catch (Exception e)
            {
                throw ExceptionSupport.Wrap(e);
            }
        }

        internal void Remove(Session ses)
        {
            Session result = null;
            if(!sessions.TryRemove(ses.Id, out result))
            {
                Tracer.WarnFormat("Could not disassociate Session {0} with Connection {1}.", ses.Id, ClientId);
            }
        }

        private Session[] GetSessions()
        {   
            return sessions.Values.ToArray();
        }

        private void CheckIfClosed()
        {
            if (this.state.Value.Equals(ConnectionState.CLOSED))
            {
                throw new IllegalStateException("Operation invalid on closed connection.");
            }
        }

        private void ProcessCapabilities(Open openResponse)
        {
            if(openResponse.OfferedCapabilities != null || openResponse.OfferedCapabilities.Length > 0)
            {
                foreach(Amqp.Types.Symbol symbol in openResponse.OfferedCapabilities)
                {
                    if (SymbolUtil.OPEN_CAPABILITY_ANONYMOUS_RELAY.Equals(symbol))
                    {
                        connInfo.IsAnonymousRelay = true;
                    }
                    else if (SymbolUtil.OPEN_CAPABILITY_DELAYED_DELIVERY.Equals(symbol))
                    {
                        connInfo.IsDelayedDelivery = true;
                    }
                    else
                    {
                        connInfo.AddCapability(symbol);
                    }
                }
            }
        }

        private void ProcessRemoteConnectionProperties(Open openResponse)
        {
            if (openResponse.Properties != null && openResponse.Properties.Count > 0)
            {
                foreach(object key in openResponse.Properties.Keys)
                {
                    string keyString = key.ToString();
                    string valueString = openResponse.Properties[key]?.ToString();
                    this.connInfo.RemotePeerProperies.Add(keyString, valueString);
                }
            }
        }

        private void OpenResponse(Amqp.IConnection conn, Open openResp)
        {
            Tracer.InfoFormat("Connection {0}, Open {0}", conn.ToString(), openResp.ToString());
            Tracer.DebugFormat("Open Response : \n Hostname = {0},\n ContainerId = {1},\n MaxChannel = {2},\n MaxFrame = {3}\n", openResp.HostName, openResp.ContainerId, openResp.ChannelMax, openResp.MaxFrameSize);
            Tracer.DebugFormat("Open Response Descriptor : \n Descriptor Name = {0},\n Descriptor Code = {1}\n", openResp.Descriptor.Name, openResp.Descriptor.Code);
            ProcessCapabilities(openResp);
            ProcessRemoteConnectionProperties(openResp);
            if (SymbolUtil.CheckAndCompareFields(openResp.Properties, SymbolUtil.CONNECTION_ESTABLISH_FAILED, SymbolUtil.BOOLEAN_TRUE))
            {
                Tracer.InfoFormat("Open response contains {0} property the connection {1} will soon be closed.", SymbolUtil.CONNECTION_ESTABLISH_FAILED, this.ClientId);
            }
            else
            {
                object value = SymbolUtil.GetFromFields(openResp.Properties, SymbolUtil.CONNECTION_PROPERTY_TOPIC_PREFIX);
                if(value != null && value is string)
                {
                    this.connInfo.TopicPrefix = value as string;
                }
                value = SymbolUtil.GetFromFields(openResp.Properties, SymbolUtil.CONNECTION_PROPERTY_QUEUE_PREFIX);
                if (value != null && value is string)
                {
                    this.connInfo.QueuePrefix = value as string;
                }
                this.latch?.countDown();
            }
        }

        private Open CreateOpenFrame(ConnectionInfo connInfo)
        {
            Open frame = new Open();
            frame.ContainerId = connInfo.clientId;
            frame.ChannelMax = connInfo.channelMax;
            frame.MaxFrameSize = Convert.ToUInt32(connInfo.maxFrameSize);
            frame.HostName = connInfo.remoteHost.Host;
            frame.IdleTimeOut = Convert.ToUInt32(connInfo.idleTimout);
            frame.DesiredCapabilities = new Amqp.Types.Symbol[] {
                SymbolUtil.OPEN_CAPABILITY_SOLE_CONNECTION_FOR_CONTAINER,
                SymbolUtil.OPEN_CAPABILITY_DELAYED_DELIVERY,
                SymbolUtil.OPEN_CAPABILITY_ANONYMOUS_RELAY
            };

            return frame;
        }

        internal void Connect()
        {
            if (this.state.CompareAndSet(ConnectionState.INITIAL, ConnectionState.CONNECTING))
            {
                Address addr = UriUtil.ToAddress(connInfo.remoteHost, connInfo.username, connInfo.password ?? string.Empty);
                Tracer.InfoFormat("Creating Address: {0}", addr.Host);
                if (this.clientIdCanSet.CompareAndSet(true, false))
                {
                    if (this.ClientId == null)
                    {
                        connInfo.ResourceId = this.clientIdGenerator.GenerateId();
                    }
                    else
                    {
                        connInfo.ResourceId = new Id(ClientId);
                    }
                    Tracer.InfoFormat("Staring Connection with Client Id : {0}", this.ClientId);
                }
                
                Open openFrame = CreateOpenFrame(this.connInfo);
                
                Task<Amqp.Connection> fconn = this.implCreate(addr, openFrame, this.OpenResponse);
                // wait until the Open request is sent
                this.impl = TaskUtil.Wait(fconn, connInfo.connectTimeout);
                if(fconn.Exception != null)
                {
                    // exceptions thrown from TaskUtil are typically System.AggregateException and are usually transport exceptions for secure transport.
                    // extract the innerException of interest and wrap it as an NMS exception
                    if (fconn.Exception is AggregateException)
                    {
                        throw ExceptionSupport.Wrap(fconn.Exception.InnerException,
                               "Failed to connect host {0}. Cause: {1}", openFrame.HostName, fconn.Exception.InnerException?.Message ?? fconn.Exception.Message);
                    }
                    else {
                        throw ExceptionSupport.Wrap(fconn.Exception, 
                               "Failed to connect host {0}. Cause: {1}", openFrame.HostName, fconn.Exception?.Message);
                    }
                }

                this.impl.Closed += OnInternalClosed;
                this.impl.AddClosedCallback(OnInternalClosed);
                this.latch = new CountDownLatch(1);

                ConnectionState finishedState = ConnectionState.UNKNOWN;
                // Wait for Open response 
                try
                {
                    bool received = this.latch.await((this.Info.requestTimeout==0) ? Timeout.InfiniteTimeSpan : this.RequestTimeout);
                    if (received && this.impl.Error == null && fconn.Exception == null)
                    {

                        Tracer.InfoFormat("Connection {0} has connected.", this.impl.ToString());
                        finishedState = ConnectionState.CONNECTED;
                        // register connection factory once client Id accepted.
                        MessageFactory<ConnectionInfo>.Register(this);
                    }
                    else
                    {
                        if (!received)
                        {
                            // Timeout occured waiting on response
                            Tracer.InfoFormat("Connection Response Timeout. Failed to receive response from {0} in {1}ms", addr.Host, this.Info.requestTimeout);
                        }
                        finishedState = ConnectionState.INITIAL;
                        
                        if (fconn.Exception == null)
                        {
                            if (!received) throw ExceptionSupport.GetTimeoutException(this.impl, "Connection {0} has failed to connect in {1}ms.", ClientId, connInfo.closeTimeout);
                            Tracer.ErrorFormat("Connection {0} has Failed to connect. Message: {1}", ClientId, (this.impl.Error == null ? "Unknown" : this.impl.Error.ToString()));

                            throw ExceptionSupport.GetException(this.impl, "Connection {0} has failed to connect.", ClientId);
                        }
                        else
                        {
                            throw ExceptionSupport.Wrap(fconn.Exception, "Connection {0} failed to connect.", ClientId);
                        }

                    }
                }
                finally
                {
                    this.latch = null;
                    this.state.GetAndSet(finishedState);
                    if (finishedState != ConnectionState.CONNECTED)
                    {
                        this.impl.Close(TimeSpan.FromMilliseconds(connInfo.closeTimeout),null);
                    }
                }
            }
        }

        private void Shutdown()
        {
            foreach(Session s in GetSessions())
            {
                s.Shutdown();
            }
            // signals to the DispatchExecutor to stop enqueue exception notifications
            // and drain off remaining notifications.
            this.exceptionExecutor?.Shutdown();
        }

        private void OnInternalClosed(IAmqpObject sender, Error error)
        {
            string name = null;
            Connection self = null;
            try
            {
                self = this;
                // name should throw should the finalizer of the Connection object already completed.
                name = self.ClientId;
                Tracer.InfoFormat("Received Close Request for Connection {0}.", name);
                if (self.state.CompareAndSet(ConnectionState.CONNECTED, ConnectionState.CLOSED))
                {
                    // unexpected or amqp transport close.

                    // notify any error to exception Dispatcher.
                    if (error != null)
                    {
                        NMSException nmse = ExceptionSupport.GetException(error, "Connection {0} Closed.", name);
                        self.OnException(nmse);
                    }

                    // shutdown connection facilities.
                    if (self.IsStarted)
                    {
                        self.Shutdown();
                    }
                    MessageFactory<ConnectionInfo>.Unregister(self);
                }
                else if (self.state.CompareAndSet(ConnectionState.CLOSING, ConnectionState.CLOSED))
                {
                    // application close.
                    MessageFactory<ConnectionInfo>.Unregister(self);
                }
                self.latch?.countDown();
            }
            catch (Exception ex)
            {
                Tracer.DebugFormat("Caught Exception during Amqp Connection close for NMS Connection{0}. Exception {1}",
                    name != null ? (" " + name) : "", ex);
            }
        }

        private void Disconnect()
        {
            if(this.state.CompareAndSet(ConnectionState.CONNECTED, ConnectionState.CLOSING) && this.impl!=null)
            {
                Tracer.InfoFormat("Sending Close Request On Connection {0}.", ClientId);
                try
                {
                    if (!this.impl.IsClosed)
                    {
                        this.impl.Close(TimeSpan.FromMilliseconds(connInfo.closeTimeout), null);
                    }
                }
                catch (AmqpException amqpEx)
                {
                    throw ExceptionSupport.Wrap(amqpEx, "Error Closing Amqp Connection " + ClientId);
                }
                catch (TimeoutException tmoutEx)
                {
                    throw ExceptionSupport.GetTimeoutException(this.impl, "Timeout waiting for Amqp Connection {0} Close response. Message : {1}", ClientId, tmoutEx.Message);
                }
                finally
                {
                    if (this.state.CompareAndSet(ConnectionState.CLOSING, ConnectionState.CLOSED))
                    {
                        // connection cleanup.
                        MessageFactory<ConnectionInfo>.Unregister(this);
                        this.impl = null;
                    }
                    
                }
            }
        }
        
        protected DispatchExecutor ExceptionExecutor
        {
            get
            {
                if(exceptionExecutor == null && !IsClosed)
                {
                    exceptionExecutor = new DispatchExecutor(true);
                    exceptionExecutor.Start();
                }
                return exceptionExecutor;
            }
        }

        internal void OnException(Exception ex)
        {
            Apache.NMS.ExceptionListener listener = this.ExceptionListener;
            if(listener != null)
            {
                ExceptionNotification en = new ExceptionNotification(this, ex);
                this.ExceptionExecutor.Enqueue(en);
            }
        }

        protected void DispatchException(Exception ex)
        {
            Apache.NMS.ExceptionListener listener = this.ExceptionListener;
            if (listener != null)
            {
                // Wrap does nothing if this is already a NMS exception, otherwise
                // wrap it appropriately.
                listener(ExceptionSupport.Wrap(ex));
            }
            else
            {
                Tracer.WarnFormat("Received Async exception. Type {0} Message {1}", ex.GetType().Name, ex.Message);
                Tracer.DebugFormat("Async Exception Stack {0}", ex);
            }
        }

        private class ExceptionNotification : DispatchEvent
        {
            private readonly Connection connection;
            private readonly Exception exception;
            public ExceptionNotification(Connection owner, Exception ex)
            {
                connection = owner;
                exception = ex;
                Callback = this.Nofify;
            }

            public override void OnFailure(Exception e)
            {
                base.OnFailure(e);
                connection.DispatchException(e);
            }

            private void Nofify()
            {
                connection.DispatchException(exception);
            }

        }
        
        #endregion

        #region IConnection methods

        AcknowledgementMode acknowledgementMode = AcknowledgementMode.AutoAcknowledge;
        /// <summary>
        /// Sets the <see cref="Apache.NMS.AcknowledgementMode"/> for the <see cref="Apache.NMS.ISession"/> 
        /// objects created by the connection.
        /// </summary>
        public AcknowledgementMode AcknowledgementMode
        {
            get { return acknowledgementMode; }
            set { acknowledgementMode = value; }
        }
        
        /// <summary>
        /// See <see cref="Apache.NMS.IConnection.ClientId"/>.
        /// </summary>
        public string ClientId
        {
            get { return connInfo.clientId; }
            set
            {
                if (this.clientIdCanSet.Value)
                {
                    if (value != null && value.Length > 0)
                    {
                        connInfo.clientId = value;
                        try
                        {
                            this.Connect();
                        }
                        catch (NMSException nms)
                        {
                            NMSException ex = nms;
                            if (nms.Message.Contains("invalid-field:container-id"))
                            {
                                ex = new InvalidClientIDException(nms.Message);
                            }
                            throw ex;
                        }
                    }
                }
                else
                {
                    throw new InvalidClientIDException("Client Id can not be set after connection is Started.");
                }
            }
        }

        /// <summary>
        /// Throws <see cref="NotImplementedException"/>.
        /// </summary>
        public ConsumerTransformerDelegate ConsumerTransformer
        {
            get => throw new NotImplementedException();
            set => throw new NotImplementedException();
        }

        /// <summary>
        /// Throws <see cref="NotImplementedException"/>.
        /// </summary>
        public ProducerTransformerDelegate ProducerTransformer
        {
            get => throw new NotImplementedException();
            set => throw new NotImplementedException();
        }

        /// <summary>
        /// AMQP Provider access to remote connection properties. 
        /// This is used by <see cref="ConnectionProviderUtilities.GetRemotePeerConnectionProperties(Apache.NMS.IConnection)"/>.
        /// </summary>
        public StringDictionary RemotePeerProperties
        {
            get { return PropertyUtil.Clone(this.Info.RemotePeerProperies); }
        }

        /// <summary>
        /// See <see cref="Apache.NMS.IConnection.MetaData"/>.
        /// </summary>
        public IConnectionMetaData MetaData
        {
            get
            {
                return ConnectionMetaData.Version;
            }
        }

        /// <summary>
        /// See <see cref="Apache.NMS.IConnection.RedeliveryPolicy"/>.
        /// </summary>
        public IRedeliveryPolicy RedeliveryPolicy
        {
            get { return this.redeliveryPolicy; }
            set
            {
                if (value != null)
                {
                    this.redeliveryPolicy = value;
                }
            }
        }

        /// <summary>
        /// See <see cref="Apache.NMS.IConnection.RequestTimeout"/>.
        /// </summary>
        public TimeSpan RequestTimeout
        {
            get
            {
                return TimeSpan.FromMilliseconds(this.connInfo.requestTimeout);
            }

            set
            {
                connInfo.requestTimeout = Convert.ToInt64(value.TotalMilliseconds);
            }
        }

        /// <summary>
        /// Not Implemented, throws <see cref="NotImplementedException"/>.
        /// </summary>
        public event ConnectionInterruptedListener ConnectionInterruptedListener
        {
            add => throw new NotImplementedException("AMQP Provider does not implement ConnectionInterruptedListener events.");
            remove => throw new NotImplementedException("AMQP Provider does not implement ConnectionInterruptedListener events.");
        }

        /// <summary>
        /// Not Implemented, throws <see cref="NotImplementedException"/>.
        /// </summary>
        public event ConnectionResumedListener ConnectionResumedListener
        {
            add => throw new NotImplementedException("AMQP Provider does not implement ConnectionResumedListener events.");
            remove => throw new NotImplementedException("AMQP Provider does not implement ConnectionResumedListener events.");
        }

        /// <summary>
        /// See <see cref="Apache.NMS.IConnection.ExceptionListener"/>.
        /// </summary>
        public event ExceptionListener ExceptionListener;

        /// <summary>
        /// Creates a <see cref="Apache.NMS.ISession"/> with 
        /// the connection <see cref="Apache.NMS.IConnection.AcknowledgementMode"/>.
        /// </summary>
        /// <returns>An <see cref="Apache.NMS.ISession"/> provider instance.</returns>
        public Apache.NMS.ISession CreateSession()
        {
            return CreateSession(acknowledgementMode);
        }

        /// <summary>
        /// Creates a <see cref="Apache.NMS.ISession"/> with the given <see cref="Apache.NMS.AcknowledgementMode"/> parameter.
        /// <para>
        /// Throws <see cref="NotImplementedException"/> for the <see cref="Apache.NMS.AcknowledgementMode.Transactional"/>.
        /// </para>
        /// </summary>
        /// <param name="acknowledgementMode"></param>
        /// <returns></returns>
        public Apache.NMS.ISession CreateSession(AcknowledgementMode acknowledgementMode)
        {
            this.CheckIfClosed();
            this.Connect();
            Session ses = new Session(this);
            ses.AcknowledgementMode = acknowledgementMode;
            try
            {
                ses.Begin();
            }
            catch(NMSException) { throw; }
            catch(Exception ex)
            {
                throw ExceptionSupport.Wrap(ex, "Failed to establish amqp Session.");
            }

            if(!this.sessions.TryAdd(ses.Id, ses))
            {
                Tracer.ErrorFormat("Failed to add Session {0}.", ses.Id);
            }
            Tracer.InfoFormat("Created Session {0} on connection {1}.", ses.Id, this.ClientId);
            return ses;
        }

        /// <summary>
        /// Destroys all temporary destinations for the connection. 
        /// <para>
        /// Throws <see cref="IllegalStateException"/> should any Temporary Topic or Temporary Queue in 
        /// the connection have an active consumer using it.
        /// </para>
        /// </summary>
        public void PurgeTempDestinations()
        {
            foreach(TemporaryDestination temp in temporaryLinks.Keys.ToArray())
            {
                this.DestroyTemporaryDestination(temp);
            }
        }

        /// <summary>
        /// See <see cref="Apache.NMS.IConnection.Close"/>.
        /// </summary>
        public void Close()
        {
            Dispose(true);
            if (this.IsClosed)
            {
                GC.SuppressFinalize(this);
            }
        }

        #endregion

        #region IDisposable Methods


        protected virtual void Dispose(bool disposing)
        {
            if (IsClosed) return;
            Tracer.DebugFormat("Closing of Connection {0}", ClientId);
            if (disposing)
            {   
                // full shutdown
                if (this.closing.CompareAndSet(false, true))
                {
                    Session[] connectionSessions = GetSessions();
                    foreach (Session s in connectionSessions)
                    {
                        try
                        {
                            s.CheckOnDispatchThread();
                        }
                        catch
                        {
                            this.closing.Value = false;
                            throw;
                        }
                    }

                    this.Stop();

                    this.temporaryLinks.Close();

                    try
                    {
                        this.Disconnect();
                    }
                    catch (Exception ex)
                    {
                        // log network errors
                        NMSException nmse = ExceptionSupport.Wrap(ex, "Amqp Connection close failure for NMS Connection {0}", this.Id);
                        Tracer.DebugFormat("Caught Exception while closing Amqp Connection {0}. Exception {1}", this.Id, nmse);
                    }
                    finally
                    {
                        sessions?.Clear();
                        sessions = null;
                        if (this.state.Value.Equals(ConnectionState.CLOSED))
                        {
                            if (this.exceptionExecutor != null)
                            {
                                this.exceptionExecutor.Close();
                                this.exceptionExecutor = null;
                            }
                        }
                    }
                }
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
                Tracer.DebugFormat("Caught Exception while Disposing of NMS Connection {0}. Exception {1}", this.ClientId, ex);
            }
        }

        #endregion

        #region NMSResource Methods

        public override bool IsStarted { get { return !mode.Value.Equals(Resource.Mode.Stopped); } }

        protected override void ThrowIfClosed()
        {
            this.CheckIfClosed();
        }

        protected override void StartResource()
        {
            this.Connect();

            if (!IsConnected)
            {
                throw new NMSConnectionException("Connection Failed to connect to Client.");
            }

            //start sessions here
            foreach (Session s in GetSessions())
            {
                s.Start();
            }
            
        }

        protected override void StopResource()
        {
            if ( this.impl != null && !this.impl.IsClosed)
            {
                // stop all sessions here.
                foreach (Session s in GetSessions())
                {
                    s.Stop();
                }
            }
        }

        #endregion

        public override string ToString()
        {
            return "Connection:\nConnection Info:"+connInfo.ToString();
        }

        
    }

    #region Connection Provider Utilities

    /// <summary>
    /// This give access to provider specific functions and capabilities for a provider connection.
    /// </summary>
    public static class ConnectionProviderUtilities
    {
        public static bool IsAMQPConnection(Apache.NMS.IConnection connection)
        {
            return connection != null && connection is Apache.NMS.AMQP.Connection;
        }

        public static StringDictionary GetRemotePeerConnectionProperties(Apache.NMS.IConnection connection)
        {
            if (connection == null)
            {
                return null;
            }
            else if (connection is Apache.NMS.AMQP.Connection)
            {
                return (connection as Apache.NMS.AMQP.Connection).RemotePeerProperties;
            }
            return null;
        }
    }

    #endregion

    #region Connection Information inner Class

    internal class ConnectionInfo : ResourceInfo
    {
        static ConnectionInfo()
        {
            Amqp.ConnectionFactory defaultCF = new Amqp.ConnectionFactory();
            AmqpSettings defaultAMQPSettings = defaultCF.AMQP;
            
            DEFAULT_CHANNEL_MAX = defaultAMQPSettings.MaxSessionsPerConnection;
            DEFAULT_MAX_FRAME_SIZE = defaultAMQPSettings.MaxFrameSize;
            DEFAULT_IDLE_TIMEOUT = defaultAMQPSettings.IdleTimeout;
            
            DEFAULT_REQUEST_TIMEOUT = Convert.ToInt64(NMSConstants.defaultRequestTimeout.TotalMilliseconds);

        }
        public const long INFINITE = -1;
        public const long DEFAULT_CONNECT_TIMEOUT = 15000;
        public const int DEFAULT_CLOSE_TIMEOUT = 15000;
        public static readonly long DEFAULT_REQUEST_TIMEOUT;
        public static readonly long DEFAULT_IDLE_TIMEOUT;

        public static readonly ushort DEFAULT_CHANNEL_MAX;
        public static readonly int DEFAULT_MAX_FRAME_SIZE;

        public ConnectionInfo() : this(null) { }
        public ConnectionInfo(Id clientId) : base(clientId)
        {
            if (clientId != null)
                this.clientId = clientId.ToString();
        }

        private Id ClientId = null;

        public override Id Id
        {
            get
            {
                if (base.Id == null)
                {
                    if (ClientId == null && clientId != null)
                    {
                        ClientId = new Id(clientId);
                    }
                    return ClientId;
                }
                else
                {
                    return base.Id;
                }
            }
        }

        internal Id ResourceId
        {
            set
            {
                if(ClientId == null && value != null)
                {
                    ClientId = value;
                    clientId = ClientId.ToString();
                }
                
            }
        }

        internal Uri remoteHost { get; set; }
        public string clientId { get; internal set; } = null;
        public string username { get; set; } = null;
        public string password { get; set; } = null;

        public long requestTimeout { get; set; } = DEFAULT_REQUEST_TIMEOUT;
        public long connectTimeout { get; set; } = DEFAULT_CONNECT_TIMEOUT;
        public int closeTimeout { get; set; } = DEFAULT_CLOSE_TIMEOUT;
        public long idleTimout { get; set; } = DEFAULT_IDLE_TIMEOUT;

        public ushort channelMax { get; set; } = DEFAULT_CHANNEL_MAX;
        public int maxFrameSize { get; set; } = DEFAULT_MAX_FRAME_SIZE;

        public string TopicPrefix { get; internal set; } = null;

        public string QueuePrefix { get; internal set; } = null;

        public bool IsAnonymousRelay { get; internal set; } = false;

        public bool IsDelayedDelivery { get; internal set; } = false;

        public Message.Cloak.AMQPObjectEncodingType? EncodingType { get; internal set; } = null;


        public IList<string> Capabilities { get { return new List<string>(capabilities); } }

        public bool HasCapability(string capability)
        {
            return capabilities.Contains(capability);
        }

        public void AddCapability(string capability)
        {
            if (capability != null && capability.Length > 0)
                capabilities.Add(capability);
        }

        public StringDictionary RemotePeerProperies { get => remoteConnectionProperties; }

        private StringDictionary remoteConnectionProperties = new StringDictionary();
        private List<string> capabilities = new List<string>();

        public override string ToString()
        {
            string result = "";
            result += "connInfo = [\n";
            foreach (MemberInfo info in this.GetType().GetMembers())
            {
                if (info is PropertyInfo)
                {
                    PropertyInfo prop = info as PropertyInfo;

                    if (prop.GetGetMethod(true).IsPublic)
                    {
                        if (prop.GetGetMethod(true).ReturnParameter.ParameterType.IsEquivalentTo(typeof(List<string>)))
                        {
                            result += string.Format("{0} = {1},\n", prop.Name, PropertyUtil.ToString(prop.GetValue(this,null) as IList));
                        }
                        else
                        {
                            result += string.Format("{0} = {1},\n", prop.Name, prop.GetValue(this, null));
                        }

                    }
                }
            }
            result = result.Substring(0, result.Length - 2) + "\n]";
            return result;
        }

    }

    #endregion

}
