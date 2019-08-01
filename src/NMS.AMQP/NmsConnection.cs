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
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Apache.NMS.AMQP.Message;
using Apache.NMS.AMQP.Meta;
using Apache.NMS.AMQP.Provider;
using Apache.NMS.AMQP.Util;
using Apache.NMS.Util;

namespace Apache.NMS.AMQP
{
    public class NmsConnection : IConnection, IProviderListener
    {
        private readonly AtomicBool closed = new AtomicBool();
        private readonly AtomicBool connected = new AtomicBool();
        private readonly HashSet<INmsConnectionListener> connectionListeners = new HashSet<INmsConnectionListener>();
        private readonly IProvider provider;
        private readonly ConcurrentDictionary<Id, NmsSession> sessions = new ConcurrentDictionary<Id, NmsSession>();
        private readonly ConcurrentDictionary<Id, NmsTemporaryDestination> tempDestinations = new ConcurrentDictionary<Id, NmsTemporaryDestination>();
        private readonly AtomicBool started = new AtomicBool();
        private IdGenerator sessionIdGenerator;
        private IdGenerator temporaryTopicGenerator;
        private IdGenerator temporaryQueueGenerator;
        private readonly object syncRoot = new object();

        public NmsConnection(ConnectionInfo connectionInfo, IProvider provider)
        {
            if (provider == null)
            {
                throw new ArgumentNullException($"{nameof(provider)}", "Remove provider instance not set.");
            }

            ConnectionInfo = connectionInfo;
            this.provider = provider;
            provider.SetProviderListener(this);

            try
            {
                provider.Start();
            }
            catch (Exception e)
            {
                throw NMSExceptionSupport.Create(e);
            }
        }

        public ConnectionInfo ConnectionInfo { get; }

        private IdGenerator SessionIdGenerator
        {
            get
            {
                if (sessionIdGenerator == null)
                {
                    lock (syncRoot)
                    {
                        if (sessionIdGenerator == null)
                        {
                            sessionIdGenerator = new NestedIdGenerator("ID:ses", ConnectionInfo.Id, true);
                        }
                    }
                }

                return sessionIdGenerator;
            }
        }

        private IdGenerator TemporaryTopicGenerator
        {
            get
            {
                if (temporaryTopicGenerator == null)
                {
                    lock (syncRoot)
                    {
                        if (temporaryTopicGenerator == null)
                        {
                            temporaryTopicGenerator = new NestedIdGenerator("ID:nms-temp-topic", ConnectionInfo.Id, true);
                        }
                    }
                }

                return temporaryTopicGenerator;
            }
        }

        private IdGenerator TemporaryQueueGenerator
        {
            get
            {
                if (temporaryQueueGenerator == null)
                {
                    lock (syncRoot)
                    {
                        if (temporaryQueueGenerator == null)
                        {
                            temporaryQueueGenerator = new NestedIdGenerator("ID:nms-temp-queue", ConnectionInfo.Id, true);
                        }
                    }
                }

                return temporaryQueueGenerator;
            }
        }

        public bool IsClosed => closed.Value;
        public bool IsConnected => connected.Value;
        public Id Id => ConnectionInfo.Id;
        public INmsMessageFactory MessageFactory { get; private set; }

        public void Dispose()
        {
            try
            {
                Close();
            }
            catch (Exception ex)
            {
                Tracer.DebugFormat("Caught exception while disposing {0} {1}. Exception {2}", GetType().Name, ConnectionInfo.Id, ex);
            }
        }

        public void Stop()
        {
            DoStop(true);
        }

        private void DoStop(bool checkClosed)
        {
            if (checkClosed)
                CheckClosed();

            CheckIsOnDeliveryThread();

            if (started.CompareAndSet(true, false))
            {
                foreach (NmsSession session in sessions.Values)
                {
                    session.Stop();
                }
            }
        }

        public ISession CreateSession()
        {
            return CreateSession(AcknowledgementMode);
        }

        public ISession CreateSession(AcknowledgementMode acknowledgementMode)
        {
            CheckClosed();
            CreateNmsConnection();

            NmsSession session = new NmsSession(this, SessionIdGenerator.GenerateId(), acknowledgementMode)
            {
                SessionInfo = { requestTimeout = ConnectionInfo.requestTimeout }
            };
            try
            {
                session.Begin().ConfigureAwait(false).GetAwaiter().GetResult();
                sessions.TryAdd(session.SessionInfo.Id, session);
                if (started)
                {
                    session.Start();
                }

                return session;
            }
            catch (NMSException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw ExceptionSupport.Wrap(ex, "Failed to establish amqp Session.");
            }
        }

        public void Close()
        {
            CheckIsOnDeliveryThread();

            if (closed.CompareAndSet(false, true))
            {
                DoStop(false);

                foreach (NmsSession session in sessions.Values)
                    session.Shutdown(null);

                try
                {
                    provider.Close();
                }
                catch (Exception)
                {
                    Tracer.Debug("Ignoring provider exception during connection close");
                }

                sessions.Clear();
                started.Set(false);
                connected.Set(false);
            }
        }

        public void PurgeTempDestinations()
        {
            throw new NotImplementedException();
        }

        public void Start()
        {
            CreateNmsConnection();

            if (started.CompareAndSet(false, true))
            {
                foreach (var session in sessions.Values.ToArray())
                {
                    session.Start();
                }
            }
        }

        public bool IsStarted => started;

        public ConsumerTransformerDelegate ConsumerTransformer { get; set; }
        public ProducerTransformerDelegate ProducerTransformer { get; set; }

        public TimeSpan RequestTimeout
        {
            get => TimeSpan.FromMilliseconds(ConnectionInfo.requestTimeout);
            set => ConnectionInfo.requestTimeout = Convert.ToInt64(value.TotalMilliseconds);
        }

        public AcknowledgementMode AcknowledgementMode { get; set; }

        public string ClientId
        {
            get => ConnectionInfo.ClientId;
            set
            {
                if (ConnectionInfo.IsExplicitClientId)
                {
                    throw new IllegalStateException("The clientId has already been set");
                }

                if (string.IsNullOrEmpty(value))
                {
                    throw new InvalidClientIDException("Cannot have a null or empty clientId");
                }

                if (connected)
                {
                    throw new IllegalStateException("Cannot set the client id once connected.");
                }

                ConnectionInfo.SetClientId(value, true);

                // We weren't connected if we got this far, we should now connect to ensure the
                // configured clientId is valid.
                CreateNmsConnection();
            }
        }

        public IRedeliveryPolicy RedeliveryPolicy { get; set; }
        public IConnectionMetaData MetaData { get; } = ConnectionMetaData.Version;
        public event ExceptionListener ExceptionListener;
        public event ConnectionInterruptedListener ConnectionInterruptedListener;
        public event ConnectionResumedListener ConnectionResumedListener;

        public void OnInboundMessage(InboundMessageDispatch envelope)
        {
            if (sessions.TryGetValue(envelope.ConsumerInfo.SessionId, out NmsSession session))
            {
                session.OnInboundMessage(envelope);
            }
        }

        public void OnConnectionFailure(NMSException exception)
        {
            OnAsyncException(exception);

            if (closed.CompareAndSet(false, true))
            {
                try
                {
                    provider?.Close();
                }
                catch (Exception error)
                {
                    Tracer.Error($"Error while closing failed Provider: {error.Message}");
                }

                connected.Set(false);

                try
                {
                    foreach (NmsSession session in sessions.Values.ToArray())
                    {
                        session.Shutdown(exception);
                    }
                }
                catch (NMSException e)
                {
                    Tracer.Error($"Exception during connection cleanup, {e}");
                }

                foreach (INmsConnectionListener listener in connectionListeners)
                {
                    listener.OnConnectionFailure(exception);
                }
            }
        }

        public async Task OnConnectionRecovery(IProvider provider)
        {
            foreach (NmsTemporaryDestination tempDestination in tempDestinations.Values)
            {
                await provider.CreateResource(tempDestination);
            }

            foreach (NmsSession session in sessions.Values)
            {
                await session.OnConnectionRecovery(provider).ConfigureAwait(false);
            }
        }

        public void OnConnectionEstablished(Uri remoteUri)
        {
            Tracer.Info($"Connection {ConnectionInfo.Id} connected to remote Broker: {remoteUri.Scheme}://{remoteUri.Host}:{remoteUri.Port}");
            MessageFactory = provider.MessageFactory;

            foreach (var listener in connectionListeners)
            {
                listener.OnConnectionEstablished(remoteUri);
            }
        }

        public async Task OnConnectionRecovered(IProvider provider)
        {
            Tracer.Debug($"Connection {ConnectionInfo.Id} is finalizing recovery.");

            foreach (NmsSession session in sessions.Values)
            {
                await session.OnConnectionRecovered(provider).ConfigureAwait(false);
            }
        }

        public void OnConnectionRestored(Uri remoteUri)
        {
            MessageFactory = provider.MessageFactory;

            foreach (var listener in connectionListeners)
            {
                listener.OnConnectionRestored(remoteUri);
            }
        }

        public void OnResourceClosed(ResourceInfo resourceInfo, Exception error)
        {
            switch (resourceInfo)
            {
                case ConsumerInfo consumerInfo:
                {
                    if (!sessions.TryGetValue(consumerInfo.SessionId, out NmsSession session))
                        return;

                    NmsMessageConsumer messageConsumer = session.ConsumerClosed(consumerInfo.Id, error);
                    if (messageConsumer == null)
                        return;
                    foreach (var connectionListener in connectionListeners)
                        connectionListener.OnConsumerClosed(messageConsumer, error);
                    break;
                }

                case ProducerInfo producerInfo:
                {
                    if (!sessions.TryGetValue(producerInfo.SessionId, out NmsSession session))
                        return;

                    NmsMessageProducer messageProducer = session.ProducerClosed(producerInfo.Id, error);
                    if (messageProducer == null)
                        return;

                    foreach (var connectionListener in connectionListeners)
                        connectionListener.OnProducerClosed(messageProducer, error);
                    break;
                }
            }
        }

        public void OnConnectionInterrupted(Uri failedUri)
        {
            foreach (INmsConnectionListener listener in connectionListeners)
                listener.OnConnectionInterrupted(failedUri);
        }

        private void CheckClosed()
        {
            if (closed)
            {
                throw new IllegalStateException("The Connection is closed");
            }
        }

        internal Task CreateResource(ResourceInfo resourceInfo)
        {
            return provider.CreateResource(resourceInfo);
        }

        internal Task DestroyResource(ResourceInfo resourceInfo)
        {
            return provider.DestroyResource(resourceInfo);
        }

        internal Task Send(OutboundMessageDispatch envelope)
        {
            return provider.Send(envelope);
        }

        private void CheckIsOnDeliveryThread()
        {
            foreach (NmsSession session in sessions.Values)
            {
                session.CheckIsOnDeliveryThread();
            }
        }

        private void CreateNmsConnection()
        {
            if (connected || closed)
            {
                return;
            }

            lock (syncRoot)
            {
                if (!closed && connected.CompareAndSet(false, true))
                {
                    try
                    {
                        provider.Connect(ConnectionInfo).ConfigureAwait(false).GetAwaiter().GetResult();
                    
                    }
                    catch (Exception e)
                    {
                        try
                        {
                            provider.Close();
                        }
                        catch
                        {
                        }

                        throw NMSExceptionSupport.Create(e);
                    }
                }
            }
        }

        public void OnAsyncException(Exception error)
        {
            ExceptionListener?.Invoke(error);
        }

        internal Task Recover(Id sessionId)
        {
            return provider.Recover(sessionId);
        }

        public Task StartResource(ResourceInfo resourceInfo)
        {
            return provider.StartResource(resourceInfo);
        }

        public void AddConnectionListener(INmsConnectionListener listener)
        {
            connectionListeners.Add(listener);
        }

        public void Acknowledge(Id sessionId, AckType ackType)
        {
            provider.Acknowledge(sessionId, ackType);
        }

        internal Task Acknowledge(InboundMessageDispatch envelope, AckType ackType)
        {
            return provider.Acknowledge(envelope, ackType);
        }

        internal void RemoveSession(SessionInfo sessionInfo)
        {
            sessions.TryRemove(sessionInfo.Id, out _);
        }

        public ITemporaryQueue CreateTemporaryQueue()
        {
            NmsTemporaryQueue queue = new NmsTemporaryQueue(TemporaryQueueGenerator.GenerateId());
            InitializeTemporaryDestination(queue);
            return queue;
        }

        public ITemporaryTopic CreateTemporaryTopic()
        {
            NmsTemporaryTopic topic = new NmsTemporaryTopic(TemporaryTopicGenerator.GenerateId());
            InitializeTemporaryDestination(topic);
            return topic;
        }

        private void InitializeTemporaryDestination(NmsTemporaryDestination temporaryDestination)
        {
            CreateResource(temporaryDestination).ConfigureAwait(false).GetAwaiter().GetResult();
            tempDestinations.TryAdd(temporaryDestination.Id, temporaryDestination);
            temporaryDestination.Connection = this;
        }

        internal void CheckConsumeFromTemporaryDestination(NmsTemporaryDestination destination)
        {
            if (!Equals(this, destination.Connection))
                throw new InvalidDestinationException("Can't consume from a temporary destination created using another connection");
        }

        public void DeleteTemporaryDestination(NmsTemporaryDestination destination)
        {
            CheckClosed();

            try
            {
                foreach (NmsSession session in sessions.Values)
                {
                    if (session.IsDestinationInUse(destination))
                    {
                        throw new IllegalStateException("A consumer is consuming from the temporary destination");
                    }
                }

                tempDestinations.TryRemove(destination.Id, out _);

                DestroyResource(destination).ConfigureAwait(false).GetAwaiter().GetResult();
            }
            catch (Exception e)
            {
                throw NMSExceptionSupport.Create(e);
            }
        }

        public void Unsubscribe(string subscriptionName)
        {
            CheckClosed();

            provider.Unsubscribe(subscriptionName).ConfigureAwait(false).GetAwaiter().GetResult();
        }
    }
}