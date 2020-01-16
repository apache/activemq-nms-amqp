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
using System.Threading;
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
        private readonly ConcurrentDictionary<NmsSessionId, NmsSession> sessions = new ConcurrentDictionary<NmsSessionId, NmsSession>();
        private readonly ConcurrentDictionary<NmsTemporaryDestination, NmsTemporaryDestination> tempDestinations = new ConcurrentDictionary<NmsTemporaryDestination, NmsTemporaryDestination>();
        private readonly AtomicBool started = new AtomicBool();
        private readonly AtomicLong sessionIdGenerator = new AtomicLong();
        private readonly AtomicLong temporaryTopicIdGenerator = new AtomicLong();
        private readonly AtomicLong temporaryQueueIdGenerator = new AtomicLong();
        private readonly AtomicLong transactionIdGenerator = new AtomicLong();
        private Exception failureCause;
        private readonly object syncRoot = new object();

        public NmsConnection(NmsConnectionInfo connectionInfo, IProvider provider)
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

        public NmsConnectionInfo ConnectionInfo { get; }
        public bool IsClosed => closed.Value;
        public bool IsConnected => connected.Value;
        public NmsConnectionId Id => ConnectionInfo.Id;
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
                CheckClosedOrFailed();

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
            CheckClosedOrFailed();
            CreateNmsConnection();

            NmsSession session = new NmsSession(this, GetNextSessionId(), acknowledgementMode);
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
            get => TimeSpan.FromMilliseconds(ConnectionInfo.RequestTimeout);
            set => ConnectionInfo.RequestTimeout = Convert.ToInt64(value.TotalMilliseconds);
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
            else
            {
                Tracer.Error($"Could not dispatch message {envelope.Message.NMSMessageId} because session {envelope.ConsumerInfo.SessionId} not found.");
            }

            if (connectionListeners.Any())
            {
                foreach (INmsConnectionListener listener in connectionListeners)
                    listener.OnInboundMessage(envelope.Message);
            }
        }

        public void OnConnectionFailure(NMSException exception)
        {
            Interlocked.CompareExchange(ref failureCause, exception, null);
            
            OnAsyncException(exception);

            if (!closed)
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
            
            ConnectionResumedListener?.Invoke();
        }

        public void OnResourceClosed(INmsResource resource, Exception error)
        {
            switch (resource)
            {
                case NmsConsumerInfo consumerInfo:
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

                case NmsProducerInfo producerInfo:
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
            foreach (NmsSession session in sessions.Values)
            {
                session.OnConnectionInterrupted();
            }
            
            foreach (INmsConnectionListener listener in connectionListeners)
                listener.OnConnectionInterrupted(failedUri);
            
            ConnectionInterruptedListener?.Invoke();
        }

        private void CheckClosedOrFailed()
        {
            if (closed)
            {
                throw new IllegalStateException("The Connection is closed");
            }
            if (failureCause != null)
            {
                throw new NMSConnectionException(failureCause.Message, failureCause);
            }
        }

        internal Task CreateResource(INmsResource resource)
        {
            return provider.CreateResource(resource);
        }

        internal Task DestroyResource(INmsResource resource)
        {
            return provider.DestroyResource(resource);
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
                if (closed || connected)
                {
                    return;
                }
                
                try
                {
                    provider.Connect(ConnectionInfo).ConfigureAwait(false).GetAwaiter().GetResult();
                    connected.Set(true);
                }
                catch (Exception e)
                {
                    try
                    {
                        provider.Close();
                    }
                    catch
                    {
                        // ignored
                    }

                    throw NMSExceptionSupport.Create(e);
                }
            }
        }

        internal void OnAsyncException(Exception error)
        {
            ExceptionListener?.Invoke(error);
        }

        internal Task Recover(NmsSessionId sessionId)
        {
            return provider.Recover(sessionId);
        }

        public Task StartResource(INmsResource resourceInfo)
        {
            return provider.StartResource(resourceInfo);
        }

        public Task StopResource(INmsResource resourceInfo)
        {
            return provider.StopResource(resourceInfo);
        }

        public void AddConnectionListener(INmsConnectionListener listener)
        {
            connectionListeners.Add(listener);
        }

        internal Task Acknowledge(NmsSessionId sessionId, AckType ackType)
        {
            return provider.Acknowledge(sessionId, ackType);
        }

        internal Task Acknowledge(InboundMessageDispatch envelope, AckType ackType)
        {
            return provider.Acknowledge(envelope, ackType);
        }

        internal void RemoveSession(NmsSessionInfo sessionInfo)
        {
            sessions.TryRemove(sessionInfo.Id, out _);
        }

        public ITemporaryQueue CreateTemporaryQueue()
        {
            var destinationName = $"{Id}:{temporaryQueueIdGenerator.IncrementAndGet().ToString()}";
            var queue = new NmsTemporaryQueue(destinationName);
            InitializeTemporaryDestination(queue);
            return queue;
        }

        public ITemporaryTopic CreateTemporaryTopic()
        {
            var destinationName = $"{Id}:{temporaryTopicIdGenerator.IncrementAndGet().ToString()}";
            NmsTemporaryTopic topic = new NmsTemporaryTopic(destinationName);
            InitializeTemporaryDestination(topic);
            return topic;
        }

        private void InitializeTemporaryDestination(NmsTemporaryDestination temporaryDestination)
        {
            CreateResource(temporaryDestination).ConfigureAwait(false).GetAwaiter().GetResult();
            tempDestinations.TryAdd(temporaryDestination, temporaryDestination);
            temporaryDestination.Connection = this;
        }

        internal void CheckConsumeFromTemporaryDestination(NmsTemporaryDestination destination)
        {
            if (!Equals(this, destination.Connection))
                throw new InvalidDestinationException("Can't consume from a temporary destination created using another connection");
        }

        public void DeleteTemporaryDestination(NmsTemporaryDestination destination)
        {
            CheckClosedOrFailed();

            try
            {
                foreach (NmsSession session in sessions.Values)
                {
                    if (session.IsDestinationInUse(destination))
                    {
                        throw new IllegalStateException("A consumer is consuming from the temporary destination");
                    }
                }

                tempDestinations.TryRemove(destination, out _);

                DestroyResource(destination).ConfigureAwait(false).GetAwaiter().GetResult();
            }
            catch (Exception e)
            {
                throw NMSExceptionSupport.Create(e);
            }
        }

        public void Unsubscribe(string subscriptionName)
        {
            CheckClosedOrFailed();

            provider.Unsubscribe(subscriptionName).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        public Task Rollback(NmsTransactionInfo transactionInfo, NmsTransactionInfo nextTransactionInfo)
        {
            return provider.Rollback(transactionInfo, nextTransactionInfo);
        }

        public Task Commit(NmsTransactionInfo transactionInfo, NmsTransactionInfo nextTransactionInfo)
        {
            return provider.Commit(transactionInfo, nextTransactionInfo);
        }

        private NmsSessionId GetNextSessionId()
        {
            return new NmsSessionId(ConnectionInfo.Id, sessionIdGenerator.IncrementAndGet());
        }

        public NmsTransactionId GetNextTransactionId()
        {
            return new NmsTransactionId(Id, transactionIdGenerator);
        }
    }
}