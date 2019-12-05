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
using Amqp;
using Amqp.Framing;
using Apache.NMS.AMQP.Message;
using Apache.NMS.AMQP.Meta;
using Apache.NMS.AMQP.Transport;
using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP.Provider.Amqp
{
    public class AmqpProvider : IProvider
    {
        public static readonly uint DEFAULT_MAX_HANDLE = 1024;
        private static readonly uint DEFAULT_SESSION_OUTGOING_WINDOW = 2048; // AmqpNetLite default

        private readonly ITransportContext transport;
        private NmsConnectionInfo connectionInfo;
        private AmqpConnection connection;
        public AmqpProvider(Uri remoteUri, ITransportContext transport)
        {
            RemoteUri = remoteUri;
            this.transport = transport;
        }

        static AmqpProvider()
        {
            // Set up tracing in AMQP. We capture all AMQP traces in the TraceListener below
            // and map to NMS 'Tracer' logs as follows:
            //    AMQP          Tracer
            //    Verbose       Debug
            //    Frame         Debug
            //    Information   Info
            //    Output        Info    (should not happen)
            //    Warning       Warn
            //    Error         Error
            Trace.TraceLevel = TraceLevel.Verbose;
            Trace.TraceListener = (level, format, args) =>
            {
                switch (level)
                {
                    case TraceLevel.Verbose:
                    case TraceLevel.Frame:
                        Tracer.DebugFormat(format, args);
                        break;
                    case TraceLevel.Information:
                    case TraceLevel.Output:
                        // 
                        // Applications should not access AmqpLite directly so there
                        // should be no 'Output' level logs.
                        Tracer.InfoFormat(format, args);
                        break;
                    case TraceLevel.Warning:
                        Tracer.WarnFormat(format, args);
                        break;
                    case TraceLevel.Error:
                        Tracer.ErrorFormat(format, args);
                        break;
                    default:
                        Tracer.InfoFormat("Unknown AMQP LogLevel: {}", level);
                        Tracer.InfoFormat(format, args);
                        break;
                }
            };
        }
        
        /// <summary>
        /// Enables AmqpNetLite's Frame logging level.
        /// </summary>
        public bool TraceFrames
        {
            get => ((Trace.TraceLevel & TraceLevel.Frame) == TraceLevel.Frame);
            set
            {
                if (value)
                {
                    Trace.TraceLevel = Trace.TraceLevel | TraceLevel.Frame;
                }
                else
                {
                    Trace.TraceLevel = Trace.TraceLevel & ~TraceLevel.Frame;
                }
            }
        }

        public long SendTimeout => connectionInfo?.SendTimeout ?? NmsConnectionInfo.DEFAULT_SEND_TIMEOUT;
        public long CloseTimeout => connectionInfo?.CloseTimeout ?? NmsConnectionInfo.DEFAULT_CLOSE_TIMEOUT;
        public long RequestTimeout => connectionInfo?.RequestTimeout ?? NmsConnectionInfo.DEFAULT_REQUEST_TIMEOUT;
        public uint SessionOutgoingWindow { get; set; } = DEFAULT_SESSION_OUTGOING_WINDOW;
        public uint MaxHandle { get; set; } = DEFAULT_MAX_HANDLE;
        
        public Uri RemoteUri { get; }
        public IProviderListener Listener { get; private set; }

        public void Start()
        {
        }

        public Task Connect(NmsConnectionInfo connectionInfo)
        {
            this.connectionInfo = connectionInfo;
            connection = new AmqpConnection(this, transport, connectionInfo);
            return connection.Start();
        }

        internal void OnConnectionClosed(NMSException exception)
        {
            Listener?.OnConnectionFailure(exception);
        }

        internal void FireConnectionEstablished()
        {
            Listener?.OnConnectionEstablished(RemoteUri);
        }

        public void Close()
        {
            connection?.Close();
        }

        public void SetProviderListener(IProviderListener providerListener)
        {
            Listener = providerListener;
        }

        public Task CreateResource(INmsResource resourceInfo)
        {
            switch (resourceInfo)
            {
                case NmsSessionInfo sessionInfo:
                    return connection.CreateSession(sessionInfo);
                case NmsConsumerInfo consumerInfo:
                {
                    AmqpSession session = connection.GetSession(consumerInfo.SessionId);
                    return session.CreateConsumer(consumerInfo);
                }
                case NmsProducerInfo producerInfo:
                {
                    AmqpSession session = connection.GetSession(producerInfo.SessionId);
                    return session.CreateProducer(producerInfo);
                }
                case NmsTemporaryDestination temporaryDestination:
                    return connection.CreateTemporaryDestination(temporaryDestination);
                case NmsTransactionInfo transactionInfo:
                    var amqpSession = connection.GetSession(transactionInfo.SessionId);
                    return amqpSession.BeginTransaction(transactionInfo);
                default:
                    throw new ArgumentOutOfRangeException(nameof(resourceInfo), "Not supported resource type.");
            }
        }

        public Task DestroyResource(INmsResource resourceInfo)
        {
            switch (resourceInfo)
            {
                case NmsSessionInfo sessionInfo:
                {
                    AmqpSession session = connection.GetSession(sessionInfo.Id);
                    session.Close();
                    return Task.CompletedTask;
                }
                case NmsConsumerInfo consumerInfo:
                {
                    AmqpSession session = connection.GetSession(consumerInfo.SessionId);
                    AmqpConsumer consumer = session.GetConsumer(consumerInfo.Id);
                    consumer.Close();
                    session.RemoveConsumer(consumerInfo.Id);
                    return Task.CompletedTask;
                }
                case NmsProducerInfo producerInfo:
                {
                    AmqpSession session = connection.GetSession(producerInfo.SessionId);
                    AmqpProducer producer = session.GetProducer(producerInfo.Id);
                    producer.Close();
                    session.RemoveProducer(producerInfo.Id);
                    return Task.CompletedTask;
                }
                case NmsTemporaryDestination temporaryDestination:
                {
                    AmqpTemporaryDestination amqpTemporaryDestination = connection.GetTemporaryDestination(temporaryDestination);
                    if (amqpTemporaryDestination != null)
                    {
                        amqpTemporaryDestination.Close();
                        connection.RemoveTemporaryDestination(temporaryDestination);
                    }
                    else
                        Tracer.Debug($"Could not find temporary destination {temporaryDestination} to delete.");

                    return Task.CompletedTask;
                }
                default:
                    throw new ArgumentOutOfRangeException(nameof(resourceInfo), "Not supported resource type.");
            }
        }

        public Task StartResource(INmsResource resourceInfo)
        {
            switch (resourceInfo)
            {
                case NmsConsumerInfo consumerInfo:
                    AmqpSession session = connection.GetSession(consumerInfo.SessionId);
                    AmqpConsumer amqpConsumer = session.GetConsumer(consumerInfo.Id);
                    amqpConsumer.Start();
                    return Task.CompletedTask;
                default:
                    throw new ArgumentOutOfRangeException(nameof(resourceInfo), "Not supported resource type.");
            }
        }

        public Task StopResource(INmsResource resourceInfo)
        {
            switch (resourceInfo)
            {
                case NmsConsumerInfo consumerInfo:
                    AmqpSession session = connection.GetSession(consumerInfo.SessionId);
                    AmqpConsumer amqpConsumer = session.GetConsumer(consumerInfo.Id);
                    amqpConsumer.Stop();
                    return Task.CompletedTask;
                default:
                    throw new ArgumentOutOfRangeException(nameof(resourceInfo), "Not supported resource type.");
            }
        }

        public Task Recover(NmsSessionId sessionId)
        {
            AmqpSession session = connection.GetSession(sessionId);
            session.Recover();
            return Task.CompletedTask;
        }

        public Task Acknowledge(NmsSessionId sessionId, AckType ackType)
        {
            AmqpSession session = connection.GetSession(sessionId);
            foreach (AmqpConsumer consumer in session.Consumers)
            {
                consumer.Acknowledge(ackType);
            }

            return Task.CompletedTask;
        }

        public Task Acknowledge(InboundMessageDispatch envelope, AckType ackType)
        {
            AmqpSession session = connection.GetSession(envelope.ConsumerInfo.SessionId);
            AmqpConsumer consumer = session.GetConsumer(envelope.ConsumerId);
            consumer.Acknowledge(envelope, ackType);
            return Task.CompletedTask;
        }

        public INmsMessageFactory MessageFactory => connection.MessageFactory;

        public Task Send(OutboundMessageDispatch envelope)
        {
            AmqpSession session = connection.GetSession(envelope.ProducerInfo.SessionId);
            AmqpProducer producer = session.GetProducer(envelope.ProducerId);
            producer.Send(envelope);
            envelope.Message.IsReadOnly = false;
            return Task.CompletedTask;
        }

        public Task Unsubscribe(string subscriptionName)
        {
            return connection.Unsubscribe(subscriptionName);
        }

        public Task Rollback(NmsTransactionInfo transactionInfo, NmsTransactionInfo nextTransactionInfo)
        {
            var session = connection.GetSession(transactionInfo.SessionId);
            return session.Rollback(transactionInfo, nextTransactionInfo);
        }

        public Task Commit(NmsTransactionInfo transactionInfo, NmsTransactionInfo nextTransactionInfo)
        {
            var session = connection.GetSession(transactionInfo.SessionId);
            return session.Commit(transactionInfo, nextTransactionInfo);
        }

        public void FireResourceClosed(INmsResource resourceInfo, Exception error)
        {
            Listener.OnResourceClosed(resourceInfo, error);
        }
    }
}