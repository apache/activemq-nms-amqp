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

        private readonly ITransportContext transport;
        private ConnectionInfo connectionInfo;
        private AmqpConnection connection;
        public uint MaxHandle { get; set; } = DEFAULT_MAX_HANDLE;

        public AmqpProvider(Uri remoteUri, ITransportContext transport)
        {
            RemoteUri = remoteUri;
            this.transport = transport;
        }

        public long SendTimeout => connectionInfo?.SendTimeout ?? ConnectionInfo.DEFAULT_SEND_TIMEOUT;
        public Uri RemoteUri { get; }
        public IProviderListener Listener { get; private set; }

        public void Start()
        {
        }

        public Task Connect(ConnectionInfo connectionInfo)
        {
            this.connectionInfo = connectionInfo;
            connection = new AmqpConnection(this, transport, connectionInfo);
            return connection.Start();
        }

        internal void OnInternalClosed(IAmqpObject sender, Error error)
        {
            Listener?.OnConnectionFailure(ExceptionSupport.GetException(error));
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

        public Task CreateResource(ResourceInfo resourceInfo)
        {
            switch (resourceInfo)
            {
                case SessionInfo sessionInfo:
                    return connection.CreateSession(sessionInfo);
                case ConsumerInfo consumerInfo:
                {
                    AmqpSession session = connection.GetSession(consumerInfo.SessionId);
                    return session.CreateConsumer(consumerInfo);
                }
                case ProducerInfo producerInfo:
                {
                    AmqpSession session = connection.GetSession(producerInfo.SessionId);
                    return session.CreateProducer(producerInfo);
                }
                case NmsTemporaryDestination temporaryDestination:
                    return connection.CreateTemporaryDestination(temporaryDestination);
                case TransactionInfo transactionInfo:
                    var amqpSession = connection.GetSession(transactionInfo.SessionId);
                    return amqpSession.BeginTransaction(transactionInfo);
                default:
                    throw new ArgumentOutOfRangeException(nameof(resourceInfo), "Not supported resource type.");
            }
        }

        public Task DestroyResource(ResourceInfo resourceInfo)
        {
            switch (resourceInfo)
            {
                case SessionInfo sessionInfo:
                {
                    AmqpSession session = connection.GetSession(sessionInfo.Id);
                    session.Close();
                    return Task.CompletedTask;
                }
                case ConsumerInfo consumerInfo:
                {
                    AmqpSession session = connection.GetSession(consumerInfo.SessionId);
                    AmqpConsumer consumer = session.GetConsumer(consumerInfo.Id);
                    consumer.Close();
                    session.RemoveConsumer(consumerInfo.Id);
                    return Task.CompletedTask;
                }
                case ProducerInfo producerInfo:
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
                        connection.RemoveTemporaryDestination(temporaryDestination.Id);
                    }
                    else
                        Tracer.Debug($"Could not find temporary destination {temporaryDestination.Id} to delete.");

                    return Task.CompletedTask;
                }
                default:
                    throw new ArgumentOutOfRangeException(nameof(resourceInfo), "Not supported resource type.");
            }
        }

        public Task StartResource(ResourceInfo resourceInfo)
        {
            switch (resourceInfo)
            {
                case ConsumerInfo consumerInfo:
                    AmqpSession session = connection.GetSession(consumerInfo.SessionId);
                    AmqpConsumer amqpConsumer = session.GetConsumer(consumerInfo.Id);
                    amqpConsumer.Start();
                    return Task.CompletedTask;
                default:
                    throw new ArgumentOutOfRangeException(nameof(resourceInfo), "Not supported resource type.");
            }
        }

        public Task StopResource(ResourceInfo resourceInfo)
        {
            switch (resourceInfo)
            {
                case ConsumerInfo consumerInfo:
                    AmqpSession session = connection.GetSession(consumerInfo.SessionId);
                    AmqpConsumer amqpConsumer = session.GetConsumer(consumerInfo.Id);
                    amqpConsumer.Stop();
                    return Task.CompletedTask;
                default:
                    throw new ArgumentOutOfRangeException(nameof(resourceInfo), "Not supported resource type.");
            }
        }

        public Task Recover(Id sessionId)
        {
            AmqpSession session = connection.GetSession(sessionId);
            session.Recover();
            return Task.CompletedTask;
        }

        public Task Acknowledge(Id sessionId, AckType ackType)
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

        public Task Rollback(TransactionInfo transactionInfo, TransactionInfo nextTransactionInfo)
        {
            var session = connection.GetSession(transactionInfo.SessionId);
            return session.Rollback(transactionInfo, nextTransactionInfo);
        }

        public Task Commit(TransactionInfo transactionInfo, TransactionInfo nextTransactionInfo)
        {
            var session = connection.GetSession(transactionInfo.SessionId);
            return session.Commit(transactionInfo, nextTransactionInfo);
        }

        public void FireResourceClosed(ResourceInfo resourceInfo, Exception error)
        {
            Listener.OnResourceClosed(resourceInfo, error);
        }
    }
}