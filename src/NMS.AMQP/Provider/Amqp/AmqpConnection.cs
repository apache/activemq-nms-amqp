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
using System.Threading.Tasks;
using Amqp;
using Amqp.Framing;
using Apache.NMS.AMQP.Message;
using Apache.NMS.AMQP.Meta;
using Apache.NMS.AMQP.Provider.Amqp.Message;
using Apache.NMS.AMQP.Transport;
using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP.Provider.Amqp
{
    public interface IAmqpConnection
    {
        string QueuePrefix { get; }
        string TopicPrefix { get; }
        bool ObjectMessageUsesAmqpTypes { get; }
    }

    public class AmqpConnection : IAmqpConnection
    {
        private readonly ConcurrentDictionary<Id, AmqpSession> sessions = new ConcurrentDictionary<Id, AmqpSession>();
        private readonly ConcurrentDictionary<Id, AmqpTemporaryDestination> temporaryDestinations = new ConcurrentDictionary<Id, AmqpTemporaryDestination>();

        public AmqpProvider Provider { get; }
        private readonly ITransportContext transport;
        private readonly ConnectionInfo info;
        private readonly Uri remoteUri;
        private global::Amqp.Connection underlyingConnection;
        private readonly AmqpMessageFactory messageFactory;
        private AmqpConnectionSession connectionSession;

        public AmqpConnection(AmqpProvider provider, ITransportContext transport, ConnectionInfo info)
        {
            this.Provider = provider;
            this.transport = transport;
            this.remoteUri = provider.RemoteUri;
            this.info = info;
            this.messageFactory = new AmqpMessageFactory(this);
        }

        public global::Amqp.Connection UnderlyingConnection => underlyingConnection;
        public string QueuePrefix => info.QueuePrefix;
        public string TopicPrefix => info.TopicPrefix;
        public bool ObjectMessageUsesAmqpTypes { get; set; } = false;

        public INmsMessageFactory MessageFactory => messageFactory;

        internal async Task Start()
        {
            Address address = UriUtil.ToAddress(remoteUri, info.username, info.password);
            underlyingConnection = await transport.CreateAsync(address, CreateOpenFrame(info), OnOpened);
            underlyingConnection.AddClosedCallback(Provider.OnInternalClosed);

            // Create a Session for this connection that is used for Temporary Destinations
            // and perhaps later on management and advisory monitoring.

            // TODO: change the way how connection session id is obtained
            SessionInfo sessionInfo = new SessionInfo(info.Id);
            sessionInfo.ackMode = AcknowledgementMode.AutoAcknowledge;

            connectionSession = new AmqpConnectionSession(this, sessionInfo);
            await connectionSession.Start();
        }

        private Open CreateOpenFrame(ConnectionInfo connInfo)
        {
            return new Open
            {
                ContainerId = connInfo.ClientId,
                ChannelMax = connInfo.channelMax,
                MaxFrameSize = Convert.ToUInt32(connInfo.maxFrameSize),
                HostName = remoteUri.Host,
                IdleTimeOut = Convert.ToUInt32(connInfo.idleTimout),
                DesiredCapabilities = new[]
                {
                    SymbolUtil.OPEN_CAPABILITY_SOLE_CONNECTION_FOR_CONTAINER,
                    SymbolUtil.OPEN_CAPABILITY_DELAYED_DELIVERY,
                    SymbolUtil.OPEN_CAPABILITY_ANONYMOUS_RELAY
                }
            };
        }

        private void OnOpened(global::Amqp.IConnection connection, Open open)
        {
            if (SymbolUtil.CheckAndCompareFields(open.Properties, SymbolUtil.CONNECTION_ESTABLISH_FAILED, SymbolUtil.BOOLEAN_TRUE))
            {
                Tracer.InfoFormat("Open response contains {0} property the connection {1} will soon be closed.",
                    SymbolUtil.CONNECTION_ESTABLISH_FAILED, info.Id);
            }
            else
            {
                object value = SymbolUtil.GetFromFields(open.Properties, SymbolUtil.CONNECTION_PROPERTY_TOPIC_PREFIX);
                if (value is string topicPrefix)
                {
                    info.TopicPrefix = topicPrefix;
                }

                value = SymbolUtil.GetFromFields(open.Properties, SymbolUtil.CONNECTION_PROPERTY_QUEUE_PREFIX);
                if (value is string queuePrefix)
                {
                    info.QueuePrefix = queuePrefix;
                }

                Provider.FireConnectionEstablished();
            }
        }

        public async Task CreateSession(SessionInfo sessionInfo)
        {
            var amqpSession = new AmqpSession(this, sessionInfo);
            await amqpSession.Start();
            sessions.TryAdd(sessionInfo.Id, amqpSession);
        }

        public void Close()
        {
            connectionSession?.Close();

            foreach (var session in sessions.Values.ToArray())
            {
                session.Close();
            }

            try
            {
                UnderlyingConnection?.Close();
            }
            catch (Exception ex)
            {
                // log network errors
                NMSException nmse = ExceptionSupport.Wrap(ex, "Amqp Connection close failure for NMS Connection {0}", this.info.Id);
                Tracer.DebugFormat("Caught Exception while closing Amqp Connection {0}. Exception {1}", this.info.Id, nmse);
            }
        }

        public AmqpSession GetSession(Id sessionId)
        {
            if (sessions.TryGetValue(sessionId, out AmqpSession session))
            {
                return session;
            }

            throw new Exception();
        }

        public void RemoveSession(Id sessionId)
        {
            sessions.TryRemove(sessionId, out AmqpSession removedSession);
        }

        public async Task CreateTemporaryDestination(NmsTemporaryDestination destination)
        {
            AmqpTemporaryDestination amqpTemporaryDestination = new AmqpTemporaryDestination(connectionSession, destination);
            await amqpTemporaryDestination.Attach();
            temporaryDestinations.TryAdd(destination.Id, amqpTemporaryDestination);
        }

        public AmqpTemporaryDestination GetTemporaryDestination(NmsTemporaryDestination destination)
        {
            return temporaryDestinations.TryGetValue(destination.Id, out AmqpTemporaryDestination amqpTemporaryDestination) ? amqpTemporaryDestination : null;
        }
        
        public void RemoveTemporaryDestination(Id destinationId)
        {
            temporaryDestinations.TryRemove(destinationId, out _);
        }

        public Task Unsubscribe(string subscriptionName)
        {
            // check for any active consumers on the subscription name.
            if (sessions.Values.Any(session => session.ContainsSubscriptionName(subscriptionName)))
            {
                throw new IllegalStateException("Cannot unsubscribe from Durable Consumer while consuming messages.");
            }

            return connectionSession.Unsubscribe(subscriptionName);
        }
    }
}