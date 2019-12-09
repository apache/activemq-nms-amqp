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
        private readonly ConcurrentDictionary<NmsSessionId, AmqpSession> sessions = new ConcurrentDictionary<NmsSessionId, AmqpSession>();
        private readonly ConcurrentDictionary<NmsTemporaryDestination, AmqpTemporaryDestination> temporaryDestinations = new ConcurrentDictionary<NmsTemporaryDestination, AmqpTemporaryDestination>();

        public AmqpProvider Provider { get; }
        private readonly ITransportContext transport;
        private readonly Uri remoteUri;
        private Connection underlyingConnection;
        private readonly AmqpMessageFactory messageFactory;
        private AmqpConnectionSession connectionSession;
        private TaskCompletionSource<bool> tsc;

        public AmqpConnection(AmqpProvider provider, ITransportContext transport, NmsConnectionInfo info)
        {
            this.Provider = provider;
            this.transport = transport;
            this.remoteUri = provider.RemoteUri;
            this.Info = info;
            this.messageFactory = new AmqpMessageFactory(this);
        }

        public Connection UnderlyingConnection => underlyingConnection;
        public string QueuePrefix => Info.QueuePrefix;
        public string TopicPrefix => Info.TopicPrefix;
        public bool ObjectMessageUsesAmqpTypes { get; set; } = false;
        public NmsConnectionInfo Info { get; }

        public INmsMessageFactory MessageFactory => messageFactory;

        internal async Task Start()
        {
            Address address = UriUtil.ToAddress(remoteUri, Info.UserName, Info.Password);
            this.tsc = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            underlyingConnection = await transport.CreateAsync(address, new AmqpHandler(this)).ConfigureAwait(false);
            underlyingConnection.AddClosedCallback(OnClosed);

            // Wait for connection to be opened
            await this.tsc.Task.ConfigureAwait(false);

            // Create a Session for this connection that is used for Temporary Destinations
            // and perhaps later on management and advisory monitoring.
            NmsSessionInfo sessionInfo = new NmsSessionInfo(Info, -1);
            sessionInfo.AcknowledgementMode = AcknowledgementMode.AutoAcknowledge;

            connectionSession = new AmqpConnectionSession(this, sessionInfo);
            await connectionSession.Start().ConfigureAwait(false);
        }

        private void OnClosed(IAmqpObject sender, Error error)
        {
            if (Tracer.IsDebugEnabled)
            {
                Tracer.Debug($"Connection closed. {error}");
            }

            bool connectionExplicitlyClosed = error == null;
            if (!connectionExplicitlyClosed)
            {
                var exception = ExceptionSupport.GetException(error);
                if (!this.tsc.TrySetException(exception))
                {
                    Provider.OnConnectionClosed(exception);
                }
            }
        }

        internal void OnLocalOpen(Open open)
        {
            open.ContainerId = Info.ClientId;
            open.ChannelMax = Info.ChannelMax;
            open.MaxFrameSize = (uint) Info.MaxFrameSize;
            open.HostName = remoteUri.Host;
            open.IdleTimeOut = (uint) Info.IdleTimeOut;
            open.DesiredCapabilities = new[]
            {
                SymbolUtil.OPEN_CAPABILITY_SOLE_CONNECTION_FOR_CONTAINER,
                SymbolUtil.OPEN_CAPABILITY_DELAYED_DELIVERY,
                SymbolUtil.OPEN_CAPABILITY_ANONYMOUS_RELAY
            };
        }

        internal void OnRemoteOpened(Open open)
        {
            if (SymbolUtil.CheckAndCompareFields(open.Properties, SymbolUtil.CONNECTION_ESTABLISH_FAILED, SymbolUtil.BOOLEAN_TRUE))
            {
                Tracer.InfoFormat("Open response contains {0} property the connection {1} will soon be closed.",
                    SymbolUtil.CONNECTION_ESTABLISH_FAILED, Info.Id);
            }
            else
            {
                object value = SymbolUtil.GetFromFields(open.Properties, SymbolUtil.CONNECTION_PROPERTY_TOPIC_PREFIX);
                if (value is string topicPrefix)
                {
                    Info.TopicPrefix = topicPrefix;
                }

                value = SymbolUtil.GetFromFields(open.Properties, SymbolUtil.CONNECTION_PROPERTY_QUEUE_PREFIX);
                if (value is string queuePrefix)
                {
                    Info.QueuePrefix = queuePrefix;
                }

                this.tsc.TrySetResult(true);
                Provider.FireConnectionEstablished();
            }
        }

        public async Task CreateSession(NmsSessionInfo sessionInfo)
        {
            var amqpSession = new AmqpSession(this, sessionInfo);
            await amqpSession.Start().ConfigureAwait(false);
            sessions.TryAdd(sessionInfo.Id, amqpSession);
        }

        public void Close()
        {
            try
            {
                UnderlyingConnection?.Close();
            }
            catch (Exception ex)
            {
                // log network errors
                NMSException nmse = ExceptionSupport.Wrap(ex, "Amqp Connection close failure for NMS Connection {0}", this.Info.Id);
                Tracer.DebugFormat("Caught Exception while closing Amqp Connection {0}. Exception {1}", this.Info.Id, nmse);
            }
        }

        public AmqpSession GetSession(NmsSessionId sessionId)
        {
            if (sessions.TryGetValue(sessionId, out AmqpSession session))
            {
                return session;
            }
            throw new InvalidOperationException($"Amqp Session {sessionId} doesn't exist and cannot be retrieved.");
        }

        public void RemoveSession(NmsSessionId sessionId)
        {
            sessions.TryRemove(sessionId, out AmqpSession _);
        }

        public async Task CreateTemporaryDestination(NmsTemporaryDestination destination)
        {
            AmqpTemporaryDestination amqpTemporaryDestination = new AmqpTemporaryDestination(connectionSession, destination);
            await amqpTemporaryDestination.Attach();
            temporaryDestinations.TryAdd(destination, amqpTemporaryDestination);
        }

        public AmqpTemporaryDestination GetTemporaryDestination(NmsTemporaryDestination destination)
        {
            return temporaryDestinations.TryGetValue(destination, out AmqpTemporaryDestination amqpTemporaryDestination) ? amqpTemporaryDestination : null;
        }

        public void RemoveTemporaryDestination(NmsTemporaryDestination destinationId)
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