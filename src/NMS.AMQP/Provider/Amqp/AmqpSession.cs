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
using Amqp.Framing;
using Apache.NMS.AMQP.Meta;
using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP.Provider.Amqp
{
    public class AmqpSession
    {
        protected readonly SessionInfo sessionInfo;
        private global::Amqp.Session underlyingSession;
        private readonly ConcurrentDictionary<Id, AmqpConsumer> consumers = new ConcurrentDictionary<Id, AmqpConsumer>();
        private readonly ConcurrentDictionary<Id, AmqpProducer> producers = new ConcurrentDictionary<Id, AmqpProducer>();

        public AmqpSession(AmqpConnection connection, SessionInfo sessionInfo)
        {
            this.Connection = connection;
            this.sessionInfo = sessionInfo;
        }

        public AmqpConnection Connection { get; }
        public global::Amqp.Session UnderlyingSession => underlyingSession;

        public IEnumerable<AmqpConsumer> Consumers => consumers.Values.ToArray();

        public Task Start()
        {
            TaskCompletionSource<bool> tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            if (sessionInfo.requestTimeout > 0)
            {
                CancellationTokenSource ct = new CancellationTokenSource(TimeSpan.FromMilliseconds(sessionInfo.requestTimeout));
                ct.Token.Register(() => tcs.TrySetCanceled(), false);
            }
            
            underlyingSession = new global::Amqp.Session(Connection.UnderlyingConnection, CreateBeginFrame(),
                (session, begin) =>
                {
                    sessionInfo.remoteChannel = begin.RemoteChannel;
                    tcs.TrySetResult(true);
                });
            underlyingSession.AddClosedCallback((sender, error) =>
            {
                if (!tcs.TrySetException(ExceptionSupport.GetException(error)))
                {
                    Connection.RemoveSession(sessionInfo.Id);
                }
            });
            return tcs.Task;
        }

        public void Close()
        {
            underlyingSession.Close(TimeSpan.FromMilliseconds(sessionInfo.closeTimeout));
            Connection.RemoveSession(sessionInfo.Id);
        }

        private Begin CreateBeginFrame()
        {
            return new Begin
            {
                HandleMax = Connection.Provider.MaxHandle,
                IncomingWindow = sessionInfo.incomingWindow,
                OutgoingWindow = sessionInfo.outgoingWindow,
                NextOutgoingId = sessionInfo.nextOutgoingId,
            };
        }

        public async Task CreateConsumer(ConsumerInfo consumerInfo)
        {
            AmqpConsumer amqpConsumer = new AmqpConsumer(this, consumerInfo);
            await amqpConsumer.Attach();
            consumers.TryAdd(consumerInfo.Id, amqpConsumer);
        }

        public async Task CreateProducer(ProducerInfo producerInfo)
        {
            var amqpProducer = new AmqpProducer(this, producerInfo);
            await amqpProducer.Attach();
            producers.TryAdd(producerInfo.Id, amqpProducer);
        }

        public AmqpConsumer GetConsumer(Id consumerId)
        {
            if (consumers.TryGetValue(consumerId, out var consumer))
            {
                return consumer;
            }

            throw new Exception();
        }

        public AmqpProducer GetProducer(Id producerId)
        {
            if (producers.TryGetValue(producerId, out var producer))
            {
                return producer;
            }

            throw new Exception();
        }

        public void RemoveConsumer(Id consumerId)
        {
            consumers.TryRemove(consumerId, out _);
        }

        public void RemoveProducer(Id producerId)
        {
            producers.TryRemove(producerId, out _);
        }

        /// <summary>
        /// Perform re-send of all delivered but not yet acknowledged messages for all consumers
        /// active in this Session.
        /// </summary>
        public void Recover()
        {
            foreach (var consumer in consumers.Values)
            {
                consumer.Recover();
            }
        }

        public bool ContainsSubscriptionName(string subscriptionName)
        {
            return consumers.Values.Any(consumer => consumer.HasSubscription(subscriptionName));
        }
    }
}