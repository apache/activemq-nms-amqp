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
using Amqp;
using Amqp.Framing;
using Apache.NMS.AMQP.Meta;
using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP.Provider.Amqp
{
    public class AmqpSession
    {
        private readonly ConcurrentDictionary<NmsConsumerId, AmqpConsumer> consumers = new ConcurrentDictionary<NmsConsumerId, AmqpConsumer>();
        private readonly ConcurrentDictionary<NmsProducerId, AmqpProducer> producers = new ConcurrentDictionary<NmsProducerId, AmqpProducer>();
        protected readonly NmsSessionInfo SessionInfo;

        public AmqpSession(AmqpConnection connection, NmsSessionInfo sessionInfo)
        {
            Connection = connection;
            SessionInfo = sessionInfo;

            if (sessionInfo.IsTransacted)
            {
                TransactionContext = new AmqpTransactionContext(this);
            }
        }

        public AmqpTransactionContext TransactionContext { get; }

        public AmqpConnection Connection { get; }
        public Session UnderlyingSession { get; private set; }

        public IEnumerable<AmqpConsumer> Consumers => consumers.Values.ToArray();
        public NmsSessionId SessionId => SessionInfo.Id;

        internal bool IsTransacted => SessionInfo.IsTransacted;

        internal bool IsTransactionFailed => TransactionContext?.IsTransactionFailed ?? false;

        public Task Start()
        {
            TaskCompletionSource<bool> tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            var requestTimeout = Connection.Provider.RequestTimeout;
            if (requestTimeout > 0)
            {
                CancellationTokenSource ct = new CancellationTokenSource(TimeSpan.FromMilliseconds(requestTimeout));
                ct.Token.Register(() => tcs.TrySetCanceled(), false);
            }

            UnderlyingSession = new Session(Connection.UnderlyingConnection, CreateBeginFrame(),
                (session, begin) =>
                {
                    tcs.TrySetResult(true);
                });
            UnderlyingSession.AddClosedCallback((sender, error) =>
            {
                if (!tcs.TrySetException(ExceptionSupport.GetException(error)))
                {
                    Connection.RemoveSession(SessionInfo.Id);
                }
            });
            return tcs.Task;
        }

        public void Close()
        {
            long closeTimeout = Connection.Provider.CloseTimeout;
            TimeSpan timeout = TimeSpan.FromMilliseconds(closeTimeout);
            UnderlyingSession.Close(timeout);
            Connection.RemoveSession(SessionInfo.Id);
        }

        public Task BeginTransaction(NmsTransactionInfo transactionInfo)
        {
            if (!SessionInfo.IsTransacted)
            {
                throw new IllegalStateException("Non-transacted Session cannot start a TX.");
            }

            return TransactionContext.Begin(transactionInfo);
        }

        private Begin CreateBeginFrame()
        {
            return new Begin
            {
                HandleMax = Connection.Provider.MaxHandle,
                IncomingWindow = int.MaxValue, // value taken from qpid-jms
                OutgoingWindow = Connection.Provider.SessionOutgoingWindow,
                NextOutgoingId = 1
            };
        }

        public async Task CreateConsumer(NmsConsumerInfo consumerInfo)
        {
            AmqpConsumer amqpConsumer = new AmqpConsumer(this, consumerInfo);
            await amqpConsumer.Attach();
            consumers.TryAdd(consumerInfo.Id, amqpConsumer);
        }

        public async Task CreateProducer(NmsProducerInfo producerInfo)
        {
            var amqpProducer = new AmqpProducer(this, producerInfo);
            await amqpProducer.Attach();
            producers.TryAdd(producerInfo.Id, amqpProducer);
        }

        public AmqpConsumer GetConsumer(NmsConsumerId consumerId)
        {
            if (consumers.TryGetValue(consumerId, out var consumer))
            {
                return consumer;
            }

            throw new Exception();
        }

        public AmqpProducer GetProducer(NmsProducerId producerId)
        {
            if (producers.TryGetValue(producerId, out var producer))
            {
                return producer;
            }

            throw new Exception();
        }

        public void RemoveConsumer(NmsConsumerId consumerId)
        {
            consumers.TryRemove(consumerId, out _);
        }

        public void RemoveProducer(NmsProducerId producerId)
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

        /// <summary>
        /// Roll back the currently running Transaction
        /// </summary>
        /// <param name="transactionInfo">The TransactionInfo describing the transaction being rolled back.</param>
        /// <param name="nextTransactionInfo">The TransactionInfo describing the transaction that should be started immediately.</param>
        /// <exception cref="Exception">throws Exception if an error occurs while performing the operation.</exception>
        public Task Rollback(NmsTransactionInfo transactionInfo, NmsTransactionInfo nextTransactionInfo)
        {
            if (!SessionInfo.IsTransacted)
            {
                throw new IllegalStateException("Non-transacted Session cannot rollback a TX.");
            }

            return TransactionContext.Rollback(transactionInfo, nextTransactionInfo);
        }

        /// <summary>
        /// Commit the currently running Transaction.
        /// </summary>
        /// <param name="transactionInfo">the TransactionInfo describing the transaction being committed.</param>
        /// <param name="nextTransactionInfo">the TransactionInfo describing the transaction that should be started immediately.</param>
        /// <exception cref="Exception">throws Exception if an error occurs while performing the operation.</exception>
        public Task Commit(NmsTransactionInfo transactionInfo, NmsTransactionInfo nextTransactionInfo)
        {
            if (!SessionInfo.IsTransacted)
            {
                throw new IllegalStateException("Non-transacted Session cannot commit a TX.");
            }

            return TransactionContext.Commit(transactionInfo, nextTransactionInfo);
        }
    }
}