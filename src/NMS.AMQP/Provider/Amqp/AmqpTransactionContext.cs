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
using System.Collections.Generic;
using System.Threading.Tasks;
using Amqp.Framing;
using Amqp.Transactions;
using Apache.NMS.AMQP.Meta;
using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP.Provider.Amqp
{
    public class AmqpTransactionContext
    {
        private readonly AmqpSession session;
        private readonly Dictionary<NmsConsumerId, AmqpConsumer> txConsumers = new Dictionary<NmsConsumerId, AmqpConsumer>();
        private TransactionalState cachedAcceptedState;
        private TransactionalState cachedTransactedState;
        private AmqpTransactionCoordinator coordinator;
        private NmsTransactionId current;
        private byte[] txnId;

        public AmqpTransactionContext(AmqpSession session)
        {
            this.session = session;
        }

        public bool IsTransactionFailed => coordinator != null && coordinator.IsDetaching();
        
        public TransactionalState GetTxnEnrolledState()
        {
            return this.cachedTransactedState;
        }

        public TransactionalState GetTxnAcceptState()
        {
            return this.cachedAcceptedState;
        }

        public async Task Rollback(NmsTransactionInfo transactionInfo, NmsTransactionInfo nextTransactionInfo)
        {
            if (!Equals(transactionInfo.Id, this.current))
            {
                if (!transactionInfo.IsInDoubt && this.current == null)
                    throw new IllegalStateException("Rollback called with no active Transaction.");
                if (!transactionInfo.IsInDoubt && this.current != null)
                    throw new IllegalStateException("Attempt to rollback a transaction other than the current one");
                return;
            }

            Tracer.Debug($"TX Context{this} rolling back current TX[{this.current}]");

            this.current = null;
            await this.coordinator.DischargeAsync(this.txnId, true).ConfigureAwait(false);
            

            PostRollback();

            if (nextTransactionInfo != null)
            {
                await Begin(nextTransactionInfo).ConfigureAwait(false);
            }
        }

        private void PostRollback()
        {
            foreach (AmqpConsumer consumer in this.txConsumers.Values)
            {
                consumer.PostRollback();
            }

            this.txConsumers.Clear();
        }

        public async Task Commit(NmsTransactionInfo transactionInfo, NmsTransactionInfo nextTransactionInfo)
        {
            if (!Equals(transactionInfo.Id, this.current))
            {
                if (!transactionInfo.IsInDoubt && this.current == null)
                    throw new IllegalStateException("Commit called with no active Transaction.");
                if (!transactionInfo.IsInDoubt && this.current != null)
                    throw new IllegalStateException("Attempt to Commit a transaction other than the current one");
                throw new TransactionRolledBackException("Transaction in doubt and cannot be committed.");
            }

            Tracer.Debug($"TX Context{this} committing back current TX[{this.current}]");

            this.current = null;
            await this.coordinator.DischargeAsync(this.txnId, false).ConfigureAwait(false);

            PostCommit();

            await Begin(nextTransactionInfo).ConfigureAwait(false);
        }

        private void PostCommit()
        {
            this.txConsumers.Clear();
        }

        public async Task Begin(NmsTransactionInfo transactionInfo)
        {
            if (this.current != null)
                throw new NMSException("Begin called while a TX is still Active.");

            if (this.coordinator == null || this.coordinator.IsDetaching())
            {
                this.coordinator = new AmqpTransactionCoordinator(this.session);
            }

            this.txnId = await this.coordinator.DeclareAsync().ConfigureAwait(false);
            this.current = transactionInfo.Id;
            transactionInfo.ProviderTxId = this.txnId;
            this.cachedTransactedState = new TransactionalState { TxnId = this.txnId };
            this.cachedAcceptedState = new TransactionalState
            {
                Outcome = new Accepted(),
                TxnId = this.txnId
            };
        }

        public void RegisterTxConsumer(AmqpConsumer consumer)
        {
            this.txConsumers[consumer.ConsumerId] = consumer;
        }

        public override string ToString()
        {
            return this.session.SessionId + ": txContext";
        }

        public void Close(TimeSpan timeout)
        {
            this.coordinator.Close(timeout);
        }
    }
}