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
using System.Linq;
using System.Threading.Tasks;
using Apache.NMS.AMQP.Message;
using Apache.NMS.AMQP.Meta;
using Apache.NMS.AMQP.Provider;
using Apache.NMS.AMQP.Util;
using Apache.NMS.Util;

namespace Apache.NMS.AMQP
{
    /// <summary>
    /// Manages the details of a Session operating inside of a local NMS transaction.
    /// </summary>
    internal sealed class NmsLocalTransactionContext : INmsTransactionContext
    {
        private readonly NmsConnection connection;
        private readonly HashSet<INmsResourceId> participants = new HashSet<INmsResourceId>();
        private readonly NmsSession session;
        private NmsTransactionInfo transactionInfo;

        public NmsLocalTransactionContext(NmsSession session)
        {
            this.session = session;
            this.connection = session.Connection;
        }

        public async Task Send(OutboundMessageDispatch envelope)
        {
            if (!IsInDoubt())
            {
                await this.connection.Send(envelope);
                this.participants.Add(envelope.ProducerId);
            }
        }

        public async Task Acknowledge(InboundMessageDispatch envelope, AckType ackType)
        {
            // Consumed or delivered messages fall into a transaction otherwise just pass it in.
            if (ackType == AckType.ACCEPTED || ackType == AckType.DELIVERED)
            {
                try
                {
                    await this.connection.Acknowledge(envelope, ackType).ConfigureAwait(false);
                    this.participants.Add(envelope.ConsumerId);
                    Tracer.Debug($"TX:{this.transactionInfo.Id} has performed an acknowledge.");
                }
                catch (Exception)
                {
                    Tracer.Debug($"TX:{this.transactionInfo.Id} has failed an acknowledge.");
                    this.participants.Add(envelope.ConsumerId);
                    throw;
                }
            }
            else
            {
                await this.connection.Acknowledge(envelope, ackType).ConfigureAwait(false);
            }
        }

        public async Task Begin()
        {
            this.transactionInfo = GetNextTransactionInfo();

            try
            {
                Reset();
                await this.session.Connection.CreateResource(this.transactionInfo);
                OnTransactionStarted();
                Tracer.Debug($"Begin: {this.transactionInfo.Id}");
            }
            catch (Exception)
            {
                this.transactionInfo.SetInDoubt();
                throw;
            }
        }

        public Task Rollback()
        {
            return DoRollback(true);
        }

        public Task Shutdown()
        {
            return DoRollback(false);
        }

        public void OnConnectionInterrupted()
        {
            this.transactionInfo.SetInDoubt();
        }

        public async Task OnConnectionRecovery(IProvider provider)
        {
            if (this.participants.Any())
            {
                Tracer.Debug($"Transaction recovery marking current TX:{this.transactionInfo.Id} as in-doubt.");
                this.transactionInfo.SetInDoubt();
            }
            else
            {
                this.transactionInfo = GetNextTransactionInfo();
                Tracer.Debug($"Transaction recovery creating new TX:{this.transactionInfo.Id} after failover.");
                await provider.CreateResource(this.transactionInfo).ConfigureAwait(false);
            }
        }

        public bool IsActiveInThisContext(INmsResourceId infoId)
        {
            return this.participants.Contains(infoId);
        }

        public event SessionTxEventDelegate TransactionStartedListener;

        public event SessionTxEventDelegate TransactionCommittedListener;

        public event SessionTxEventDelegate TransactionRolledBackListener;

        public async Task Commit()
        {
            if (IsInDoubt())
            {
                try
                {
                    await Rollback();
                }
                catch (Exception e)
                {
                    Tracer.WarnFormat("Error during rollback of failed TX: ", e);
                }

                throw new TransactionRolledBackException("Transaction failed and has been rolled back.");
            }
            Tracer.Debug($"Commit: {this.transactionInfo.Id}");

            var oldTransactionId = this.transactionInfo.Id;
            var nextTx = GetNextTransactionInfo();

            try
            {
                await this.connection.Commit(this.transactionInfo, nextTx).ConfigureAwait(false);
                OnTransactionCommitted();
                Reset();
                this.transactionInfo = nextTx;
            }
            catch (NMSException)
            {
                Tracer.Info($"Commit failed for transaction :{oldTransactionId}");
                throw;
            }
            catch (Exception e)
            {
                throw NMSExceptionSupport.Create(e);
            }
            finally
            {
                try
                {
                    // If the provider failed to start a new transaction there will not be
                    // a current provider transaction id present, so we attempt to create
                    // one to recover our state.
                    if (nextTx.ProviderTxId == null)
                    {
                        await Begin().ConfigureAwait(false);
                    }
                }
                catch (Exception e)
                {
                    // TODO
                    // At this point the transacted session is now unrecoverable, we should
                    // probably close it.
                    Tracer.Info($"Failed to start new Transaction after failed rollback of: {oldTransactionId} {e}");
                }
            }
        }

        private async Task DoRollback(bool startNewTransaction)
        {
            Tracer.Debug($"Rollback: {this.transactionInfo.Id}");

            var oldTransactionId = this.transactionInfo.Id;
            var nextTx = startNewTransaction ? GetNextTransactionInfo() : null;

            try
            {
                await this.connection.Rollback(this.transactionInfo, nextTx);
                OnTransactionRolledBack();
                Reset();
                this.transactionInfo = nextTx;
            }
            catch (Exception e)
            {
                Tracer.Info($"Rollback failed for transaction: {oldTransactionId}");
                throw NMSExceptionSupport.Create(e);
            }
            finally
            {
                Reset();

                try
                {
                    // If the provider failed to start a new transaction there will not be
                    // a current provider transaction id present, so we attempt to create
                    // one to recover our state.
                    if (startNewTransaction && nextTx.ProviderTxId == null)
                    {
                        await Begin();
                    }
                }
                catch (Exception e)
                {
                    // TODO
                    // At this point the transacted session is now unrecoverable, we should
                    // probably close it.
                    Tracer.Info($"Failed to start new Transaction after failed rollback of: {this.transactionInfo} {e}");
                }
            }
        }

        private NmsTransactionInfo GetNextTransactionInfo()
        {
            NmsTransactionId transactionId = this.connection.GetNextTransactionId();
            return new NmsTransactionInfo(this.session.SessionInfo.Id, transactionId);
        }

        private bool IsInDoubt()
        {
            return this.transactionInfo?.IsInDoubt ?? false;
        }

        private void Reset()
        {
            this.participants.Clear();
        }

        private void OnTransactionStarted()
        {
            try
            {
                TransactionStartedListener?.Invoke(this.session);
            }
            catch (Exception e)
            {
                Tracer.Warn($"Local TX listener error ignored: {e}");
            }
        }

        private void OnTransactionCommitted()
        {
            try
            {
                TransactionCommittedListener?.Invoke(this.session);
            }
            catch (Exception e)
            {
                Tracer.Warn($"Local TX listener error ignored: {e}");
            }
        }

        private void OnTransactionRolledBack()
        {
            try
            {
                TransactionRolledBackListener?.Invoke(this.session);
            }
            catch (Exception e)
            {
                Tracer.Warn($"Local TX listener error ignored: {e}");
            }
        }
    }
}