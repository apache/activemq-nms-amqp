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

using System.Threading.Tasks;
using Apache.NMS.AMQP.Message;
using Apache.NMS.AMQP.Meta;
using Apache.NMS.AMQP.Provider;
using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP
{
    /// <summary>
    /// Used in non-transacted NMS Sessions to throw proper errors indicating
    /// that the Session is not transacted and cannot be treated as such.
    /// </summary>
    internal sealed class NmsNoTxTransactionContext : INmsTransactionContext
    {
        private readonly NmsSession session;

        public NmsNoTxTransactionContext(NmsSession session)
        {
            this.session = session;
        }

        public Task Acknowledge(InboundMessageDispatch envelope, AckType ackType)
        {
            return this.session.Connection.Acknowledge(envelope, ackType);
        }

        public Task Begin()
        {
            return Task.CompletedTask;
        }

        public Task Send(OutboundMessageDispatch envelope)
        {
            return this.session.Connection.Send(envelope);
        }

        public Task Rollback()
        {
            throw new IllegalStateException("Not a transacted session");
        }

        public Task Commit()
        {
            throw new IllegalStateException("Not a transacted session");
        }

        public Task Shutdown()
        {
            return Task.CompletedTask;
        }

        public void OnConnectionInterrupted()
        {
        }

        public Task OnConnectionRecovery(IProvider provider)
        {
            return Task.CompletedTask;
        }

        public bool IsActiveInThisContext(INmsResourceId infoId)
        {
            return false;
        }

        public event SessionTxEventDelegate TransactionStartedListener
        {
            add => throw new IllegalStateException("Not a transacted session");
            remove => throw new IllegalStateException("Not a transacted session");
        }

        public event SessionTxEventDelegate TransactionCommittedListener
        {
            add => throw new IllegalStateException("Not a transacted session");
            remove => throw new IllegalStateException("Not a transacted session");
        }

        public event SessionTxEventDelegate TransactionRolledBackListener
        {
            add => throw new IllegalStateException("Not a transacted session");
            remove => throw new IllegalStateException("Not a transacted session");
        }
    }
}