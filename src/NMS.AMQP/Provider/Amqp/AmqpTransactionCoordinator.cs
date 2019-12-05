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
using Amqp.Transactions;
using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP.Provider.Amqp
{
    public class AmqpTransactionCoordinator : SenderLink
    {
        private static readonly global::Amqp.Message DeclareMessage = new global::Amqp.Message(new Declare());

        private readonly AmqpSession session;

        public AmqpTransactionCoordinator(AmqpSession session) : base(session.UnderlyingSession, GetName(), new Attach
        {
            Target = new Coordinator
            {
                Capabilities = new[] { TxnCapabilities.LocalTransactions }
            },
            Source = new Source
            {
                Outcomes = new[] { SymbolUtil.ATTACH_OUTCOME_ACCEPTED, SymbolUtil.ATTACH_OUTCOME_REJECTED, SymbolUtil.ATTACH_OUTCOME_RELEASED, SymbolUtil.ATTACH_OUTCOME_MODIFIED },
            },
            SndSettleMode = SenderSettleMode.Unsettled,
            RcvSettleMode = ReceiverSettleMode.First
        }, null)
        {
            this.session = session;
        }

        private static string GetName() => "transaction-link-" + Guid.NewGuid().ToString("N").Substring(0, 5);

        public async Task<byte[]> DeclareAsync()
        {
            var outcome = await this.SendAsync(DeclareMessage, null, this.session.Connection.Provider.RequestTimeout).ConfigureAwait(false);
            if (outcome.Descriptor.Code == MessageSupport.DECLARED_INSTANCE.Descriptor.Code)
            {
                return ((Declared) outcome).TxnId;
            }
            else if (outcome.Descriptor.Code == MessageSupport.REJECTED_INSTANCE.Descriptor.Code)
            {
                var rejected = (Rejected) outcome;
                var rejectedError = rejected.Error ?? new Error(ErrorCode.InternalError);
                throw new AmqpException(rejectedError);
            }
            else
            {
                throw new NMSException(outcome.ToString(), ErrorCode.InternalError);
            }
        }

        public async Task DischargeAsync(byte[] txnId, bool fail)
        {
            var message = new global::Amqp.Message(new Discharge { TxnId = txnId, Fail = fail });
            var outcome = await this.SendAsync(message, null, this.session.Connection.Provider.RequestTimeout).ConfigureAwait(false);

            if (outcome.Descriptor.Code == MessageSupport.ACCEPTED_INSTANCE.Descriptor.Code)
            {
                // accepted, do nothing
            }
            else if (outcome.Descriptor.Code == MessageSupport.REJECTED_INSTANCE.Descriptor.Code)
            {
                var rejected = (Rejected) outcome;
                var rejectedError = rejected.Error ?? new Error(ErrorCode.TransactionRollback);
                throw new TransactionRolledBackException(rejectedError.Condition, rejectedError.Description);
            }
            else
            {
                throw new NMSException(outcome.ToString(), ErrorCode.InternalError);
            }
        }
    }
}