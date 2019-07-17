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
        public AmqpTransactionCoordinator(AmqpSession session) : base(session.UnderlyingSession, GetName(), new Attach { Target = new Coordinator(), Source = new Source() }, null)
        {
        }

        private static string GetName() => "transaction-link-" + Guid.NewGuid().ToString("N").Substring(0, 5);

        public Task<byte[]> DeclareAsync()
        {
            var message = new global::Amqp.Message(new Declare());
            var tcs = new TaskCompletionSource<byte[]>();
            Send(message, null, OnDeclareOutcome, tcs);
            return tcs.Task;
        }

        private static void OnDeclareOutcome(ILink link, global::Amqp.Message message, Outcome outcome, object state)
        {
            var tcs = (TaskCompletionSource<byte[]>) state;
            if (outcome.Descriptor.Code == MessageSupport.DECLARED_INSTANCE.Descriptor.Code)
                tcs.SetResult(((Declared) outcome).TxnId);
            else if (outcome.Descriptor.Code == MessageSupport.REJECTED_INSTANCE.Descriptor.Code)
                tcs.SetException(new AmqpException(((Rejected) outcome).Error));
            else
                tcs.SetCanceled();
        }

        public Task DischargeAsync(byte[] txnId, bool fail)
        {
            var message = new global::Amqp.Message(new Discharge { TxnId = txnId, Fail = fail });
            var tcs = new TaskCompletionSource<bool>();
            Send(message, null, OnDischargeOutcome, tcs);
            return tcs.Task;
        }

        private static void OnDischargeOutcome(ILink link, global::Amqp.Message message, Outcome outcome, object state)
        {
            var tcs = (TaskCompletionSource<bool>) state;
            if (outcome.Descriptor.Code == MessageSupport.ACCEPTED_INSTANCE.Descriptor.Code)
                tcs.SetResult(true);
            else if (outcome.Descriptor.Code == MessageSupport.REJECTED_INSTANCE.Descriptor.Code)
                tcs.SetException(new AmqpException(((Rejected) outcome).Error));
            else
                tcs.SetCanceled();
        }
    }
}