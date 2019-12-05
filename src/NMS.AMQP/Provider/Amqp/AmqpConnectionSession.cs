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
using Amqp.Types;
using Apache.NMS.AMQP.Meta;
using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP.Provider.Amqp
{
    public class AmqpConnectionSession : AmqpSession
    {
        private readonly bool hasClientId;

        public AmqpConnectionSession(AmqpConnection connection, NmsSessionInfo sessionInfo) : base(connection, sessionInfo)
        {
            this.hasClientId = connection.Info.IsExplicitClientId;
        }

        public async Task Unsubscribe(string subscriptionName)
        {
            TaskCompletionSource<bool> tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            ReceiverLink receiverLink = new ReceiverLink(UnderlyingSession, subscriptionName, CreateAttach(subscriptionName), (link, attach) =>
            {
                Tracer.InfoFormat("Attempting to close subscription {0}. Attach response {1}", subscriptionName, attach);
                if (attach.Source is Source source)
                {
                    Tracer.InfoFormat("Found subscription {0} on remote with source {1}.", subscriptionName, source);
                    tcs.TrySetResult(true);
                }
            });
            receiverLink.AddClosedCallback((sender, error) =>
            {
                string failureMessage = string.Equals(sender.Error?.Condition, ErrorCode.NotFound)
                    ? $"Cannot remove Subscription {subscriptionName} that does not exists"
                    : $"Subscription {subscriptionName} unsubscribe operation failure";

                NMSException exception = ExceptionSupport.GetException(sender, failureMessage);
                tcs.TrySetException(exception);
            });

            await tcs.Task;
            
            receiverLink.Close(TimeSpan.FromMilliseconds(Connection.Provider.CloseTimeout));
        }

        private Attach CreateAttach(string subscriptionName)
        {
            Attach attach = new Attach
            {
                LinkName = subscriptionName,
                Target = new Target(),
                SndSettleMode = SenderSettleMode.Unsettled,
                RcvSettleMode = ReceiverSettleMode.First,
            };

            if (!this.hasClientId)
            {
                // We are trying to unsubscribe a 'global' shared subs using a 'null source lookup', add link
                // desired capabilities as hints to the peer to consider this when trying to attach the link.
                attach.DesiredCapabilities = new Symbol[] { SymbolUtil.SHARED, SymbolUtil.GLOBAL };
            }

            return attach;
        }
    }
}