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
using System.Threading;
using System.Threading.Tasks;
using Amqp;
using Amqp.Framing;
using Apache.NMS.AMQP.Meta;
using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP.Provider.Amqp
{
    internal class AmqpSendTask : TaskCompletionSource<Outcome>
    {
        private readonly Timer timer;
        
        public AmqpSendTask(SenderLink link, global::Amqp.Message message, DeliveryState deliveryState, long timeoutMillis)
        {
            if (timeoutMillis != NmsConnectionInfo.INFINITE)
            {
                this.timer = new Timer(OnTimer, this, timeoutMillis, -1);
            }
            
            try
            {
                link.Send(message, deliveryState, OnOutcome, this);
            }
            catch (Exception e)
            {
                this.timer?.Dispose();
                this.SetException(ExceptionSupport.Wrap(e));
            }
        }
        
        private static void OnOutcome(ILink link, global::Amqp.Message message, Outcome outcome, object state)
        {
            var thisPtr = (AmqpSendTask) state;
            thisPtr.timer?.Dispose();
            thisPtr.TrySetResult(outcome);
        }
        
        private static void OnTimer(object state)
        {
            var thisPtr = (AmqpSendTask) state;
            thisPtr.timer.Dispose();
            thisPtr.TrySetException(new TimeoutException());
        }
    }
}