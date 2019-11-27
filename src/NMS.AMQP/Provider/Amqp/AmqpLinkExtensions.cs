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
using Amqp;
using Amqp.Framing;

namespace Apache.NMS.AMQP.Provider.Amqp
{
    internal static class AmqpLinkExtensions
    {
        internal static bool IsDetaching(this Link link)
        {
            return link.LinkState >= LinkState.DetachPipe;
        }

        internal static Task<Outcome> SendAsync(this SenderLink link, global::Amqp.Message message, DeliveryState deliveryState, long timeoutMillis)
        {
            return new AmqpSendTask(link, message, deliveryState, timeoutMillis).Task;
        }
    }
}