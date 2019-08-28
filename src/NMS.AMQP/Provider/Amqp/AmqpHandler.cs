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

using Amqp.Framing;
using Amqp.Handler;

namespace Apache.NMS.AMQP.Provider.Amqp
{
    internal class AmqpHandler : IHandler
    {
        private readonly AmqpConnection connection;

        public AmqpHandler(AmqpConnection connection)
        {
            this.connection = connection;
        }

        public bool CanHandle(EventId id)
        {
            switch (id)
            {
                case EventId.SendDelivery:
                case EventId.ConnectionRemoteOpen:
                case EventId.ConnectionLocalOpen:
                    return true;
                default:
                    return false;
            }
        }

        public void Handle(Event protocolEvent)
        {
            switch (protocolEvent.Id)
            {
                case EventId.SendDelivery when protocolEvent.Context is IDelivery delivery:
                    delivery.Batchable = false;
                    delivery.Settled = false;
                    break;
                case EventId.ConnectionRemoteOpen when protocolEvent.Context is Open open:
                    this.connection.OnRemoteOpened(open);
                    break;
                case EventId.ConnectionLocalOpen when protocolEvent.Context is Open open:
                    this.connection.OnLocalOpen(open);
                    break;
            }
        }
    }
}