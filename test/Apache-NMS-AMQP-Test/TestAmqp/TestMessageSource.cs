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
using Amqp.Listener;

namespace NMS.AMQP.Test.TestAmqp
{
    class TestMessageSource : IMessageSource
    {
        private readonly Queue<Amqp.Message> messages;
        private readonly List<Amqp.Message> rejectedMessages;
        private readonly List<Amqp.Message> releasedMessages;
        private readonly List<Amqp.Message> acceptedMessages;

        public TestMessageSource()
        {
            this.messages = new Queue<Amqp.Message>();
            this.rejectedMessages = new List<Amqp.Message>();
            this.releasedMessages = new List<Amqp.Message>();
            this.acceptedMessages = new List<Amqp.Message>();
        }

        public IEnumerable<Amqp.Message> ReleasedMessages => releasedMessages;

        public IEnumerable<Amqp.Message> AcceptedMessages => acceptedMessages;

        public Task<ReceiveContext> GetMessageAsync(ListenerLink link)
        {
            lock (this.messages)
            {
                ReceiveContext context = null;
                if (this.messages.Count > 0)
                {
                    context = new ReceiveContext(link, this.messages.Dequeue());
                }

                var tcs = new TaskCompletionSource<ReceiveContext>();
                tcs.SetResult(context);
                return tcs.Task;
            }
        }

        public void SendMessage(string payload)
        {
            Amqp.Message message = new Amqp.Message(payload) { Properties = new Properties { MessageId = Guid.NewGuid().ToString() } };
            SendMessage(message);
        }

        public void SendMessage(Amqp.Message message)
        {
            lock (messages)
            {
                this.messages.Enqueue(message);
            }
        }

        public void DisposeMessage(ReceiveContext receiveContext, DispositionContext dispositionContext)
        {
            switch (dispositionContext.DeliveryState)
            {
                case Accepted _:
                    this.acceptedMessages.Add(receiveContext.Message);
                    break;
                case Rejected _:
                    this.rejectedMessages.Add(receiveContext.Message);
                    break;
                case Released _:
                    this.releasedMessages.Add(receiveContext.Message);
                    break;
            }

            dispositionContext.Complete();
        }
    }
}