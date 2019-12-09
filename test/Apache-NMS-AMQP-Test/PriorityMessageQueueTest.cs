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
using Apache.NMS;
using Apache.NMS.AMQP.Message;
using Apache.NMS.AMQP.Util;
using NMS.AMQP.Test.Message.Facade;
using NUnit.Framework;

namespace NMS.AMQP.Test
{
    [TestFixture]
    public class PriorityMessageQueueTest
    {
        private readonly IdGenerator messageId = new IdGenerator();

        [Test]
        public void TestCreate()
        {
            PriorityMessageQueue queue = new PriorityMessageQueue();

            Assert.AreEqual(0, queue.Count);
        }

        [Test]
        public void TestDequeueWhenQueueIsNotEmpty()
        {
            PriorityMessageQueue queue = new PriorityMessageQueue();
            InboundMessageDispatch message = CreateEnvelope();
            queue.EnqueueFirst(message);
            Assert.False(queue.IsEmpty);
            Assert.AreSame(message, queue.Dequeue(1));
        }

        [Test]
        public void TestDequeueZeroWhenQueueIsNotEmpty()
        {
            PriorityMessageQueue queue = new PriorityMessageQueue();
            InboundMessageDispatch message = CreateEnvelope();
            queue.EnqueueFirst(message);
            Assert.False(queue.IsEmpty);
            Assert.AreSame(message, queue.Dequeue(1));
        }

        [Test]
        public void TestDequeueZeroWhenQueueIsEmpty()
        {
            PriorityMessageQueue queue = new PriorityMessageQueue();
            Assert.IsNull(queue.Dequeue(0));
        }

        [Test]
        public void TestDequeueReturnsWhenQueueIsDisposed()
        {
            PriorityMessageQueue queue = new PriorityMessageQueue();

            Task.Run(async () =>
            {
                await Task.Delay(TimeSpan.FromMilliseconds(100));
                queue.Dispose();
            });

            Assert.IsNull(queue.Dequeue(-1));
        }


        [Test]
        public void TestRemoveFirstOnEmptyQueue()
        {
            PriorityMessageQueue queue = new PriorityMessageQueue();
            Assert.IsNull(queue.DequeueNoWait());
        }

        [Test]
        public void TestEnqueueFirstOverridesPriority()
        {
            PriorityMessageQueue queue = new PriorityMessageQueue();
            
            // Add a higher priority message
            InboundMessageDispatch message1 = CreateEnvelope(MsgPriority.High);
            queue.Enqueue(message1);
            
            // Add other lower priority messages 'first'.
            InboundMessageDispatch message2 = CreateEnvelope(MsgPriority.BelowNormal);
            InboundMessageDispatch message3 = CreateEnvelope(MsgPriority.AboveLow);
            InboundMessageDispatch message4 = CreateEnvelope(MsgPriority.Low);
            
            queue.EnqueueFirst(message2);
            queue.EnqueueFirst(message3);
            queue.EnqueueFirst(message4);
            
            // Verify they dequeue in the reverse of the order
            // they were added, and not priority order.
            Assert.AreSame(message4, queue.DequeueNoWait());
            Assert.AreSame(message3, queue.DequeueNoWait());
            Assert.AreSame(message2, queue.DequeueNoWait());
            Assert.AreSame(message1, queue.DequeueNoWait());
        }

        [Test]
        public void TestRemoveFirst()
        {
            PriorityMessageQueue queue = new PriorityMessageQueue();

            IEnumerable<InboundMessageDispatch> messages = CreateFullRangePrioritySet();
            foreach (InboundMessageDispatch envelope in messages)
            {
                queue.Enqueue(envelope);
            }

            for (int i = (int) MsgPriority.Highest; i >= 0; i--)
            {
                InboundMessageDispatch first = queue.DequeueNoWait();
                Assert.AreEqual(i, (int) first.Message.NMSPriority);
            }

            Assert.True(queue.IsEmpty);
        }

        [Test]
        public void TestRemoveFirstSparse()
        {
            PriorityMessageQueue queue = new PriorityMessageQueue();
            
            queue.Enqueue(CreateEnvelope(MsgPriority.High));
            queue.Enqueue(CreateEnvelope(MsgPriority.BelowNormal));
            queue.Enqueue(CreateEnvelope(MsgPriority.VeryLow));
            
            Assert.AreEqual(MsgPriority.High, queue.DequeueNoWait().Message.NMSPriority);
            Assert.AreEqual(MsgPriority.BelowNormal, queue.DequeueNoWait().Message.NMSPriority);
            Assert.AreEqual(MsgPriority.VeryLow, queue.DequeueNoWait().Message.NMSPriority);
            
            Assert.True(queue.IsEmpty);
        }

        [Test]
        public void TestDequeueWaitsUntilMessageArrives()
        {
            DoDequeueWaitsUntilMessageArrivesTestImpl(-1);
        }

        [Test, Timeout(2000)]
        public void TestDequeueTimedWaitsUntilMessageArrives()
        {
            DoDequeueWaitsUntilMessageArrivesTestImpl(5000);
        }

        private void DoDequeueWaitsUntilMessageArrivesTestImpl(int timeout)
        {
            PriorityMessageQueue queue = new PriorityMessageQueue();
            InboundMessageDispatch message = CreateEnvelope();

            Task.Run(async () =>
            {
                await Task.Delay(TimeSpan.FromMilliseconds(100));
                queue.Enqueue(message);
            });
            
            Assert.AreSame(message, queue.Dequeue(timeout));
        }

        [Test]
        public void TestEnqueueFirst()
        {
            PriorityMessageQueue queue = new PriorityMessageQueue();
            
            InboundMessageDispatch message1 = CreateEnvelope();
            InboundMessageDispatch message2 = CreateEnvelope();
            InboundMessageDispatch message3 = CreateEnvelope();
            
            queue.EnqueueFirst(message1);
            queue.EnqueueFirst(message2);
            queue.EnqueueFirst(message3);
            
            Assert.AreSame(message3, queue.DequeueNoWait());
            Assert.AreSame(message2, queue.DequeueNoWait());
            Assert.AreSame(message1, queue.DequeueNoWait());
            
        }

        [Test]
        public void TestClear()
        {
            PriorityMessageQueue queue = new PriorityMessageQueue();

            IEnumerable<InboundMessageDispatch> messages = CreateFullRangePrioritySet();
            foreach (InboundMessageDispatch envelope in messages)
            {
                queue.Enqueue(envelope);
            }
            
            Assert.False(queue.IsEmpty);
            queue.Clear();
            Assert.True(queue.IsEmpty);
            Assert.IsNull(queue.DequeueNoWait());
        }

        private IEnumerable<InboundMessageDispatch> CreateFullRangePrioritySet()
        {
            for (int i = 0; i <= (int) MsgPriority.Highest; i++)
            {
                yield return CreateEnvelope((MsgPriority) i);
            }
        }

        private InboundMessageDispatch CreateEnvelope(MsgPriority priority = MsgPriority.BelowNormal)
        {
            InboundMessageDispatch envelope = new InboundMessageDispatch();
            envelope.Message = CreateMessage(priority);
            return envelope;
        }

        private NmsMessage CreateMessage(MsgPriority priority)
        {
            NmsTestMessageFacade facade = new NmsTestMessageFacade();
            facade.NMSPriority = priority;
            facade.NMSMessageId = messageId.GenerateId();
            return new NmsMessage(facade);
        }
    }
}