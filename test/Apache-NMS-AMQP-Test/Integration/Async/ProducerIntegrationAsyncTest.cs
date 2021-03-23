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
using Amqp.Framing;
using Apache.NMS;
using NMS.AMQP.Test.TestAmqp;
using NUnit.Framework;

namespace NMS.AMQP.Test.Integration.Async
{
    [TestFixture]
    public class ProducerIntegrationAsyncTestAsync : IntegrationTestFixture
    {
        [Test, Timeout(20_000)]
        public async Task TestSentAsyncIsAsynchronous()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await base.EstablishConnectionAsync(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = await session.GetQueueAsync("myQueue");
                IMessageProducer producer = await session.CreateProducerAsync(queue);

                // Create and transfer a new message
                String text = "myMessage";
                testPeer.ExpectTransfer(messageMatcher: m => Assert.AreEqual(text, (m.BodySection as AmqpValue).Value),
                    settled: false,
                    sendResponseDisposition: true,
                    responseState: new Accepted(),
                    responseSettled: true,
                    stateMatcher: Assert.IsNull,
                    dispositionDelay: 10); // 10ms should be enough
                testPeer.ExpectClose();

                ITextMessage message = await session.CreateTextMessageAsync(text);
                var sendTask = producer.SendAsync(message);
                // Instantly check if its not completed yet, we want async, so it should not be completed right after 
                Assert.AreEqual(false, sendTask.IsCompleted);
                
                // And now wait for task to complete
                sendTask.Wait(20_000);

                await connection.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }
        
        [Test, Timeout(20_000)]
        public async Task TestSentAsync()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await base.EstablishConnectionAsync(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = await session.GetQueueAsync("myQueue");
                IMessageProducer producer = await session.CreateProducerAsync(queue);

                // Create and transfer a new message
                String text = "myMessage";
                testPeer.ExpectTransfer(messageMatcher: m => Assert.AreEqual(text, (m.BodySection as AmqpValue).Value),
                    settled: false,
                    sendResponseDisposition: true,
                    responseState: new Accepted(),
                    responseSettled: true,
                    stateMatcher: Assert.IsNull,
                    dispositionDelay: 10); // 10ms should be enough
                testPeer.ExpectClose();

                ITextMessage message = await session.CreateTextMessageAsync(text);
                await producer.SendAsync(message);
              
                await connection.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }
       

        [Test, Timeout(20_000)]
        public async Task TestProducerWorkWithAsyncAwait()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await base.EstablishConnectionAsync(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = await session.GetQueueAsync("myQueue");
                IMessageProducer producer = await session.CreateProducerAsync(queue);

                // Create and transfer a new message, explicitly setting the deliveryMode on the
                // message (which applications shouldn't) to NON_PERSISTENT and sending it to check
                // that the producer ignores this value and sends the message as PERSISTENT(/durable)
                testPeer.ExpectTransfer(message => Assert.IsTrue(message.Header.Durable));
                testPeer.ExpectClose();

                ITextMessage textMessage = await session.CreateTextMessageAsync();
                textMessage.NMSDeliveryMode = MsgDeliveryMode.NonPersistent;
                Assert.AreEqual(MsgDeliveryMode.NonPersistent, textMessage.NMSDeliveryMode);

                await producer.SendAsync(textMessage);

                Assert.AreEqual(MsgDeliveryMode.Persistent, textMessage.NMSDeliveryMode);

                await connection.CloseAsync();
                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

    }
}