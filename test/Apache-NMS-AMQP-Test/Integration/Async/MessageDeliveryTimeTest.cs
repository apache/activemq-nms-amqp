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
using Amqp.Types;
using Apache.NMS;
using Apache.NMS.AMQP.Util;
using NMS.AMQP.Test.TestAmqp;
using NUnit.Framework;

namespace NMS.AMQP.Test.Integration.Async
{
    [TestFixture]
    public class MessageDeliveryTimeTestAsync : IntegrationTestFixture
    {
        [Test, Timeout(20000)]
        public async Task TestReceiveMessageWithoutDeliveryTimeSet()
        {
            await DoReceiveMessageDeliveryTime(null, null);
        }

        [Test, Timeout(20000)]
        public async Task TestDeliveryTimeIsDateTime()
        {
            DateTime deliveryTime = DateTimeOffset.FromUnixTimeMilliseconds(CurrentTimeInMillis() + 12345).DateTime.ToUniversalTime();
            await DoReceiveMessageDeliveryTime(deliveryTime, deliveryTime);
        }

        [Test, Timeout(20000)]
        public async Task TestDeliveryTimeIsULong()
        {
            ulong deliveryTime = (ulong) (CurrentTimeInMillis() + 12345);
            await DoReceiveMessageDeliveryTime(deliveryTime, DateTimeOffset.FromUnixTimeMilliseconds((long) deliveryTime).DateTime);
        }

        [Test, Timeout(20000)]
        public async Task TestDeliveryTimeIsLong()
        {
            long deliveryTime = (CurrentTimeInMillis() + 12345);
            await DoReceiveMessageDeliveryTime(deliveryTime, DateTimeOffset.FromUnixTimeMilliseconds(deliveryTime).DateTime);
        }

        [Test, Timeout(20000)]
        public async Task TestDeliveryTimeIsInt()
        {
            int deliveryTime = (int) (CurrentTimeInMillis() + 12345);
            await DoReceiveMessageDeliveryTime(deliveryTime, DateTimeOffset.FromUnixTimeMilliseconds(deliveryTime).DateTime);
        }

        [Test, Timeout(20000)]
        public async Task TestDeliveryTimeIsUInt()
        {
            uint deliveryTime = (uint) (CurrentTimeInMillis() + 12345);
            await DoReceiveMessageDeliveryTime(deliveryTime, DateTimeOffset.FromUnixTimeMilliseconds(deliveryTime).DateTime);
        }

        private long CurrentTimeInMillis()
        {
            return new DateTimeOffset(DateTime.UtcNow).ToUnixTimeMilliseconds();
        }

        private async Task DoReceiveMessageDeliveryTime(object setDeliveryTimeAnnotation, DateTime? expectedDeliveryTime)
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var connection = await EstablishConnectionAsync(testPeer);
                await connection.StartAsync();
                testPeer.ExpectBegin();
                var session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                var queue = await session.GetQueueAsync("myQueue");

                var message = CreateMessageWithNullContent();
                if (setDeliveryTimeAnnotation != null)
                {
                    message.MessageAnnotations = message.MessageAnnotations ?? new MessageAnnotations();
                    message.MessageAnnotations[SymbolUtil.NMS_DELIVERY_TIME] = setDeliveryTimeAnnotation;
                }

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message);
                testPeer.ExpectDisposition(true, (deliveryState) => { });

                DateTime startingTimeFrom = DateTime.UtcNow;
                var messageConsumer = await session.CreateConsumerAsync(queue);
                var receivedMessage = await messageConsumer.ReceiveAsync(TimeSpan.FromMilliseconds(3000));
                DateTime receivingTime = DateTime.UtcNow;

                testPeer.WaitForAllMatchersToComplete(3000);

                testPeer.ExpectClose();
                await connection.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(3000);

                Assert.IsNotNull(receivedMessage);
                if (expectedDeliveryTime != null)
                {
                    Assert.AreEqual(receivedMessage.NMSDeliveryTime, expectedDeliveryTime.Value);
                }
                else
                {
                    Assert.LessOrEqual(receivedMessage.NMSDeliveryTime, receivingTime);
                    Assert.GreaterOrEqual(receivedMessage.NMSDeliveryTime, startingTimeFrom);
                }
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestDeliveryDelayNotSupportedThrowsException()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await base.EstablishConnectionAsync(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = await session.GetQueueAsync("myQueue");
                IMessageProducer producer = await session.CreateProducerAsync(queue);
                Assert.Throws<NotSupportedException>(() => producer.DeliveryDelay = TimeSpan.FromMinutes(17));
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestDeliveryDelayHasItsReflectionInAmqpAnnotations()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                // Determine current time
                TimeSpan deliveryDelay = TimeSpan.FromMinutes(17);
                long currentUnixEpochTime = new DateTimeOffset(DateTime.UtcNow + deliveryDelay).ToUnixTimeMilliseconds();
                long currentUnixEpochTime2 = new DateTimeOffset(DateTime.UtcNow + deliveryDelay + deliveryDelay).ToUnixTimeMilliseconds();

                IConnection connection = await base.EstablishConnectionAsync(testPeer,
                    serverCapabilities: new Symbol[] {SymbolUtil.OPEN_CAPABILITY_DELAYED_DELIVERY, SymbolUtil.OPEN_CAPABILITY_SOLE_CONNECTION_FOR_CONTAINER});
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();


                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = await session.GetQueueAsync("myQueue");
                IMessageProducer producer = await session.CreateProducerAsync(queue);
                producer.DeliveryDelay = deliveryDelay;

                // Create and transfer a new message
                testPeer.ExpectTransfer(message =>
                {
                    Assert.GreaterOrEqual((long) message.MessageAnnotations[SymbolUtil.NMS_DELIVERY_TIME], currentUnixEpochTime);
                    Assert.Less((long) message.MessageAnnotations[SymbolUtil.NMS_DELIVERY_TIME], currentUnixEpochTime2);

                    Assert.IsTrue(message.Header.Durable);
                });
                testPeer.ExpectClose();

                ITextMessage textMessage = await session.CreateTextMessageAsync();

                await producer.SendAsync(textMessage);
                Assert.AreEqual(MsgDeliveryMode.Persistent, textMessage.NMSDeliveryMode);

                await connection.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }
    }
}