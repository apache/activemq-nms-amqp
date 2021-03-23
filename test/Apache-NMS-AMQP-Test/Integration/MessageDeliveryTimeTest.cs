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
using Amqp.Framing;
using Amqp.Types;
using Apache.NMS;
using Apache.NMS.AMQP.Util;
using NMS.AMQP.Test.TestAmqp;
using NUnit.Framework;

namespace NMS.AMQP.Test.Integration
{
    [TestFixture]
    public class MessageDeliveryTimeTest : IntegrationTestFixture
    {
        [Test, Timeout(20000)]
        public void TestReceiveMessageWithoutDeliveryTimeSet()
        {
            DoReceiveMessageDeliveryTime(null, null);
        }

        [Test, Timeout(20000)]
        public void TestDeliveryTimeIsDateTime()
        {
            DateTime deliveryTime = DateTimeOffset.FromUnixTimeMilliseconds(CurrentTimeInMillis() + 12345).DateTime.ToUniversalTime();
            DoReceiveMessageDeliveryTime(deliveryTime, deliveryTime);
        }

        [Test, Timeout(20000)]
        public void TestDeliveryTimeIsULong()
        {
            ulong deliveryTime = (ulong) (CurrentTimeInMillis() + 12345);
            DoReceiveMessageDeliveryTime(deliveryTime, DateTimeOffset.FromUnixTimeMilliseconds((long) deliveryTime).DateTime);
        }

        [Test, Timeout(20000)]
        public void TestDeliveryTimeIsLong()
        {
            long deliveryTime = (CurrentTimeInMillis() + 12345);
            DoReceiveMessageDeliveryTime(deliveryTime, DateTimeOffset.FromUnixTimeMilliseconds(deliveryTime).DateTime);
        }

        [Test, Timeout(20000)]
        public void TestDeliveryTimeIsInt()
        {
            int deliveryTime = (int) (CurrentTimeInMillis() + 12345);
            DoReceiveMessageDeliveryTime(deliveryTime, DateTimeOffset.FromUnixTimeMilliseconds(deliveryTime).DateTime);
        }

        [Test, Timeout(20000)]
        public void TestDeliveryTimeIsUInt()
        {
            uint deliveryTime = (uint) (CurrentTimeInMillis() + 12345);
            DoReceiveMessageDeliveryTime(deliveryTime, DateTimeOffset.FromUnixTimeMilliseconds(deliveryTime).DateTime);
        }

        private long CurrentTimeInMillis()
        {
            return new DateTimeOffset(DateTime.UtcNow).ToUnixTimeMilliseconds();
        }

        private void DoReceiveMessageDeliveryTime(object setDeliveryTimeAnnotation, DateTime? expectedDeliveryTime)
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var connection = EstablishConnection(testPeer);
                connection.Start();
                testPeer.ExpectBegin();
                var session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                var queue = session.GetQueue("myQueue");

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
                var messageConsumer = session.CreateConsumer(queue);
                var receivedMessage = messageConsumer.Receive(TimeSpan.FromMilliseconds(3000));
                DateTime receivingTime = DateTime.UtcNow;

                testPeer.WaitForAllMatchersToComplete(3000);

                testPeer.ExpectClose();
                connection.Close();

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
        public void TestDeliveryDelayNotSupportedThrowsException()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = base.EstablishConnection(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(queue);
                Assert.Throws<NotSupportedException>(() => producer.DeliveryDelay = TimeSpan.FromMinutes(17));
            }
        }

        [Test, Timeout(20_000)]
        public void TestDeliveryDelayHasItsReflectionInAmqpAnnotations()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                // Determine current time
                TimeSpan deliveryDelay = TimeSpan.FromMinutes(17);
                long currentUnixEpochTime = new DateTimeOffset(DateTime.UtcNow + deliveryDelay).ToUnixTimeMilliseconds();
                long currentUnixEpochTime2 = new DateTimeOffset(DateTime.UtcNow + deliveryDelay + deliveryDelay).ToUnixTimeMilliseconds();

                IConnection connection = base.EstablishConnection(testPeer,
                    serverCapabilities: new Symbol[] {SymbolUtil.OPEN_CAPABILITY_DELAYED_DELIVERY, SymbolUtil.OPEN_CAPABILITY_SOLE_CONNECTION_FOR_CONTAINER});
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();


                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(queue);
                producer.DeliveryDelay = deliveryDelay;

                // Create and transfer a new message
                testPeer.ExpectTransfer(message =>
                {
                    Assert.GreaterOrEqual((long) message.MessageAnnotations[SymbolUtil.NMS_DELIVERY_TIME], currentUnixEpochTime);
                    Assert.Less((long) message.MessageAnnotations[SymbolUtil.NMS_DELIVERY_TIME], currentUnixEpochTime2);

                    Assert.IsTrue(message.Header.Durable);
                });
                testPeer.ExpectClose();

                ITextMessage textMessage = session.CreateTextMessage();

                producer.Send(textMessage);
                Assert.AreEqual(MsgDeliveryMode.Persistent, textMessage.NMSDeliveryMode);

                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }
    }
}