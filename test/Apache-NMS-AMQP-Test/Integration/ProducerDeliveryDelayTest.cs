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
using Amqp.Types;
using Apache.NMS;
using Apache.NMS.AMQP.Util;
using NMS.AMQP.Test.TestAmqp;
using NUnit.Framework;

namespace NMS.AMQP.Test.Integration
{
    [TestFixture]
    public class ProducerDeliveryDelayTest : IntegrationTestFixture
    {
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