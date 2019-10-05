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
using Amqp.Framing;
using Apache.NMS;
using NMS.AMQP.Test.TestAmqp;
using NUnit.Framework;

namespace NMS.AMQP.Test.Integration
{
    [TestFixture]
    public class MessageExpirationIntegrationTest : IntegrationTestFixture
    {
        [Test, Timeout(20_000)]
        public void TestIncomingExpiredMessageGetsFiltered()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                connection.Start();

                testPeer.ExpectBegin();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");

                // Expected the consumer to attach and send credit, then send it an
                // already-expired message followed by a live message.
                testPeer.ExpectReceiverAttach();
                string expiredMsgContent = "already-expired";
                Amqp.Message message = CreateExpiredMessage(expiredMsgContent);
                testPeer.ExpectLinkFlowRespondWithTransfer(message: message);

                string liveMsgContent = "valid";
                testPeer.SendTransferToLastOpenedLinkOnLastOpenedSession(message: new Amqp.Message() { BodySection = new AmqpValue() { Value = liveMsgContent } }, nextIncomingId: 2);

                IMessageConsumer consumer = session.CreateConsumer(queue);

                // Call receive, expect the first message to be filtered due to expiry,
                // and the second message to be given to the test app and accepted.
                Action<DeliveryState> modifiedMatcher = state =>
                {
                    var modified = state as Modified;
                    Assert.IsNotNull(modified);
                    Assert.IsTrue(modified.DeliveryFailed);
                    Assert.IsTrue(modified.UndeliverableHere);
                };
                testPeer.ExpectDisposition(settled: true, stateMatcher: modifiedMatcher, firstDeliveryId: 1, lastDeliveryId: 1);
                testPeer.ExpectDisposition(settled: true, stateMatcher: Assert.IsInstanceOf<Accepted>, firstDeliveryId: 2, lastDeliveryId: 2);

                IMessage m = consumer.Receive(TimeSpan.FromMilliseconds(3000));
                Assert.NotNull(m, "Message should have been received");
                Assert.IsInstanceOf<ITextMessage>(m);
                Assert.AreEqual(liveMsgContent, (m as ITextMessage).Text, "Unexpected message content");

                // Verify the other message is not there. Will drain to be sure there are no messages.
                Assert.IsNull(consumer.Receive(TimeSpan.FromMilliseconds(10)), "Message should not have been received");

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(3000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestIncomingExpiredMessageGetsConsumedWhenFilterDisabled()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer, "?nms.localMessageExpiry=false");
                connection.Start();

                testPeer.ExpectBegin();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");

                // Expected the consumer to attach and send credit, then send it an
                // already-expired message followed by a live message.
                testPeer.ExpectReceiverAttach();

                string expiredMsgContent = "already-expired";
                Amqp.Message message = CreateExpiredMessage(expiredMsgContent);
                testPeer.ExpectLinkFlowRespondWithTransfer(message: message);

                string liveMsgContent = "valid";
                testPeer.SendTransferToLastOpenedLinkOnLastOpenedSession(message: new Amqp.Message() { BodySection = new AmqpValue() { Value = liveMsgContent } }, nextIncomingId: 2);

                IMessageConsumer consumer = session.CreateConsumer(queue);

                // Call receive, expect the expired message as we disabled local expiry.
                testPeer.ExpectDisposition(settled: true, stateMatcher: Assert.IsInstanceOf<Accepted>, firstDeliveryId: 1, lastDeliveryId: 1);

                IMessage m = consumer.Receive(TimeSpan.FromMilliseconds(3000));
                Assert.NotNull(m, "Message should have been received");
                Assert.IsInstanceOf<ITextMessage>(m);
                Assert.AreEqual(expiredMsgContent, ((ITextMessage) m).Text, "Unexpected message content");

                // Verify the other message is there
                testPeer.ExpectDisposition(settled: true, stateMatcher: Assert.IsInstanceOf<Accepted>, firstDeliveryId: 2, lastDeliveryId: 2);

                m = consumer.Receive(TimeSpan.FromMilliseconds(3000));
                Assert.NotNull(m, "Message should have been received");
                Assert.IsInstanceOf<ITextMessage>(m);
                Assert.AreEqual(liveMsgContent, ((ITextMessage) m).Text, "Unexpected message content");

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(3000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestIncomingExpiredMessageGetsFilteredAsync()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                connection.Start();

                testPeer.ExpectBegin();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");

                // Expected the consumer to attach and send credit, then send it an
                // already-expired message followed by a live message.
                testPeer.ExpectReceiverAttach();

                string expiredMsgContent = "already-expired";
                Amqp.Message message = CreateExpiredMessage(expiredMsgContent);
                testPeer.ExpectLinkFlowRespondWithTransfer(message: message);

                string liveMsgContent = "valid";
                testPeer.SendTransferToLastOpenedLinkOnLastOpenedSession(message: new Amqp.Message() { BodySection = new AmqpValue() { Value = liveMsgContent } }, nextIncomingId: 2);

                IMessageConsumer consumer = session.CreateConsumer(queue);

                // Add message listener, expect the first message to be filtered due to expiry,
                // and the second message to be given to the test app and accepted.
                Action<DeliveryState> modifiedMatcher = state =>
                {
                    var modified = state as Modified;
                    Assert.IsNotNull(modified);
                    Assert.IsTrue(modified.DeliveryFailed);
                    Assert.IsTrue(modified.UndeliverableHere);
                };
                testPeer.ExpectDisposition(settled: true, stateMatcher: modifiedMatcher, firstDeliveryId: 1, lastDeliveryId: 1);
                testPeer.ExpectDisposition(settled: true, stateMatcher: Assert.IsInstanceOf<Accepted>, firstDeliveryId: 2, lastDeliveryId: 2);


                ManualResetEvent success = new ManualResetEvent(false);
                ManualResetEvent listenerFailure = new ManualResetEvent(false);

                consumer.Listener += m =>
                {
                    if (liveMsgContent.Equals(((ITextMessage) m).Text))
                        success.Set();
                    else
                        listenerFailure.Set();
                };

                Assert.True(success.WaitOne(TimeSpan.FromSeconds(5)), "didn't get expected message");
                Assert.False(listenerFailure.WaitOne(TimeSpan.FromMilliseconds(100)), "Received message when message should not have been received");

                testPeer.WaitForAllMatchersToComplete(3000);

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(3000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestIncomingExpiredMessageGetsConsumedWhenFilterDisabledAsync()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer, "?nms.localMessageExpiry=false");
                connection.Start();

                testPeer.ExpectBegin();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");

                // Expected the consumer to attach and send credit, then send it an
                // already-expired message followed by a live message.
                testPeer.ExpectReceiverAttach();

                string expiredMsgContent = "already-expired";
                Amqp.Message message = CreateExpiredMessage(expiredMsgContent);
                testPeer.ExpectLinkFlowRespondWithTransfer(message: message);

                string liveMsgContent = "valid";
                testPeer.SendTransferToLastOpenedLinkOnLastOpenedSession(message: new Amqp.Message() { BodySection = new AmqpValue() { Value = liveMsgContent } }, nextIncomingId: 2);

                IMessageConsumer consumer = session.CreateConsumer(queue);
                
                // Add message listener, expect both messages as the filter is disabled
                testPeer.ExpectDisposition(settled: true, stateMatcher: Assert.IsInstanceOf<Accepted>, firstDeliveryId:1, lastDeliveryId:1);
                testPeer.ExpectDisposition(settled: true, stateMatcher: Assert.IsInstanceOf<Accepted>, firstDeliveryId:2, lastDeliveryId:2);

                CountdownEvent success = new CountdownEvent(2);

                consumer.Listener += m =>
                {
                    if (expiredMsgContent.Equals(((ITextMessage) m).Text) || liveMsgContent.Equals(((ITextMessage) m).Text))
                        success.Signal();
                };
                
                Assert.IsTrue(success.Wait(TimeSpan.FromSeconds(5)), "Didn't get expected messages");
                
                testPeer.WaitForAllMatchersToComplete(3000);
                
                testPeer.ExpectClose();
                connection.Close();
                
                testPeer.WaitForAllMatchersToComplete(3000);
            }
        }

        private static Amqp.Message CreateExpiredMessage(string value)
        {
            return new Amqp.Message
            {
                BodySection = new AmqpValue() { Value = value },
                Properties = new Properties { AbsoluteExpiryTime = DateTime.UtcNow - TimeSpan.FromMilliseconds(100) }
            };
        }
    }
}