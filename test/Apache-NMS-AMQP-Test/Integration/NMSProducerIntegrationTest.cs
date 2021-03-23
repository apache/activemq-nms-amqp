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
    // Adapted from ProducerIntegrationTest to use NMSContext
    [TestFixture]
    public class NMSProducerIntegrationTest : IntegrationTestFixture
    {
        private const long TICKS_PER_MILLISECOND = 10000;

        [Test, Timeout(20_000)]
        public void TestCloseSender()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = base.EstablishNMSContext(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                IQueue queue = context.GetQueue("myQueue");
                var producer = context.CreateProducer();

                testPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                producer.Close();
                context.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestSentTextMessageCanBeModified()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = base.EstablishNMSContext(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                IQueue queue = context.GetQueue("myQueue");
                var producer = context.CreateProducer();

                // Create and transfer a new message
                String text = "myMessage";
                testPeer.ExpectTransfer(x => Assert.AreEqual(text, (x.BodySection as AmqpValue).Value));
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                ITextMessage message = context.CreateTextMessage(text);
                producer.Send(queue, message);

                Assert.AreEqual(text, message.Text);
                message.Text = text + text;
                Assert.AreEqual(text + text, message.Text);

                context.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestDefaultDeliveryModeProducesDurableMessages()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = base.EstablishNMSContext(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                IQueue queue = context.GetQueue("myQueue");
                var producer = context.CreateProducer();

                // Create and transfer a new message
                testPeer.ExpectTransfer(message => Assert.IsTrue(message.Header.Durable));
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                ITextMessage textMessage = context.CreateTextMessage();

                producer.Send(queue, textMessage);
                Assert.AreEqual(MsgDeliveryMode.Persistent, textMessage.NMSDeliveryMode);

                context.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestProducerOverridesMessageDeliveryMode()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = base.EstablishNMSContext(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                IQueue queue = context.GetQueue("myQueue");
                var producer = context.CreateProducer();

                // Create and transfer a new message, explicitly setting the deliveryMode on the
                // message (which applications shouldn't) to NON_PERSISTENT and sending it to check
                // that the producer ignores this value and sends the message as PERSISTENT(/durable)
                testPeer.ExpectTransfer(message => Assert.IsTrue(message.Header.Durable));
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                ITextMessage textMessage = context.CreateTextMessage();
                textMessage.NMSDeliveryMode = MsgDeliveryMode.NonPersistent;
                Assert.AreEqual(MsgDeliveryMode.NonPersistent, textMessage.NMSDeliveryMode);

                producer.Send(queue, textMessage);

                Assert.AreEqual(MsgDeliveryMode.Persistent, textMessage.NMSDeliveryMode);

                context.Close();
                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestSendingMessageNonPersistentProducerSetDurableFalse()
        {
            DoSendingMessageNonPersistentTestImpl(true);
        }

        [Test, Timeout(20_000)]
        public void TestSendingMessageNonPersistentProducerOmitsHeader()
        {
            DoSendingMessageNonPersistentTestImpl(false);
        }

        private void DoSendingMessageNonPersistentTestImpl(bool setPriority)
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                //Add capability to indicate support for ANONYMOUS-RELAY
                Symbol[] serverCapabilities = {SymbolUtil.OPEN_CAPABILITY_ANONYMOUS_RELAY};
                var context = EstablishNMSContext(testPeer, serverCapabilities: serverCapabilities);
                testPeer.ExpectBegin();

                string queueName = "myQueue";
                Action<object> targetMatcher = t =>
                {
                    var target = t as Target;
                    Assert.IsNotNull(target);
                };


                testPeer.ExpectSenderAttach(targetMatcher: targetMatcher, sourceMatcher: Assert.NotNull, senderSettled: false);

                IQueue queue = context.GetQueue(queueName);
                INMSProducer producer = context.CreateProducer();

                byte priority = 5;
                String text = "myMessage";
                testPeer.ExpectTransfer(messageMatcher: message =>
                    {
                        if (setPriority)
                        {
                            Assert.IsFalse(message.Header.Durable);
                            Assert.AreEqual(priority, message.Header.Priority);
                        }

                        Assert.AreEqual(text, (message.BodySection as AmqpValue).Value);
                    }, stateMatcher: Assert.IsNull,
                    settled: false,
                    sendResponseDisposition: true,
                    responseState: new Accepted(),
                    responseSettled: true);

                ITextMessage textMessage = context.CreateTextMessage(text);

                producer.DeliveryMode = MsgDeliveryMode.NonPersistent;
                if (setPriority)
                    producer.Priority = (MsgPriority) priority;

                producer.Send(queue, textMessage);

                Assert.AreEqual(MsgDeliveryMode.NonPersistent, textMessage.NMSDeliveryMode, "Should have NonPersistent delivery mode set");

                testPeer.WaitForAllMatchersToComplete(1000);

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                context.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestSendingMessageSetsNMSDestination()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = EstablishNMSContext(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                IQueue destination = context.GetQueue("myQueue");
                var producer = context.CreateProducer();

                string text = "myMessage";
                ITextMessage message = context.CreateTextMessage(text);

                testPeer.ExpectTransfer(m => Assert.AreEqual(text, (m.BodySection as AmqpValue).Value));
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                Assert.IsNull(message.NMSDestination, "Should not yet have a NMSDestination");

                producer.Send(destination, message);

                Assert.AreEqual(destination, message.NMSDestination, "Should have had NMSDestination set");

                context.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestSendingMessageSetsNMSTimestamp()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = EstablishNMSContext(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                IQueue destination = context.GetQueue("myQueue");
                var producer = context.CreateProducer();

                // Create matcher to expect the absolute-expiry-time field of the properties section to
                // be set to a value greater than 'now'+ttl, within a delta.

                DateTime creationLower = DateTime.UtcNow;
                DateTime creationUpper = creationLower + TimeSpan.FromMilliseconds(3000);

                var text = "myMessage";
                testPeer.ExpectTransfer(m =>
                {
                    Assert.IsTrue(m.Header.Durable);
                    Assert.That(m.Properties.CreationTime.Ticks, Is.GreaterThanOrEqualTo(creationLower.Ticks).Within(TICKS_PER_MILLISECOND));
                    Assert.That(m.Properties.CreationTime.Ticks, Is.LessThanOrEqualTo(creationUpper.Ticks).Within(TICKS_PER_MILLISECOND));
                    Assert.AreEqual(text, (m.BodySection as AmqpValue).Value);
                });

                ITextMessage message = context.CreateTextMessage(text);
                producer.Send(destination, message);

                testPeer.WaitForAllMatchersToComplete(1000);

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                context.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestSendingMessageSetsNMSExpirationRelatedAbsoluteExpiryAndTtlFields()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = EstablishNMSContext(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                IQueue destination = context.GetQueue("myQueue");
                var producer = context.CreateProducer();

                uint ttl = 100_000;
                DateTime currentTime = DateTime.UtcNow;
                DateTime expirationLower = currentTime + TimeSpan.FromMilliseconds(ttl);
                DateTime expirationUpper = currentTime + TimeSpan.FromMilliseconds(ttl) + TimeSpan.FromMilliseconds(5000);

                // Create matcher to expect the absolute-expiry-time field of the properties section to
                // be set to a value greater than 'now'+ttl, within a delta.
                string text = "myMessage";
                testPeer.ExpectTransfer(m =>
                {
                    Assert.IsTrue(m.Header.Durable);
                    Assert.AreEqual(ttl, m.Header.Ttl);
                    Assert.That(m.Properties.AbsoluteExpiryTime.Ticks, Is.GreaterThanOrEqualTo(expirationLower.Ticks).Within(TICKS_PER_MILLISECOND));
                    Assert.That(m.Properties.AbsoluteExpiryTime.Ticks, Is.LessThanOrEqualTo(expirationUpper.Ticks).Within(TICKS_PER_MILLISECOND));
                    Assert.AreEqual(text, (m.BodySection as AmqpValue).Value);
                });

                ITextMessage message = context.CreateTextMessage(text);
                producer.TimeToLive = TimeSpan.FromMilliseconds(ttl);
                producer.Priority = NMSConstants.defaultPriority;
                producer.DeliveryMode = NMSConstants.defaultDeliveryMode;
                producer.Send(destination, message);

                testPeer.WaitForAllMatchersToComplete(1000);

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                context.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestMessagesAreProducedWithProperDefaultPriorityWhenNoPrioritySpecified()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = EstablishNMSContext(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                IQueue destination = context.GetQueue("myQueue");
                var producer = context.CreateProducer();

                byte priority = 4;

                testPeer.ExpectTransfer(m => Assert.AreEqual(priority, m.Header.Priority));
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                ITextMessage message = context.CreateTextMessage();
                Assert.AreEqual(MsgPriority.BelowNormal, message.NMSPriority);

                producer.Send(destination, message);

                Assert.AreEqual((MsgPriority) priority, message.NMSPriority);

                context.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestNonDefaultPriorityProducesMessagesWithPriorityFieldAndSetsNMSPriority()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = EstablishNMSContext(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                IQueue destination = context.GetQueue("myQueue");
                var producer = context.CreateProducer();

                byte priority = 9;

                testPeer.ExpectTransfer(m => Assert.AreEqual(priority, m.Header.Priority));
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                ITextMessage message = context.CreateTextMessage();
                Assert.AreEqual(MsgPriority.BelowNormal, message.NMSPriority);

                producer.DeliveryMode = MsgDeliveryMode.Persistent;
                producer.Priority = (MsgPriority) priority;
                producer.TimeToLive = NMSConstants.defaultTimeToLive;
                producer.Send(destination, message);

                Assert.AreEqual((MsgPriority) priority, message.NMSPriority);

                context.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestSendingMessageSetsNMSMessageId()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = EstablishNMSContext(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                IQueue destination = context.GetQueue("myQueue");
                var producer = context.CreateProducer();

                string text = "myMessage";
                string actualMessageId = null;
                testPeer.ExpectTransfer(m =>
                {
                    Assert.IsTrue(m.Header.Durable);
                    Assert.IsNotEmpty(m.Properties.MessageId);
                    actualMessageId = m.Properties.MessageId;
                });
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                ITextMessage message = context.CreateTextMessage(text);
                Assert.IsNull(message.NMSMessageId, "NMSMessageId should not yet be set");

                producer.Send(destination, message);

                Assert.IsNotNull(message.NMSMessageId);
                Assert.IsNotEmpty(message.NMSMessageId, "NMSMessageId should be set");
                Assert.IsTrue(message.NMSMessageId.StartsWith("ID:"), "MMS 'ID:' prefix not found");

                context.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
                // Get the value that was actually transmitted/received, verify it is a string, compare to what we have locally
                Assert.AreEqual(message.NMSMessageId, actualMessageId, "Expected NMSMessageId value to be present in AMQP message");
            }
        }

        [Test, Timeout(20_000)]
        public void TestSendingMessageWithDisableMessageIdHint()
        {
            DoSendingMessageWithDisableMessageIdHintTestImpl(false);
        }

        [Test, Timeout(20_000)]
        public void TestSendingMessageWithDisableMessageIdHintAndExistingMessageId()
        {
            DoSendingMessageWithDisableMessageIdHintTestImpl(true);
        }

        private void DoSendingMessageWithDisableMessageIdHintTestImpl(bool existingId)
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = EstablishNMSContext(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                IQueue destination = context.GetQueue("myQueue");
                var producer = context.CreateProducer();

                string text = "myMessage";
                testPeer.ExpectTransfer(m =>
                {
                    Assert.IsTrue(m.Header.Durable);
                    Assert.IsNull(m.Properties.MessageId); // Check there is no message-id value;
                    Assert.AreEqual(text, (m.BodySection as AmqpValue).Value);
                });
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                ITextMessage message = context.CreateTextMessage(text);

                Assert.IsNull(message.NMSMessageId, "NMSMessageId should not yet be set");

                if (existingId)
                {
                    string existingMessageId = "ID:this-should-be-overwritten-in-send";
                    message.NMSMessageId = existingMessageId;
                    Assert.AreEqual(existingMessageId, message.NMSMessageId, "NMSMessageId should now be se");
                }

                producer.DisableMessageID = true;

                producer.Send(destination, message);

                Assert.IsNull(message.NMSMessageId, "NMSMessageID should be null");

                context.Close();

                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }

        // TODO No connection listener in nms context
        // [Test, Timeout(20_000)]
        // public void TestRemotelyCloseProducer()
        // {
        //     string breadCrumb = "ErrorMessageBreadCrumb";
        //
        //     ManualResetEvent producerClosed = new ManualResetEvent(false);
        //     Mock<INmsConnectionListener> mockConnectionListener = new Mock<INmsConnectionListener>();
        //     mockConnectionListener
        //         .Setup(listener => listener.OnProducerClosed(It.IsAny<NmsMessageProducer>(), It.IsAny<Exception>()))
        //         .Callback(() => { producerClosed.Set(); });
        //
        //     using (TestAmqpPeer testPeer = new TestAmqpPeer())
        //     {
        //         NmsContext context = (NmsContext) EstablishNMSContext(testPeer);
        //         context.AddConnectionListener(mockConnectionListener.Object);
        //
        //         testPeer.ExpectBegin();
        //         ISession session = context.CreateSession(AcknowledgementMode.AutoAcknowledge);
        //
        //         // Create a producer, then remotely end it afterwards.
        //         testPeer.ExpectSenderAttach();
        //         testPeer.RemotelyDetachLastOpenedLinkOnLastOpenedSession(expectDetachResponse: true, closed: true, errorType: AmqpError.RESOURCE_DELETED, breadCrumb, delayBeforeSend: 10);
        //
        //         IQueue destination = session.GetQueue("myQueue");
        //         IMessageProducer producer = session.CreateProducer(destination);
        //
        //         // Verify the producer gets marked closed
        //         testPeer.WaitForAllMatchersToComplete(1000);
        //
        //         Assert.True(producerClosed.WaitOne(TimeSpan.FromMilliseconds(1000)), "Producer closed callback didn't trigger");
        //         Assert.That(() => producer.DisableMessageID, Throws.Exception.InstanceOf<IllegalStateException>(), "Producer never closed");
        //
        //         // Try closing it explicitly, should effectively no-op in client.
        //         // The test peer will throw during close if it sends anything.
        //         producer.Close();
        //     }
        // }

        [Test, Timeout(20_000)]
        public void TestSendWhenLinkCreditIsZeroAndTimeout()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = EstablishNMSContext(testPeer, optionsString: "nms.sendTimeout=500");
                testPeer.ExpectBegin();

                IQueue queue = context.GetQueue("myQueue");

                ITextMessage message = context.CreateTextMessage("text");

                // Expect the producer to attach. Don't send any credit so that the client will
                // block on a send and we can test our timeouts.
                testPeer.ExpectSenderAttachWithoutGrantingCredit();
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                var producer = context.CreateProducer();

                Assert.Catch<Exception>(() => producer.Send(queue, message), "Send should time out.");

                context.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestSendTimesOutWhenNoDispositionArrives()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = EstablishNMSContext(testPeer, optionsString: "nms.sendTimeout=500");
                testPeer.ExpectBegin();

                IQueue queue = context.GetQueue("myQueue");

                ITextMessage message = context.CreateTextMessage("text");

                // Expect the producer to attach and grant it some credit, it should send
                // a transfer which we will not send any response for which should cause the
                // send operation to time out.
                testPeer.ExpectSenderAttach();
                testPeer.ExpectTransferButDoNotRespond(messageMatcher: Assert.NotNull);
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                var producer = context.CreateProducer();

                Assert.Catch<Exception>(() => producer.Send(queue, message), "Send should time out.");

                context.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestSendWorksWhenConnectionNotStarted()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = EstablishNMSContext(testPeer);

                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                IQueue destination = context.GetQueue("myQueue");
                var producer = context.CreateProducer();

                testPeer.ExpectTransfer(Assert.IsNotNull);

                producer.Send(destination, context.CreateMessage());

                testPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);
                producer.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestSendWorksAfterConnectionStopped()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = EstablishNMSContext(testPeer);
                context.Start();

                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                IQueue destination = context.GetQueue("myQueue");
                var producer = context.CreateProducer();

                testPeer.ExpectTransfer(Assert.IsNotNull);

                context.Stop();

                producer.Send(destination, context.CreateMessage());

                testPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                producer.Close();
                context.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestSendingMessagePersistentSetsBatchableFalse()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = EstablishNMSContext(testPeer);
                context.Start();

                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                IQueue destination = context.GetQueue("myQueue");
                var producer = context.CreateProducer();
                testPeer.ExpectTransfer(messageMatcher: Assert.IsNotNull,
                    stateMatcher: Assert.IsNull,
                    settled: false,
                    sendResponseDisposition: true,
                    responseState: new Accepted(),
                    responseSettled: true,
                    batchable: false);

                IMessage message = context.CreateMessage();
                producer.DeliveryMode = MsgDeliveryMode.Persistent;
                producer.Priority = MsgPriority.Normal;
                producer.TimeToLive = NMSConstants.defaultTimeToLive;
                producer.Send(destination, message);

                testPeer.WaitForAllMatchersToComplete(1000);

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                context.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestSendingMessageNonPersistentSetsBatchableFalse()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = EstablishNMSContext(testPeer);
                context.Start();

                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                IQueue destination = context.GetQueue("myQueue");
                var producer = context.CreateProducer();
                testPeer.ExpectTransfer(messageMatcher: Assert.IsNotNull,
                    stateMatcher: Assert.IsNull,
                    settled: false,
                    sendResponseDisposition: true,
                    responseState: new Accepted(),
                    responseSettled: true,
                    batchable: false);

                IMessage message = context.CreateMessage();
                producer.DeliveryMode = MsgDeliveryMode.NonPersistent;
                producer.Priority = MsgPriority.Normal;
                producer.TimeToLive = NMSConstants.defaultTimeToLive;
                producer.Send(destination, message);

                testPeer.WaitForAllMatchersToComplete(1000);

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                context.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }
    }
}