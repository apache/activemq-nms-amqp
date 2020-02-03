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
using Amqp.Types;
using Apache.NMS;
using Apache.NMS.AMQP;
using Apache.NMS.AMQP.Util;
using Moq;
using NMS.AMQP.Test.TestAmqp;
using NMS.AMQP.Test.TestAmqp.BasicTypes;
using NUnit.Framework;

namespace NMS.AMQP.Test.Integration
{
    [TestFixture]
    public class ProducerIntegrationTest : IntegrationTestFixture
    {
        private const long TICKS_PER_MILLISECOND = 10000;

        [Test, Timeout(20_000)]
        public void TestCloseSender()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = base.EstablishConnection(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer();

                testPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);
                testPeer.ExpectClose();

                producer.Close();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestSentTextMessageCanBeModified()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = base.EstablishConnection(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(queue);

                // Create and transfer a new message
                String text = "myMessage";
                testPeer.ExpectTransfer(x => Assert.AreEqual(text, (x.BodySection as AmqpValue).Value));
                testPeer.ExpectClose();

                ITextMessage message = session.CreateTextMessage(text);
                producer.Send(message);

                Assert.AreEqual(text, message.Text);
                message.Text = text + text;
                Assert.AreEqual(text + text, message.Text);

                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestDefaultDeliveryModeProducesDurableMessages()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = base.EstablishConnection(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(queue);

                // Create and transfer a new message
                testPeer.ExpectTransfer(message => Assert.IsTrue(message.Header.Durable));
                testPeer.ExpectClose();

                ITextMessage textMessage = session.CreateTextMessage();

                producer.Send(textMessage);
                Assert.AreEqual(MsgDeliveryMode.Persistent, textMessage.NMSDeliveryMode);

                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestProducerOverridesMessageDeliveryMode()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = base.EstablishConnection(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(queue);

                // Create and transfer a new message, explicitly setting the deliveryMode on the
                // message (which applications shouldn't) to NON_PERSISTENT and sending it to check
                // that the producer ignores this value and sends the message as PERSISTENT(/durable)
                testPeer.ExpectTransfer(message => Assert.IsTrue(message.Header.Durable));
                testPeer.ExpectClose();

                ITextMessage textMessage = session.CreateTextMessage();
                textMessage.NMSDeliveryMode = MsgDeliveryMode.NonPersistent;
                Assert.AreEqual(MsgDeliveryMode.NonPersistent, textMessage.NMSDeliveryMode);

                producer.Send(textMessage);

                Assert.AreEqual(MsgDeliveryMode.Persistent, textMessage.NMSDeliveryMode);

                connection.Close();
                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestSendingMessageNonPersistentProducerSetDurableFalse()
        {
            DoSendingMessageNonPersistentTestImpl(false, true, true);
        }

        [Test, Timeout(20_000)]
        public void TestSendingMessageNonPersistentProducerSetDurableFalseAnonymousProducer()
        {
            DoSendingMessageNonPersistentTestImpl(true, true, true);
        }

        [Test, Timeout(20_000)]
        public void TestSendingMessageNonPersistentSendSetDurableFalse()
        {
            DoSendingMessageNonPersistentTestImpl(false, true, false);
        }

        [Test, Timeout(20_000)]
        public void TestSendingMessageNonPersistentSendSetDurableFalseAnonymousProducer()
        {
            DoSendingMessageNonPersistentTestImpl(true, true, false);
        }

        [Test, Timeout(20_000)]
        public void TestSendingMessageNonPersistentProducerOmitsHeader()
        {
            DoSendingMessageNonPersistentTestImpl(false, false, true);
        }

        [Test, Timeout(20_000)]
        public void TestSendingMessageNonPersistentProducerOmitsHeaderAnonymousProducer()
        {
            DoSendingMessageNonPersistentTestImpl(true, false, true);
        }

        [Test, Timeout(20_000)]
        public void TestSendingMessageNonPersistentSendOmitsHeader()
        {
            DoSendingMessageNonPersistentTestImpl(false, false, false);
        }

        [Test, Timeout(20_000)]
        public void TestSendingMessageNonPersistentSendOmitsHeaderAnonymousProducer()
        {
            DoSendingMessageNonPersistentTestImpl(true, false, false);
        }

        private void DoSendingMessageNonPersistentTestImpl(bool anonymousProducer, bool setPriority, bool setOnProducer)
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                //Add capability to indicate support for ANONYMOUS-RELAY
                Symbol[] serverCapabilities = { SymbolUtil.OPEN_CAPABILITY_ANONYMOUS_RELAY };
                IConnection connection = EstablishConnection(testPeer, serverCapabilities: serverCapabilities);
                testPeer.ExpectBegin();

                string queueName = "myQueue";
                Action<object> targetMatcher = t =>
                {
                    var target = t as Target;
                    Assert.IsNotNull(target);
                    if (anonymousProducer)
                        Assert.IsNull(target.Address);
                    else
                        Assert.AreEqual(queueName, target.Address);
                };
                

                testPeer.ExpectSenderAttach(targetMatcher: targetMatcher, sourceMatcher: Assert.NotNull, senderSettled: false);

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue(queueName);
                IMessageProducer producer = null;
                if (anonymousProducer)
                    producer = session.CreateProducer();
                else
                    producer = session.CreateProducer(queue);

                byte priority = 5;
                String text = "myMessage";
                testPeer.ExpectTransfer(messageMatcher: message =>
                    {
                        if (setPriority)
                        {
                            Assert.IsFalse(message.Header.Durable);
                            Assert.AreEqual(5, message.Header.Priority);
                        }

                        Assert.AreEqual(text, (message.BodySection as AmqpValue).Value);
                    }, stateMatcher: Assert.IsNull,
                    settled: false,
                    sendResponseDisposition: true,
                    responseState: new Accepted(),
                    responseSettled: true);

                ITextMessage textMessage = session.CreateTextMessage(text);

                if (setOnProducer)
                {
                    producer.DeliveryMode = MsgDeliveryMode.NonPersistent;
                    if (setPriority)
                        producer.Priority = (MsgPriority) 5;

                    if (anonymousProducer)
                        producer.Send(queue, textMessage);
                    else
                        producer.Send(textMessage);
                }
                else
                {
                    if (anonymousProducer)
                    {
                        producer.Send(destination: queue,
                            message: textMessage,
                            deliveryMode: MsgDeliveryMode.NonPersistent,
                            priority: setPriority ? (MsgPriority) priority : NMSConstants.defaultPriority,
                            timeToLive: NMSConstants.defaultTimeToLive);
                    }
                    else
                    {
                        producer.Send(message: textMessage,
                            deliveryMode: MsgDeliveryMode.NonPersistent,
                            priority: setPriority ? (MsgPriority) priority : NMSConstants.defaultPriority,
                            timeToLive: NMSConstants.defaultTimeToLive);
                    }
                }

                Assert.AreEqual(MsgDeliveryMode.NonPersistent, textMessage.NMSDeliveryMode, "Should have NonPersistent delivery mode set");

                testPeer.WaitForAllMatchersToComplete(1000);

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestSendingMessageSetsNMSDestination()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(destination);

                string text = "myMessage";
                ITextMessage message = session.CreateTextMessage(text);

                testPeer.ExpectTransfer(m => Assert.AreEqual(text, (m.BodySection as AmqpValue).Value));
                testPeer.ExpectClose();

                Assert.IsNull(message.NMSDestination, "Should not yet have a NMSDestination");

                producer.Send(message);

                Assert.AreEqual(destination, message.NMSDestination, "Should have had NMSDestination set");

                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestSendingMessageSetsNMSTimestamp()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(destination);

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

                ITextMessage message = session.CreateTextMessage(text);
                producer.Send(message);

                testPeer.WaitForAllMatchersToComplete(1000);

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestSendingMessageSetsNMSExpirationRelatedAbsoluteExpiryAndTtlFields()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(destination);

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

                ITextMessage message = session.CreateTextMessage(text);
                producer.Send(message, NMSConstants.defaultDeliveryMode, NMSConstants.defaultPriority, TimeSpan.FromMilliseconds(ttl));

                testPeer.WaitForAllMatchersToComplete(1000);

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }
        
        [Test, Timeout(20_000)]
        public void TestMessagesAreProducedWithProperDefaultPriorityWhenNoPrioritySpecified()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(destination);

                byte priority = 4;

                testPeer.ExpectTransfer(m => Assert.AreEqual(priority, m.Header.Priority));
                testPeer.ExpectClose();

                ITextMessage message = session.CreateTextMessage();
                Assert.AreEqual(MsgPriority.BelowNormal, message.NMSPriority);

                producer.Send(message);

                Assert.AreEqual((MsgPriority) priority, message.NMSPriority);

                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestNonDefaultPriorityProducesMessagesWithPriorityFieldAndSetsNMSPriority()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(destination);

                byte priority = 9;

                testPeer.ExpectTransfer(m => Assert.AreEqual(priority, m.Header.Priority));
                testPeer.ExpectClose();

                ITextMessage message = session.CreateTextMessage();
                Assert.AreEqual(MsgPriority.BelowNormal, message.NMSPriority);

                producer.Send(message, MsgDeliveryMode.Persistent, (MsgPriority) priority, NMSConstants.defaultTimeToLive);

                Assert.AreEqual((MsgPriority) priority, message.NMSPriority);

                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestSendingMessageSetsNMSMessageId()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(destination);

                string text = "myMessage";
                string actualMessageId = null;
                testPeer.ExpectTransfer(m =>
                {
                    Assert.IsTrue(m.Header.Durable);
                    Assert.IsNotEmpty(m.Properties.MessageId);
                    actualMessageId = m.Properties.MessageId;
                });
                testPeer.ExpectClose();

                ITextMessage message = session.CreateTextMessage(text);
                Assert.IsNull(message.NMSMessageId, "NMSMessageId should not yet be set");

                producer.Send(message);

                Assert.IsNotNull(message.NMSMessageId);
                Assert.IsNotEmpty(message.NMSMessageId, "NMSMessageId should be set");
                Assert.IsTrue(message.NMSMessageId.StartsWith("ID:"), "MMS 'ID:' prefix not found");

                connection.Close();

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
                IConnection connection = EstablishConnection(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(destination);

                string text = "myMessage";
                testPeer.ExpectTransfer(m =>
                {
                    Assert.IsTrue(m.Header.Durable);
                    Assert.IsNull(m.Properties.MessageId); // Check there is no message-id value;
                    Assert.AreEqual(text, (m.BodySection as AmqpValue).Value);
                });
                testPeer.ExpectClose();

                ITextMessage message = session.CreateTextMessage(text);

                Assert.IsNull(message.NMSMessageId, "NMSMessageId should not yet be set");

                if (existingId)
                {
                    string existingMessageId = "ID:this-should-be-overwritten-in-send";
                    message.NMSMessageId = existingMessageId;
                    Assert.AreEqual(existingMessageId, message.NMSMessageId, "NMSMessageId should now be se");
                }

                producer.DisableMessageID = true;

                producer.Send(message);

                Assert.IsNull(message.NMSMessageId, "NMSMessageID should be null");

                connection.Close();

                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestRemotelyCloseProducer()
        {
            string breadCrumb = "ErrorMessageBreadCrumb";

            ManualResetEvent producerClosed = new ManualResetEvent(false);
            Mock<INmsConnectionListener> mockConnectionListener = new Mock<INmsConnectionListener>();
            mockConnectionListener
                .Setup(listener => listener.OnProducerClosed(It.IsAny<NmsMessageProducer>(), It.IsAny<Exception>()))
                .Callback(() => { producerClosed.Set(); });

            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                NmsConnection connection = (NmsConnection) EstablishConnection(testPeer);
                connection.AddConnectionListener(mockConnectionListener.Object);

                testPeer.ExpectBegin();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);

                // Create a producer, then remotely end it afterwards.
                testPeer.ExpectSenderAttach();
                testPeer.RemotelyDetachLastOpenedLinkOnLastOpenedSession(expectDetachResponse: true, closed: true, errorType: AmqpError.RESOURCE_DELETED, breadCrumb, delayBeforeSend: 10);

                IQueue destination = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(destination);

                // Verify the producer gets marked closed
                testPeer.WaitForAllMatchersToComplete(1000);

                Assert.True(producerClosed.WaitOne(TimeSpan.FromMilliseconds(1000)), "Producer closed callback didn't trigger");
                Assert.That(() => producer.DisableMessageID, Throws.Exception.InstanceOf<IllegalStateException>(), "Producer never closed");

                // Try closing it explicitly, should effectively no-op in client.
                // The test peer will throw during close if it sends anything.
                producer.Close();
            }
        }

        [Test, Timeout(20_000)]
        public void TestSendWhenLinkCreditIsZeroAndTimeout()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer, optionsString: "nms.sendTimeout=500");
                testPeer.ExpectBegin();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");

                ITextMessage message = session.CreateTextMessage("text");

                // Expect the producer to attach. Don't send any credit so that the client will
                // block on a send and we can test our timeouts.
                testPeer.ExpectSenderAttachWithoutGrantingCredit();
                testPeer.ExpectClose();

                IMessageProducer producer = session.CreateProducer(queue);

                Assert.Catch<Exception>(() => producer.Send(message), "Send should time out.");

                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestSendTimesOutWhenNoDispositionArrives()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer, optionsString: "nms.sendTimeout=500");
                testPeer.ExpectBegin();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");

                ITextMessage message = session.CreateTextMessage("text");

                // Expect the producer to attach and grant it some credit, it should send
                // a transfer which we will not send any response for which should cause the
                // send operation to time out.
                testPeer.ExpectSenderAttach();
                testPeer.ExpectTransferButDoNotRespond(messageMatcher: Assert.NotNull);
                testPeer.ExpectClose();

                IMessageProducer producer = session.CreateProducer(queue);

                Assert.Catch<Exception>(() => producer.Send(message), "Send should time out.");

                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestSendWorksWhenConnectionNotStarted()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);

                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(destination);

                testPeer.ExpectTransfer(Assert.IsNotNull);

                producer.Send(session.CreateMessage());

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
                IConnection connection = EstablishConnection(testPeer);
                connection.Start();

                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(destination);

                testPeer.ExpectTransfer(Assert.IsNotNull);

                connection.Stop();

                producer.Send(session.CreateMessage());

                testPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);
                testPeer.ExpectClose();

                producer.Close();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }
  
        [Test, Timeout(20_000)]
        public void TestSendingMessagePersistentSetsBatchableFalse()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                connection.Start();

                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(destination);
                testPeer.ExpectTransfer(messageMatcher: Assert.IsNotNull,
                    stateMatcher: Assert.IsNull,
                    settled: false,
                    sendResponseDisposition: true,
                    responseState: new Accepted(),
                    responseSettled: true,
                    batchable: false);

                IMessage message = session.CreateMessage();
                producer.Send(message: message, deliveryMode: MsgDeliveryMode.Persistent, MsgPriority.Normal, NMSConstants.defaultTimeToLive);
                
                testPeer.WaitForAllMatchersToComplete(1000);

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestSendingMessageNonPersistentSetsBatchableFalse()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                connection.Start();

                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(destination);
                testPeer.ExpectTransfer(messageMatcher: Assert.IsNotNull,
                    stateMatcher: Assert.IsNull,
                    settled: false,
                    sendResponseDisposition: true,
                    responseState: new Accepted(),
                    responseSettled: true,
                    batchable: false);

                IMessage message = session.CreateMessage();
                producer.Send(message: message, deliveryMode: MsgDeliveryMode.NonPersistent, MsgPriority.Normal, NMSConstants.defaultTimeToLive);
                
                testPeer.WaitForAllMatchersToComplete(1000);

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }
    }
}