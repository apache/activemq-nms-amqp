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
    // Adapted from ProducerIntegrationTest to use NMSContext
    [TestFixture]
    public class NMSProducerIntegrationTestAsync : IntegrationTestFixture
    {
        private const long TICKS_PER_MILLISECOND = 10000;

        [Test, Timeout(20_000)]
        public async Task TestCloseSender()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await base.EstablishNMSContextAsync(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                IQueue queue = await context.GetQueueAsync("myQueue");
                var producer = await context.CreateProducerAsync();

                testPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                await producer.CloseAsync();
                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestSentTextMessageCanBeModified()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await base.EstablishNMSContextAsync(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                IQueue queue = await context.GetQueueAsync("myQueue");
                var producer = await context.CreateProducerAsync();

                // Create and transfer a new message
                String text = "myMessage";
                testPeer.ExpectTransfer(x => Assert.AreEqual(text, (x.BodySection as AmqpValue).Value));
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                ITextMessage message = await context.CreateTextMessageAsync(text);
                await producer.SendAsync(queue, message);

                Assert.AreEqual(text, message.Text);
                message.Text = text + text;
                Assert.AreEqual(text + text, message.Text);

                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestDefaultDeliveryModeProducesDurableMessages()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await base.EstablishNMSContextAsync(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                IQueue queue = await context.GetQueueAsync("myQueue");
                var producer = await context.CreateProducerAsync();

                // Create and transfer a new message
                testPeer.ExpectTransfer(message => Assert.IsTrue(message.Header.Durable));
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                ITextMessage textMessage = await context.CreateTextMessageAsync();

                await producer.SendAsync(queue, textMessage);
                Assert.AreEqual(MsgDeliveryMode.Persistent, textMessage.NMSDeliveryMode);

                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestProducerOverridesMessageDeliveryMode()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await base.EstablishNMSContextAsync(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                IQueue queue = await context.GetQueueAsync("myQueue");
                var producer = await context.CreateProducerAsync();

                // Create and transfer a new message, explicitly setting the deliveryMode on the
                // message (which applications shouldn't) to NON_PERSISTENT and sending it to check
                // that the producer ignores this value and sends the message as PERSISTENT(/durable)
                testPeer.ExpectTransfer(message => Assert.IsTrue(message.Header.Durable));
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                ITextMessage textMessage = await context.CreateTextMessageAsync();
                textMessage.NMSDeliveryMode = MsgDeliveryMode.NonPersistent;
                Assert.AreEqual(MsgDeliveryMode.NonPersistent, textMessage.NMSDeliveryMode);

                await producer.SendAsync(queue, textMessage);

                Assert.AreEqual(MsgDeliveryMode.Persistent, textMessage.NMSDeliveryMode);

                await context.CloseAsync();
                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestSendingMessageNonPersistentProducerSetDurableFalse()
        {
            await DoSendingMessageNonPersistentTestImpl(true);
        }

        [Test, Timeout(20_000)]
        public async Task TestSendingMessageNonPersistentProducerOmitsHeader()
        {
            await DoSendingMessageNonPersistentTestImpl(false);
        }

        private async Task DoSendingMessageNonPersistentTestImpl(bool setPriority)
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                //Add capability to indicate support for ANONYMOUS-RELAY
                Symbol[] serverCapabilities = {SymbolUtil.OPEN_CAPABILITY_ANONYMOUS_RELAY};
                var context = await EstablishNMSContextAsync(testPeer, serverCapabilities: serverCapabilities);
                testPeer.ExpectBegin();

                string queueName = "myQueue";
                Action<object> targetMatcher = t =>
                {
                    var target = t as Target;
                    Assert.IsNotNull(target);
                };


                testPeer.ExpectSenderAttach(targetMatcher: targetMatcher, sourceMatcher: Assert.NotNull, senderSettled: false);

                IQueue queue = await context.GetQueueAsync(queueName);
                INMSProducer producer = await context.CreateProducerAsync();

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

                ITextMessage textMessage = await context.CreateTextMessageAsync(text);

                producer.DeliveryMode = MsgDeliveryMode.NonPersistent;
                if (setPriority)
                    producer.Priority = (MsgPriority) priority;

                await producer.SendAsync(queue, textMessage);

                Assert.AreEqual(MsgDeliveryMode.NonPersistent, textMessage.NMSDeliveryMode, "Should have NonPersistent delivery mode set");

                testPeer.WaitForAllMatchersToComplete(1000);

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestSendingMessageSetsNMSDestination()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                IQueue destination = await context.GetQueueAsync("myQueue");
                var producer = await context.CreateProducerAsync();

                string text = "myMessage";
                ITextMessage message = await context.CreateTextMessageAsync(text);

                testPeer.ExpectTransfer(m => Assert.AreEqual(text, (m.BodySection as AmqpValue).Value));
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                Assert.IsNull(message.NMSDestination, "Should not yet have a NMSDestination");

                await producer.SendAsync(destination, message);

                Assert.AreEqual(destination, message.NMSDestination, "Should have had NMSDestination set");

                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestSendingMessageSetsNMSTimestamp()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                IQueue destination = await context.GetQueueAsync("myQueue");
                var producer = await context.CreateProducerAsync();

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

                ITextMessage message = await context.CreateTextMessageAsync(text);
                await producer.SendAsync(destination, message);

                testPeer.WaitForAllMatchersToComplete(1000);

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestSendingMessageSetsNMSExpirationRelatedAbsoluteExpiryAndTtlFields()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                IQueue destination = await context.GetQueueAsync("myQueue");
                var producer = await context.CreateProducerAsync();

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

                ITextMessage message = await context.CreateTextMessageAsync(text);
                producer.TimeToLive = TimeSpan.FromMilliseconds(ttl);
                producer.Priority = NMSConstants.defaultPriority;
                producer.DeliveryMode = NMSConstants.defaultDeliveryMode;
                await producer.SendAsync(destination, message);

                testPeer.WaitForAllMatchersToComplete(1000);

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestMessagesAreProducedWithProperDefaultPriorityWhenNoPrioritySpecified()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                IQueue destination = await context.GetQueueAsync("myQueue");
                var producer = await context.CreateProducerAsync();

                byte priority = 4;

                testPeer.ExpectTransfer(m => Assert.AreEqual(priority, m.Header.Priority));
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                ITextMessage message = await context.CreateTextMessageAsync();
                Assert.AreEqual(MsgPriority.BelowNormal, message.NMSPriority);

                await producer.SendAsync(destination, message);

                Assert.AreEqual((MsgPriority) priority, message.NMSPriority);

                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestNonDefaultPriorityProducesMessagesWithPriorityFieldAndSetsNMSPriority()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                IQueue destination = await context.GetQueueAsync("myQueue");
                var producer = await context.CreateProducerAsync();

                byte priority = 9;

                testPeer.ExpectTransfer(m => Assert.AreEqual(priority, m.Header.Priority));
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                ITextMessage message = await context.CreateTextMessageAsync();
                Assert.AreEqual(MsgPriority.BelowNormal, message.NMSPriority);

                producer.DeliveryMode = MsgDeliveryMode.Persistent;
                producer.Priority = (MsgPriority) priority;
                producer.TimeToLive = NMSConstants.defaultTimeToLive;
                await producer.SendAsync(destination, message);

                Assert.AreEqual((MsgPriority) priority, message.NMSPriority);

                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestSendingMessageSetsNMSMessageId()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                IQueue destination = await context.GetQueueAsync("myQueue");
                var producer = await context.CreateProducerAsync();

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

                ITextMessage message = await context.CreateTextMessageAsync(text);
                Assert.IsNull(message.NMSMessageId, "NMSMessageId should not yet be set");

                await producer.SendAsync(destination, message);

                Assert.IsNotNull(message.NMSMessageId);
                Assert.IsNotEmpty(message.NMSMessageId, "NMSMessageId should be set");
                Assert.IsTrue(message.NMSMessageId.StartsWith("ID:"), "MMS 'ID:' prefix not found");

                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
                // Get the value that was actually transmitted/received, verify it is a string, compare to what we have locally
                Assert.AreEqual(message.NMSMessageId, actualMessageId, "Expected NMSMessageId value to be present in AMQP message");
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestSendingMessageWithDisableMessageIdHint()
        {
            await DoSendingMessageWithDisableMessageIdHintTestImpl(false);
        }

        [Test, Timeout(20_000)]
        public async Task TestSendingMessageWithDisableMessageIdHintAndExistingMessageId()
        {
            await DoSendingMessageWithDisableMessageIdHintTestImpl(true);
        }

        private async Task DoSendingMessageWithDisableMessageIdHintTestImpl(bool existingId)
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                IQueue destination = await context.GetQueueAsync("myQueue");
                var producer = await context.CreateProducerAsync();

                string text = "myMessage";
                testPeer.ExpectTransfer(m =>
                {
                    Assert.IsTrue(m.Header.Durable);
                    Assert.IsNull(m.Properties.MessageId); // Check there is no message-id value;
                    Assert.AreEqual(text, (m.BodySection as AmqpValue).Value);
                });
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                ITextMessage message = await context.CreateTextMessageAsync(text);

                Assert.IsNull(message.NMSMessageId, "NMSMessageId should not yet be set");

                if (existingId)
                {
                    string existingMessageId = "ID:this-should-be-overwritten-in-send";
                    message.NMSMessageId = existingMessageId;
                    Assert.AreEqual(existingMessageId, message.NMSMessageId, "NMSMessageId should now be se");
                }

                producer.DisableMessageID = true;

                await producer.SendAsync(destination, message);

                Assert.IsNull(message.NMSMessageId, "NMSMessageID should be null");

                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }

        // TODO No connection listener in nms context
        // [Test, Timeout(20_000)]
        // public async Task TestRemotelyCloseProducer()
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
        public async Task TestSendWhenLinkCreditIsZeroAndTimeout()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer, optionsString: "nms.sendTimeout=500");
                testPeer.ExpectBegin();

                IQueue queue = await context.GetQueueAsync("myQueue");

                ITextMessage message = await context.CreateTextMessageAsync("text");

                // Expect the producer to attach. Don't send any credit so that the client will
                // block on a send and we can test our timeouts.
                testPeer.ExpectSenderAttachWithoutGrantingCredit();
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                var producer = await context.CreateProducerAsync();

                Assert.CatchAsync<Exception>(async () => await producer.SendAsync(queue, message), "Send should time out.");

                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestSendTimesOutWhenNoDispositionArrives()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer, optionsString: "nms.sendTimeout=500");
                testPeer.ExpectBegin();

                IQueue queue = await context.GetQueueAsync("myQueue");

                ITextMessage message = await context.CreateTextMessageAsync("text");

                // Expect the producer to attach and grant it some credit, it should send
                // a transfer which we will not send any response for which should cause the
                // send operation to time out.
                testPeer.ExpectSenderAttach();
                testPeer.ExpectTransferButDoNotRespond(messageMatcher: Assert.NotNull);
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                var producer = await context.CreateProducerAsync();

                Assert.CatchAsync<Exception>(async () => await producer.SendAsync(queue, message), "Send should time out.");

                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestSendWorksWhenConnectionNotStarted()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);

                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                IQueue destination = await context.GetQueueAsync("myQueue");
                var producer = await context.CreateProducerAsync();

                testPeer.ExpectTransfer(Assert.IsNotNull);

                await producer.SendAsync(destination, await context.CreateMessageAsync());

                testPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);
                await producer.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestSendWorksAfterConnectionStopped()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);
                await context.StartAsync();

                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                IQueue destination = await context.GetQueueAsync("myQueue");
                var producer = await context.CreateProducerAsync();

                testPeer.ExpectTransfer(Assert.IsNotNull);

                await context.StopAsync();

                await producer.SendAsync(destination, await context.CreateMessageAsync());

                testPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                await producer.CloseAsync();
                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestSendingMessagePersistentSetsBatchableFalse()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);
                await context.StartAsync();

                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                IQueue destination = await context.GetQueueAsync("myQueue");
                var producer = await context.CreateProducerAsync();
                testPeer.ExpectTransfer(messageMatcher: Assert.IsNotNull,
                    stateMatcher: Assert.IsNull,
                    settled: false,
                    sendResponseDisposition: true,
                    responseState: new Accepted(),
                    responseSettled: true,
                    batchable: false);

                IMessage message = await context.CreateMessageAsync();
                producer.DeliveryMode = MsgDeliveryMode.Persistent;
                producer.Priority = MsgPriority.Normal;
                producer.TimeToLive = NMSConstants.defaultTimeToLive;
                await producer.SendAsync(destination, message);

                testPeer.WaitForAllMatchersToComplete(1000);

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestSendingMessageNonPersistentSetsBatchableFalse()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);
                await context.StartAsync();

                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                IQueue destination = await context.GetQueueAsync("myQueue");
                var producer = await context.CreateProducerAsync();
                testPeer.ExpectTransfer(messageMatcher: Assert.IsNotNull,
                    stateMatcher: Assert.IsNull,
                    settled: false,
                    sendResponseDisposition: true,
                    responseState: new Accepted(),
                    responseSettled: true,
                    batchable: false);

                IMessage message = await context.CreateMessageAsync();
                producer.DeliveryMode = MsgDeliveryMode.NonPersistent;
                producer.Priority = MsgPriority.Normal;
                producer.TimeToLive = NMSConstants.defaultTimeToLive;
                await producer.SendAsync(destination, message);

                testPeer.WaitForAllMatchersToComplete(1000);

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }
    }
}