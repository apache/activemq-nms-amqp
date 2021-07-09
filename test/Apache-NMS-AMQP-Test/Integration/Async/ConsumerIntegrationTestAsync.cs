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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amqp.Framing;
using Apache.NMS;
using Apache.NMS.AMQP;
using Apache.NMS.AMQP.Message;
using Apache.NMS.AMQP.Util;
using Moq;
using NMS.AMQP.Test.TestAmqp;
using NMS.AMQP.Test.TestAmqp.BasicTypes;
using NUnit.Framework;

namespace NMS.AMQP.Test.Integration.Async
{
    [TestFixture]
    public class ConsumerIntegrationTestAsync : IntegrationTestFixture
    {
        [Test, Timeout(20_000)]
        public async Task TestCloseConsumer()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();

                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = await session.GetQueueAsync("myQueue");
                IMessageConsumer consumer = await session.CreateConsumerAsync(queue);

                testPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);
                await consumer.CloseAsync();

                testPeer.ExpectClose();
                await connection.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestRemotelyCloseConsumer()
        {
            Mock<INmsConnectionListener> mockConnectionListener = new Mock<INmsConnectionListener>();
            string errorMessage = "buba";

            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                ManualResetEvent consumerClosed = new ManualResetEvent(false);
                ManualResetEvent exceptionFired = new ManualResetEvent(false);

                mockConnectionListener
                    .Setup(listener => listener.OnConsumerClosed(It.IsAny<IMessageConsumer>(), It.IsAny<Exception>()))
                    .Callback(() => consumerClosed.Set());

                NmsConnection connection = (NmsConnection) await EstablishConnectionAsync(testPeer);
                connection.AddConnectionListener(mockConnectionListener.Object);
                connection.ExceptionListener += exception => { exceptionFired.Set(); };

                testPeer.ExpectBegin();
                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);

                // Create a consumer, then remotely end it afterwards.
                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();
                testPeer.RemotelyDetachLastOpenedLinkOnLastOpenedSession(expectDetachResponse: true, closed: true, errorType: AmqpError.RESOURCE_DELETED, errorMessage: errorMessage, delayBeforeSend: 400);

                IQueue queue = await session.GetQueueAsync("myQueue");
                IMessageConsumer consumer = await session.CreateConsumerAsync(queue);

                // Verify the consumer gets marked closed
                testPeer.WaitForAllMatchersToComplete(1000);

                Assert.True(consumerClosed.WaitOne(2000), "Consumer closed callback didn't trigger");
                Assert.False(exceptionFired.WaitOne(20), "Exception listener shouldn't fire with no MessageListener");

                // Try closing it explicitly, should effectively no-op in client.
                // The test peer will throw during close if it sends anything.
                await consumer.CloseAsync();
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestRemotelyCloseConsumerWithMessageListenerFiresExceptionListener()
        {
            Mock<INmsConnectionListener> mockConnectionListener = new Mock<INmsConnectionListener>();
            string errorMessage = "buba";

            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                ManualResetEvent consumerClosed = new ManualResetEvent(false);
                ManualResetEvent exceptionFired = new ManualResetEvent(false);

                mockConnectionListener
                    .Setup(listener => listener.OnConsumerClosed(It.IsAny<IMessageConsumer>(), It.IsAny<Exception>()))
                    .Callback(() => consumerClosed.Set());

                NmsConnection connection = (NmsConnection) await EstablishConnectionAsync(testPeer);
                connection.AddConnectionListener(mockConnectionListener.Object);
                connection.ExceptionListener += exception => { exceptionFired.Set(); };

                testPeer.ExpectBegin();
                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);

                // Create a consumer, then remotely end it afterwards.
                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();
                testPeer.RemotelyDetachLastOpenedLinkOnLastOpenedSession(expectDetachResponse: true, closed: true, errorType: AmqpError.RESOURCE_DELETED, errorMessage: errorMessage, 10);

                IQueue queue = await session.GetQueueAsync("myQueue");
                IMessageConsumer consumer = await session.CreateConsumerAsync(queue);

                consumer.Listener += message => { };

                // Verify the consumer gets marked closed
                testPeer.WaitForAllMatchersToComplete(1000);

                Assert.True(consumerClosed.WaitOne(2000), "Consumer closed callback didn't trigger");
                Assert.True(exceptionFired.WaitOne(2000), "Exception listener should have fired with a MessageListener");

                // Try closing it explicitly, should effectively no-op in client.
                // The test peer will throw during close if it sends anything.
                await consumer.CloseAsync();
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestReceiveMessageWithReceiveZeroTimeout()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);
                await connection.StartAsync();

                testPeer.ExpectBegin();

                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = await session.GetQueueAsync("myQueue");

                testPeer.ExpectReceiverAttach();

                testPeer.ExpectLinkFlowRespondWithTransfer(message: new Amqp.Message() { BodySection = new AmqpValue() { Value = null } }, count: 1);
                testPeer.ExpectDispositionThatIsAcceptedAndSettled();

                IMessageConsumer consumer = await session.CreateConsumerAsync(queue);
                IMessage message = await consumer.ReceiveAsync();
                Assert.NotNull(message, "A message should have been received");

                testPeer.ExpectClose();
                await connection.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(10000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestExceptionInOnMessageReleasesInAutoAckMode()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);
                await connection.StartAsync();

                testPeer.ExpectBegin();

                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = await session.GetQueueAsync("myQueue");

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: new Amqp.Message() { BodySection = new AmqpValue() { Value = null } }, count: 1);
                testPeer.ExpectDispositionThatIsReleasedAndSettled();

                IMessageConsumer consumer = await session.CreateConsumerAsync(queue);
                consumer.Listener += message => throw new Exception();

                testPeer.WaitForAllMatchersToComplete(2000);

                testPeer.ExpectClose();
                await connection.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(10000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestCloseDurableTopicSubscriberDetachesWithCloseFalse()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);
                await connection.StartAsync();

                testPeer.ExpectBegin();
                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);

                string topicName = "myTopic";
                string subscriptionName = "mySubscription";
                ITopic topic = await session.GetTopicAsync(topicName);

                testPeer.ExpectDurableSubscriberAttach(topicName, subscriptionName);
                testPeer.ExpectLinkFlow();

                IMessageConsumer durableConsumer = await session.CreateDurableConsumerAsync(topic, subscriptionName, null, false);

                testPeer.ExpectDetach(expectClosed: false, sendResponse: true, replyClosed: false);
                await durableConsumer.CloseAsync();

                testPeer.ExpectClose();
                await connection.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestConsumerReceiveThrowsIfConnectionLost()
        {
            await DoTestConsumerReceiveThrowsIfConnectionLost(false);
        }

        [Test, Timeout(20_000)]
        public async Task TestConsumerTimedReceiveThrowsIfConnectionLost()
        {
            await DoTestConsumerReceiveThrowsIfConnectionLost(true);
        }

        private async Task DoTestConsumerReceiveThrowsIfConnectionLost(bool useTimeout)
        {
            ManualResetEvent consumerReady = new ManualResetEvent(false);

            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);

                testPeer.ExpectBegin();

                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = await session.GetQueueAsync("queue");
                await connection.StartAsync();

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();
                testPeer.RunAfterLastHandler(() => { consumerReady.WaitOne(2000); });
                testPeer.DropAfterLastMatcher(delay: 10);

                IMessageConsumer consumer = await session.CreateConsumerAsync(queue);
                consumerReady.Set();

                try
                {
                    if (useTimeout)
                    {
                        await consumer.ReceiveAsync(TimeSpan.FromMilliseconds(10_0000));
                    }
                    else
                    {
                        await consumer.ReceiveAsync();
                    }


                    Assert.Fail("An exception should have been thrown");
                }
                catch (NMSException)
                {
                    // Expected
                }

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestConsumerReceiveNoWaitThrowsIfConnectionLost()
        {
            ManualResetEvent disconnected = new ManualResetEvent(false);

            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                NmsConnection connection = (NmsConnection) await EstablishConnectionAsync(testPeer);
                Mock<INmsConnectionListener> connectionListener = new Mock<INmsConnectionListener>();

                connectionListener
                    .Setup(listener => listener.OnConnectionFailure(It.IsAny<NMSException>()))
                    .Callback(() => { disconnected.Set(); });

                connection.AddConnectionListener(connectionListener.Object);

                await connection.StartAsync();

                testPeer.ExpectBegin();

                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = await session.GetQueueAsync("queue");

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();
                testPeer.RemotelyCloseConnection(expectCloseResponse: true, errorCondition: ConnectionError.CONNECTION_FORCED, errorMessage: "buba");

                IMessageConsumer consumer = await session.CreateConsumerAsync(queue);

                Assert.True(disconnected.WaitOne(), "Connection should be disconnected");

                try
                {
                    consumer.ReceiveNoWait();
                    Assert.Fail("An exception should have been thrown");
                }
                catch (NMSException)
                {
                    // Expected
                }
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestSetMessageListenerAfterStartAndSend()
        {
            int messageCount = 4;
            CountdownEvent latch = new CountdownEvent(messageCount);
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);
                await connection.StartAsync();

                testPeer.ExpectBegin();
                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = await session.GetQueueAsync("myQueue");
                await connection.StartAsync();

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithContent(), messageCount);

                IMessageConsumer consumer = await session.CreateConsumerAsync(destination);

                for (int i = 0; i < messageCount; i++)
                {
                    testPeer.ExpectDispositionThatIsAcceptedAndSettled();
                }

                consumer.Listener += message => latch.Signal();

                Assert.True(latch.Wait(4000), "Messages not received within given timeout. Count remaining: " + latch.CurrentCount);

                testPeer.WaitForAllMatchersToComplete(2000);

                testPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);

                await consumer.CloseAsync();

                testPeer.ExpectClose();
                await connection.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestNoReceivedMessagesWhenConnectionNotStarted()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);

                testPeer.ExpectBegin();

                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = await session.GetQueueAsync("myQueue");

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithContent(), count: 3);

                IMessageConsumer consumer = await session.CreateConsumerAsync(destination);

                // Wait for a message to arrive then try and receive it, which should not happen
                // since the connection is not started.
                Assert.Null(await consumer.ReceiveAsync(TimeSpan.FromMilliseconds(100)));

                testPeer.ExpectClose();
                await connection.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestNoReceivedNoWaitMessagesWhenConnectionNotStarted()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);

                testPeer.ExpectBegin();

                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = await session.GetQueueAsync("myQueue");

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithContent(), count: 3);

                IMessageConsumer consumer = await session.CreateConsumerAsync(destination);

                // Wait for a message to arrive then try and receive it, which should not happen
                // since the connection is not started.
                Assert.Null(consumer.ReceiveNoWait());

                testPeer.ExpectClose();
                await connection.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestSyncReceiveFailsWhenListenerSet()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);

                testPeer.ExpectBegin();

                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = await session.GetQueueAsync("myQueue");

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();

                IMessageConsumer consumer = await session.CreateConsumerAsync(destination);

                consumer.Listener += message => { };

                Assert.CatchAsync<NMSException>(async () => await consumer.ReceiveAsync(), "Should have thrown an exception.");
                Assert.CatchAsync<NMSException>(async () => await consumer.ReceiveAsync(TimeSpan.FromMilliseconds(1000)), "Should have thrown an exception.");
                Assert.CatchAsync<NMSException>(async () => consumer.ReceiveNoWait(), "Should have thrown an exception.");

                testPeer.ExpectClose();
                await connection.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestCreateProducerInOnMessage()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);
                await connection.StartAsync();

                testPeer.ExpectBegin();

                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = await session.GetQueueAsync("myQueue");
                IQueue outbound = await session.GetQueueAsync("ForwardDest");

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithContent(), 1);

                testPeer.ExpectSenderAttach();
                testPeer.ExpectTransfer(messageMatcher: Assert.NotNull);
                testPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);

                testPeer.ExpectDispositionThatIsAcceptedAndSettled();

                IMessageConsumer consumer = await session.CreateConsumerAsync(destination);

                consumer.Listener += message =>
                {
                    IMessageProducer producer = session.CreateProducer(outbound);
                    producer.Send(message);
                    producer.Close();
                };

                testPeer.WaitForAllMatchersToComplete(10_000);

                testPeer.ExpectClose();
                await connection.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestMessageListenerCallsConnectionCloseThrowsIllegalStateException()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);
                await connection.StartAsync();

                testPeer.ExpectBegin();

                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = await session.GetQueueAsync("myQueue");
                await connection.StartAsync();

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithContent(), 1);

                IMessageConsumer consumer = await session.CreateConsumerAsync(destination);

                testPeer.ExpectDispositionThatIsAcceptedAndSettled();

                ManualResetEvent latch = new ManualResetEvent(false);
                Exception exception = null;
                consumer.Listener += message =>
                {
                    try
                    {
                        connection.Close();
                    }
                    catch (Exception e)
                    {
                        exception = e;
                    }

                    latch.Set();
                };

                Assert.True(latch.WaitOne(4000), "Messages not received within given timeout.");
                Assert.IsNotNull(exception);
                Assert.IsInstanceOf<IllegalStateException>(exception);

                testPeer.WaitForAllMatchersToComplete(2000);

                testPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);
                await consumer.CloseAsync();

                testPeer.ExpectClose();
                await connection.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestMessageListenerCallsConnectionStopThrowsIllegalStateException()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);
                await connection.StartAsync();

                testPeer.ExpectBegin();

                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = await session.GetQueueAsync("myQueue");
                await connection.StartAsync();

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithContent(), 1);

                IMessageConsumer consumer = await session.CreateConsumerAsync(destination);

                testPeer.ExpectDispositionThatIsAcceptedAndSettled();

                ManualResetEvent latch = new ManualResetEvent(false);
                Exception exception = null;
                consumer.Listener += message =>
                {
                    try
                    {
                        connection.Stop();
                    }
                    catch (Exception e)
                    {
                        exception = e;
                    }

                    latch.Set();
                };

                Assert.True(latch.WaitOne(3000), "Messages not received within given timeout.");
                Assert.IsNotNull(exception);
                Assert.IsInstanceOf<IllegalStateException>(exception);

                testPeer.WaitForAllMatchersToComplete(2000);

                testPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);
                await consumer.CloseAsync();

                testPeer.ExpectClose();
                await connection.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestMessageListenerCallsSessionCloseThrowsIllegalStateException()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);
                await connection.StartAsync();

                testPeer.ExpectBegin();

                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = await session.GetQueueAsync("myQueue");
                await connection.StartAsync();

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithContent(), 1);

                IMessageConsumer consumer = await session.CreateConsumerAsync(destination);

                testPeer.ExpectDispositionThatIsAcceptedAndSettled();

                ManualResetEvent latch = new ManualResetEvent(false);
                Exception exception = null;
                consumer.Listener += message =>
                {
                    try
                    {
                        session.Close();
                    }
                    catch (Exception e)
                    {
                        exception = e;
                    }

                    latch.Set();
                };

                Assert.True(latch.WaitOne(3000), "Messages not received within given timeout.");
                Assert.IsNotNull(exception);
                Assert.IsInstanceOf<IllegalStateException>(exception);

                testPeer.WaitForAllMatchersToComplete(2000);

                testPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);
                await consumer.CloseAsync();

                testPeer.ExpectClose();
                await connection.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }

        // TODO: To be fixed
        [Test, Timeout(20_000), Ignore("Ignore")]
        public async Task TestMessageListenerClosesItsConsumer()
        {
            var latch = new ManualResetEvent(false);
            var exceptionListenerFired = new ManualResetEvent(false);
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);
                await connection.StartAsync();

                connection.ExceptionListener += _ => exceptionListenerFired.Set();

                testPeer.ExpectBegin();

                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = await session.GetQueueAsync("myQueue");
                await connection.StartAsync();

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithContent(), 1);

                IMessageConsumer consumer = await session.CreateConsumerAsync(destination);

                testPeer.ExpectLinkFlow(drain: true, sendDrainFlowResponse: true, creditMatcher: credit => Assert.AreEqual(99, credit)); // Not sure if expected credit is right
                testPeer.ExpectDispositionThatIsAcceptedAndSettled();
                testPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);

                Exception exception = null;
                consumer.Listener += message =>
                {
                    try
                    {
                        consumer.Close();
                        latch.Set();
                    }
                    catch (Exception e)
                    {
                        exception = e;
                    }
                };

                Assert.True(latch.WaitOne(TimeSpan.FromMilliseconds(1000)), "Process not completed within given timeout");
                Assert.IsNull(exception, "No error expected during close");

                testPeer.WaitForAllMatchersToComplete(2000);

                testPeer.ExpectClose();
                await connection.CloseAsync();

                Assert.False(exceptionListenerFired.WaitOne(20), "Exception listener shouldn't have fired");
                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestRecoverOrderingWithAsyncConsumer()
        {
            ManualResetEvent latch = new ManualResetEvent(false);
            Exception asyncError = null;

            int recoverCount = 5;
            int messageCount = 8;
            int testPayloadLength = 255;
            string payload = Encoding.UTF8.GetString(new byte[testPayloadLength]);

            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);
                await connection.StartAsync();

                testPeer.ExpectBegin();

                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.ClientAcknowledge);
                IQueue destination = await session.GetQueueAsync("myQueue");
                await connection.StartAsync();

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(
                    message: new Amqp.Message() { BodySection = new AmqpValue() { Value = payload } },
                    count: messageCount,
                    drain: false,
                    nextIncomingId: 1,
                    addMessageNumberProperty: true,
                    sendDrainFlowResponse: false,
                    sendSettled: false,
                    creditMatcher: credit => Assert.Greater(credit, messageCount)
                );

                IMessageConsumer consumer = await session.CreateConsumerAsync(destination);

                bool complete = false;
                int messageSeen = 0;
                int expectedIndex = 0;
                consumer.Listener += message =>
                {
                    if (complete)
                    {
                        return;
                    }

                    try
                    {
                        int actualIndex = message.Properties.GetInt(TestAmqpPeer.MESSAGE_NUMBER);
                        Assert.AreEqual(expectedIndex, actualIndex, "Received Message Out Of Order");

                        // don't ack the message until we receive it X times
                        if (messageSeen < recoverCount)
                        {
                            session.Recover();
                            messageSeen++;
                        }
                        else
                        {
                            messageSeen = 0;
                            expectedIndex++;

                            // Have the peer expect the accept the disposition (1-based, hence pre-incremented).
                            testPeer.ExpectDisposition(settled: true,
                                stateMatcher: state => Assert.AreEqual(state.Descriptor.Code, MessageSupport.ACCEPTED_INSTANCE.Descriptor.Code
                                ));

                            message.Acknowledge();

                            if (expectedIndex == messageCount)
                            {
                                complete = true;
                                latch.Set();
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        complete = true;
                        asyncError = e;
                        latch.Set();
                    }
                };

                Assert.True(latch.WaitOne(TimeSpan.FromSeconds(15)), "Messages not received within given timeout.");
                Assert.IsNull(asyncError, "Unexpected exception");

                testPeer.WaitForAllMatchersToComplete(2000);

                testPeer.ExpectClose();
                await connection.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestConsumerCloseWaitsForAsyncDeliveryToComplete()
        {
            ManualResetEvent latch = new ManualResetEvent(false);

            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);
                await connection.StartAsync();

                testPeer.ExpectBegin();

                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = await session.GetQueueAsync("myQueue");
                await connection.StartAsync();

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithContent(), count: 1);

                IMessageConsumer consumer = await session.CreateConsumerAsync(destination);

                testPeer.ExpectDispositionThatIsAcceptedAndSettled();

                consumer.Listener += _ =>
                {
                    latch.Set();
                    Task.Delay(TimeSpan.FromMilliseconds(100)).GetAwaiter().GetResult();
                };

                Assert.True(latch.WaitOne(TimeSpan.FromMilliseconds(3000)), "Messages not received within given timeout.");

                testPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);
                await consumer.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(2000);

                testPeer.ExpectClose();
                await connection.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestSessionCloseWaitsForAsyncDeliveryToComplete()
        {
            ManualResetEvent latch = new ManualResetEvent(false);

            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);
                await connection.StartAsync();

                testPeer.ExpectBegin();

                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = await session.GetQueueAsync("myQueue");
                await connection.StartAsync();

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithContent(), count: 1);

                IMessageConsumer consumer = await session.CreateConsumerAsync(destination);

                testPeer.ExpectDispositionThatIsAcceptedAndSettled();

                consumer.Listener += _ =>
                {
                    latch.Set();
                    Task.Delay(TimeSpan.FromMilliseconds(100)).GetAwaiter().GetResult();
                };

                Assert.True(latch.WaitOne(TimeSpan.FromMilliseconds(3000)), "Messages not received within given timeout.");

                testPeer.ExpectEnd();
                await session.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(2000);

                testPeer.ExpectClose();
                await connection.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestConnectionCloseWaitsForAsyncDeliveryToComplete()
        {
            ManualResetEvent latch = new ManualResetEvent(false);

            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);
                await connection.StartAsync();

                testPeer.ExpectBegin();

                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = await session.GetQueueAsync("myQueue");
                await connection.StartAsync();

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithContent(), count: 1);

                IMessageConsumer consumer = await session.CreateConsumerAsync(destination);

                testPeer.ExpectDispositionThatIsAcceptedAndSettled();

                consumer.Listener += _ =>
                {
                    latch.Set();
                    Task.Delay(TimeSpan.FromMilliseconds(100)).GetAwaiter().GetResult();
                };

                Assert.True(latch.WaitOne(TimeSpan.FromMilliseconds(3000)), "Messages not received within given timeout.");

                testPeer.ExpectClose();
                await connection.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestRecoveredMessageShouldNotBeMutated()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);
                await connection.StartAsync();

                testPeer.ExpectBegin();
                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.ClientAcknowledge);
                IQueue destination = await session.GetQueueAsync("myQueue");
                string originalPayload = "testMessage";

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: new Amqp.Message { BodySection = new AmqpValue() { Value = originalPayload } }, count: 1);

                IMessageConsumer consumer = await session.CreateConsumerAsync(destination);
                NmsTextMessage message = await consumer.ReceiveAsync() as NmsTextMessage;
                Assert.NotNull(message);
                message.IsReadOnlyBody = false;
                message.Text = message.Text + "Received";
                await session.RecoverAsync();

                ITextMessage recoveredMessage = await consumer.ReceiveAsync() as ITextMessage;
                Assert.IsNotNull(recoveredMessage);
                Assert.AreNotEqual(message.Text, recoveredMessage.Text);
                Assert.AreEqual(originalPayload, recoveredMessage.Text);
                Assert.AreNotSame(message, recoveredMessage);

                testPeer.ExpectClose();
                await connection.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }
    }
}