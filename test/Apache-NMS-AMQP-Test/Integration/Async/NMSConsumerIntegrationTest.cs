﻿/*
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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amqp.Framing;
using Amqp.Types;
using Apache.NMS;
using Apache.NMS.AMQP.Message;
using Apache.NMS.AMQP.Util;
using NMS.AMQP.Test.TestAmqp;
using NUnit.Framework;

namespace NMS.AMQP.Test.Integration.Async
{
    // Adapted from ConsumerIntegrationTest to use NMSContext
    [TestFixture]
    public class NMSConsumerIntegrationTestAsync : IntegrationTestFixture
    {
        [Test, Timeout(20_000)]
        public async Task TestCloseConsumer()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);
                testPeer.ExpectBegin();
                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();

                IQueue queue = await context.GetQueueAsync("myQueue");
                var consumer = await context.CreateConsumerAsync(queue);

                testPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);
                await consumer.CloseAsync();

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        // TODO No connection Listener in NMSContext
        // [Test, Timeout(20_000)]
        // public async Task TestRemotelyCloseConsumer()
        // {
        //     Mock<INmsConnectionListener> mockConnectionListener = new Mock<INmsConnectionListener>();
        //     string errorMessage = "buba";
        //
        //     using (TestAmqpPeer testPeer = new TestAmqpPeer())
        //     {
        //         ManualResetEvent consumerClosed = new ManualResetEvent(false);
        //         ManualResetEvent exceptionFired = new ManualResetEvent(false);
        //
        //         mockConnectionListener
        //             .Setup(listener => listener.OnConsumerClosed(It.IsAny<IMessageConsumer>(), It.IsAny<Exception>()))
        //             .Callback(() => consumerClosed.Set());
        //
        //         var context = (NmsContext) EstablishNMSContext(testPeer);
        //         context.ConnectionInterruptedListener += () => { consumerClosed.Set(); };// AddConnectionListener(mockConnectionListener.Object);}
        //         // context.list ConnectionInterruptedListener += () => { consumerClosed.Set(); };// AddConnectionListener(mockConnectionListener.Object);}
        //         context.ExceptionListener += exception => { exceptionFired.Set(); };
        //
        //         testPeer.ExpectBegin();
        //         // ISession session = context.CreateSession(AcknowledgementMode.AutoAcknowledge);
        //
        //         // Create a consumer, then remotely end it afterwards.
        //         testPeer.ExpectReceiverAttach();
        //         testPeer.ExpectLinkFlow();
        //         testPeer.RemotelyDetachLastOpenedLinkOnLastOpenedSession(expectDetachResponse: true, closed: true, errorType: AmqpError.RESOURCE_DELETED, errorMessage: errorMessage, delayBeforeSend: 400);
        //
        //         IQueue queue = context.GetQueue("myQueue");
        //         var consumer = context.CreateConsumer(queue);
        //         
        //         
        //         // Verify the consumer gets marked closed
        //         testPeer.WaitForAllMatchersToComplete(1000);
        //
        //         Assert.True(consumerClosed.WaitOne(2000), "Consumer closed callback didn't trigger");
        //         Assert.False(exceptionFired.WaitOne(20), "Exception listener shouldn't fire with no MessageListener");
        //
        //         // Try closing it explicitly, should effectively no-op in client.
        //         // The test peer will throw during close if it sends anything.
        //         consumer.Close();
        //     }
        // }

        // [Test, Timeout(20_000)]
        // public async Task TestRemotelyCloseConsumerWithMessageListenerFiresExceptionListener()
        // {
        //     Mock<INmsConnectionListener> mockConnectionListener = new Mock<INmsConnectionListener>();
        //     string errorMessage = "buba";
        //
        //     using (TestAmqpPeer testPeer = new TestAmqpPeer())
        //     {
        //         ManualResetEvent consumerClosed = new ManualResetEvent(false);
        //         ManualResetEvent exceptionFired = new ManualResetEvent(false);
        //
        //         mockConnectionListener
        //             .Setup(listener => listener.OnConsumerClosed(It.IsAny<IMessageConsumer>(), It.IsAny<Exception>()))
        //             .Callback(() => consumerClosed.Set());
        //
        //         NmsConnection connection = (NmsConnection) EstablishConnection(testPeer);
        //         connection.AddConnectionListener(mockConnectionListener.Object);
        //         connection.ExceptionListener += exception => { exceptionFired.Set(); };
        //
        //         testPeer.ExpectBegin();
        //         ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
        //
        //         // Create a consumer, then remotely end it afterwards.
        //         testPeer.ExpectReceiverAttach();
        //         testPeer.ExpectLinkFlow();
        //         testPeer.RemotelyDetachLastOpenedLinkOnLastOpenedSession(expectDetachResponse: true, closed: true, errorType: AmqpError.RESOURCE_DELETED, errorMessage: errorMessage, 10);
        //
        //         IQueue queue = session.GetQueue("myQueue");
        //         IMessageConsumer consumer = session.CreateConsumer(queue);
        //
        //         consumer.Listener += message => { };
        //
        //         // Verify the consumer gets marked closed
        //         testPeer.WaitForAllMatchersToComplete(1000);
        //
        //         Assert.True(consumerClosed.WaitOne(2000), "Consumer closed callback didn't trigger");
        //         Assert.True(exceptionFired.WaitOne(2000), "Exception listener should have fired with a MessageListener");
        //
        //         // Try closing it explicitly, should effectively no-op in client.
        //         // The test peer will throw during close if it sends anything.
        //         consumer.Close();
        //     }
        // }

        [Test, Timeout(20_000)]
        public async Task TestReceiveMessageWithReceiveZeroTimeout()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);
                await context.StartAsync();

                testPeer.ExpectBegin();

                IQueue queue = await context.GetQueueAsync("myQueue");

                testPeer.ExpectReceiverAttach();

                testPeer.ExpectLinkFlowRespondWithTransfer(message: new Amqp.Message() { BodySection = new AmqpValue() { Value = null } }, count: 1);
                testPeer.ExpectDispositionThatIsAcceptedAndSettled();

                var consumer = await context.CreateConsumerAsync(queue);
                IMessage message = await consumer.ReceiveAsync();
                Assert.NotNull(message, "A message should have been received");

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(10000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestExceptionInOnMessageReleasesInAutoAckMode()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);
                await context.StartAsync();

                testPeer.ExpectBegin();

                IQueue queue = await context.GetQueueAsync("myQueue");

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: new Amqp.Message() { BodySection = new AmqpValue() { Value = null } }, count: 1);
                testPeer.ExpectDispositionThatIsReleasedAndSettled();

                var consumer = await context.CreateConsumerAsync(queue);
                consumer.Listener += message => throw new Exception();

                testPeer.WaitForAllMatchersToComplete(2000);

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(10000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestCloseDurableTopicSubscriberDetachesWithCloseFalse()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);
                await context.StartAsync();

                testPeer.ExpectBegin();
                
                string topicName = "myTopic";
                string subscriptionName = "mySubscription";
                ITopic topic = await context.GetTopicAsync(topicName);

                testPeer.ExpectDurableSubscriberAttach(topicName, subscriptionName);
                testPeer.ExpectLinkFlow();

                var durableConsumer = await context.CreateDurableConsumerAsync(topic, subscriptionName, null, false);

                testPeer.ExpectDetach(expectClosed: false, sendResponse: true, replyClosed: false);
                await durableConsumer.CloseAsync();

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                await context.CloseAsync();

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
                var context = await EstablishNMSContextAsync(testPeer);

                testPeer.ExpectBegin();

                IQueue queue = await context.GetQueueAsync("queue");
                await context.StartAsync();

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();
                testPeer.RunAfterLastHandler(() => { consumerReady.WaitOne(2000); });
                testPeer.DropAfterLastMatcher(delay: 10);

                var consumer = await context.CreateConsumerAsync(queue);
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

        //  TODO No connection Listener in context
        // [Test, Timeout(20_000)]
        // public async Task TestConsumerReceiveNoWaitThrowsIfConnectionLost()
        // {
        //     ManualResetEvent disconnected = new ManualResetEvent(false);
        //
        //     using (TestAmqpPeer testPeer = new TestAmqpPeer())
        //     {
        //         NmsContext context = (NmsContext) EstablishNMSContext(testPeer);
        //         Mock<INmsConnectionListener> connectionListener = new Mock<INmsConnectionListener>();
        //
        //         connectionListener
        //             .Setup(listener => listener.OnConnectionFailure(It.IsAny<NMSException>()))
        //             .Callback(() => { disconnected.Set(); });
        //
        //         context.AddConnectionListener(connectionListener.Object);
        //
        //         context.Start();
        //
        //         testPeer.ExpectBegin();
        //
        //         IQueue queue = context.GetQueue("queue");
        //
        //         testPeer.ExpectReceiverAttach();
        //         testPeer.ExpectLinkFlow();
        //         testPeer.RemotelyCloseConnection(expectCloseResponse: true, errorCondition: ConnectionError.CONNECTION_FORCED, errorMessage: "buba");
        //
        //         var consumer = context.CreateConsumer(queue);
        //
        //         Assert.True(disconnected.WaitOne(), "Connection should be disconnected");
        //
        //         try
        //         {
        //             consumer.ReceiveNoWait();
        //             Assert.Fail("An exception should have been thrown");
        //         }
        //         catch (NMSException)
        //         {
        //             // Expected
        //         }
        //     }
        // }

        [Test, Timeout(20_000)]
        public async Task TestSetMessageListenerAfterStartAndSend()
        {
            int messageCount = 4;
            CountdownEvent latch = new CountdownEvent(messageCount);
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);
                await context.StartAsync();

                testPeer.ExpectBegin();
                IQueue destination = await context.GetQueueAsync("myQueue");
                await context.StartAsync();

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithContent(), messageCount);

                var consumer = await context.CreateConsumerAsync(destination);

                for (int i = 0; i < messageCount; i++)
                {
                    testPeer.ExpectDispositionThatIsAcceptedAndSettled();
                }

                consumer.Listener += message => latch.Signal();

                Assert.True(latch.Wait(4000), "Messages not received within given timeout. Count remaining: " + latch.CurrentCount);

                testPeer.WaitForAllMatchersToComplete(2000);

                testPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);

                await consumer.CloseAsync();

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestSyncReceiveFailsWhenListenerSet()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);

                testPeer.ExpectBegin();

                IQueue destination = await context.GetQueueAsync("myQueue");

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();

                var consumer = await context.CreateConsumerAsync(destination);

                consumer.Listener += message => { };

                Assert.CatchAsync<NMSException>(async () => await consumer.ReceiveAsync(), "Should have thrown an exception.");
                Assert.CatchAsync<NMSException>(async () => await consumer.ReceiveAsync(TimeSpan.FromMilliseconds(1000)), "Should have thrown an exception.");
                Assert.CatchAsync<NMSException>(async () => consumer.ReceiveNoWait(), "Should have thrown an exception.");

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestCreateProducerInOnMessage()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);
                await context.StartAsync();

                testPeer.ExpectBegin();

                IQueue destination = await context.GetQueueAsync("myQueue");
                IQueue outbound = await context.GetQueueAsync("ForwardDest");

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithContent(), 1);

                testPeer.ExpectSenderAttach();
                testPeer.ExpectTransfer(messageMatcher: Assert.NotNull);
                testPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);

                testPeer.ExpectDispositionThatIsAcceptedAndSettled();

                var consumer = await context.CreateConsumerAsync(destination);

                consumer.Listener += message =>
                {
                    var producer = context.CreateProducer();
                    producer.Send(outbound, message);
                    producer.Close();
                };

                testPeer.WaitForAllMatchersToComplete(10_000);

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestMessageListenerCallsConnectionCloseThrowsIllegalStateException()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);
                await context.StartAsync();

                testPeer.ExpectBegin();

                IQueue destination = await context.GetQueueAsync("myQueue");
                await context.StartAsync();

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithContent(), 1);

                var consumer = await context.CreateConsumerAsync(destination);

                testPeer.ExpectDispositionThatIsAcceptedAndSettled();

                ManualResetEvent latch = new ManualResetEvent(false);
                Exception exception = null;
                consumer.Listener += message =>
                {
                    try
                    {
                        context.Close();
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

                testPeer.ExpectEnd();
                // testPeer.ExpectClose();
                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestMessageListenerCallsConnectionStopThrowsIllegalStateException()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);
                await context.StartAsync();

                testPeer.ExpectBegin();

                IQueue destination = await context.GetQueueAsync("myQueue");
                await context.StartAsync();

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithContent(), 1);

                var consumer = await context.CreateConsumerAsync(destination);

                testPeer.ExpectDispositionThatIsAcceptedAndSettled();

                ManualResetEvent latch = new ManualResetEvent(false);
                Exception exception = null;
                consumer.Listener += message =>
                {
                    try
                    {
                        context.Stop();
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

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestMessageListenerCallsSessionCloseThrowsIllegalStateException()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);
                await context.StartAsync();

                testPeer.ExpectBegin();

                IQueue destination = await context.GetQueueAsync("myQueue");
                await context.StartAsync();

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithContent(), 1);

                var consumer = await context.CreateConsumerAsync(destination);

                testPeer.ExpectDispositionThatIsAcceptedAndSettled();

                ManualResetEvent latch = new ManualResetEvent(false);
                Exception exception = null;
                consumer.Listener += message =>
                {
                    try
                    {
                        context.Close();
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

                testPeer.ExpectEnd();
                // testPeer.ExpectClose();
                await context.CloseAsync();

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
                var context = await EstablishNMSContextAsync(testPeer);
                await context.StartAsync();

                context.ExceptionListener += _ => exceptionListenerFired.Set();

                testPeer.ExpectBegin();

                IQueue destination = await context.GetQueueAsync("myQueue");
                await context.StartAsync();

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithContent(), 1);

                var consumer = await context.CreateConsumerAsync(destination);

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

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                await context.CloseAsync();

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
                var context = await EstablishNMSContextAsync(testPeer, acknowledgementMode:AcknowledgementMode.ClientAcknowledge);
                await context.StartAsync();

                testPeer.ExpectBegin();

                IQueue destination = await context.GetQueueAsync("myQueue");
                await context.StartAsync();

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

                var consumer = await context.CreateConsumerAsync(destination);
                
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
                            context.Recover();
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

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestConsumerCloseWaitsForAsyncDeliveryToComplete()
        {
            ManualResetEvent latch = new ManualResetEvent(false);

            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);
                await context.StartAsync();

                testPeer.ExpectBegin();

                IQueue destination = await context.GetQueueAsync("myQueue");
                await context.StartAsync();

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithContent(), count: 1);

                var consumer = await context.CreateConsumerAsync(destination);

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

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestSessionCloseWaitsForAsyncDeliveryToComplete()
        {
            ManualResetEvent latch = new ManualResetEvent(false);

            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);
                await context.StartAsync();

                testPeer.ExpectBegin();

                IQueue destination = await context.GetQueueAsync("myQueue");
                await context.StartAsync();

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithContent(), count: 1);

                var consumer = await context.CreateConsumerAsync(destination);

                testPeer.ExpectDispositionThatIsAcceptedAndSettled();

                consumer.Listener += _ =>
                {
                    latch.Set();
                    Task.Delay(TimeSpan.FromMilliseconds(100)).GetAwaiter().GetResult();
                };

                Assert.True(latch.WaitOne(TimeSpan.FromMilliseconds(3000)), "Messages not received within given timeout.");

                
                testPeer.WaitForAllMatchersToComplete(2000);

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestConnectionCloseWaitsForAsyncDeliveryToComplete()
        {
            ManualResetEvent latch = new ManualResetEvent(false);

            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);
                await context.StartAsync();

                testPeer.ExpectBegin();

                IQueue destination = await context.GetQueueAsync("myQueue");
                await context.StartAsync();

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithContent(), count: 1);

                var consumer = await context.CreateConsumerAsync(destination);

                testPeer.ExpectDispositionThatIsAcceptedAndSettled();

                consumer.Listener += _ =>
                {
                    latch.Set();
                    Task.Delay(TimeSpan.FromMilliseconds(100)).GetAwaiter().GetResult();
                };

                Assert.True(latch.WaitOne(TimeSpan.FromMilliseconds(3000)), "Messages not received within given timeout.");

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestRecoveredMessageShouldNotBeMutated()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer, acknowledgementMode:AcknowledgementMode.ClientAcknowledge);
                await context.StartAsync();

                testPeer.ExpectBegin();
                IQueue destination = await context.GetQueueAsync("myQueue");
                string originalPayload = "testMessage";

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: new Amqp.Message { BodySection = new AmqpValue() { Value = originalPayload } }, count: 1);

                var consumer = await context.CreateConsumerAsync(destination);
                NmsTextMessage message = await consumer.ReceiveAsync() as NmsTextMessage;
                Assert.NotNull(message);
                message.IsReadOnlyBody = false;
                message.Text = message.Text + "Received";
                await context.RecoverAsync();

                ITextMessage recoveredMessage = await consumer.ReceiveAsync() as ITextMessage;
                Assert.IsNotNull(recoveredMessage);
                Assert.AreNotEqual(message.Text, recoveredMessage.Text);
                Assert.AreEqual(originalPayload, recoveredMessage.Text);
                Assert.AreNotSame(message, recoveredMessage);

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }
        
        [TestCaseSource("TestReceiveBodyCaseSource")]
        [Timeout(20_000)]
        public async Task TestReceiveBody<T>(T inputValue)
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);

                testPeer.ExpectBegin();
                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(CreateMessageWithValueContent(inputValue));
                testPeer.ExpectDisposition(true, _ => { } );
                

                IQueue destination = await context.GetQueueAsync("myQueue");
                var consumer = await context.CreateConsumerAsync(destination);

                T body = await consumer.ReceiveBodyAsync<T>();
                Assert.AreEqual(inputValue, body);
                Assert.AreNotSame(inputValue, body);


                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                
                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }
        
       

        public static IEnumerable<object> TestReceiveBodyCaseSource()
        {
            yield return new Map()
            {
                ["Parameter1"] = "test",
                ["Parameter2"] = 23423
            };
            yield return 1233;
            yield return "test";
            yield return (uint) 1233;
            yield return (ulong) 1233;
            yield return (long) -1233;
        }
    }
}