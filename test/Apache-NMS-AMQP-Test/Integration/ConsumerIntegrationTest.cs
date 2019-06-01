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
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Apache.NMS;
using Apache.NMS.AMQP;
using Apache.NMS.AMQP.Message;
using Apache.NMS.AMQP.Provider.Amqp.Message;
using Moq;
using NMS.AMQP.Test.TestAmqp;
using NUnit.Framework;
using Test.Amqp;

namespace NMS.AMQP.Test.Integration
{
    [TestFixture]
    public class ConsumerIntegrationTest
    {
        private static readonly string User = "USER";
        private static readonly string Password = "PASSWORD";
        private static readonly string Address = "amqp://127.0.0.1:5672";
        private static readonly IPEndPoint IPEndPoint = new IPEndPoint(IPAddress.Any, 5672);

        [Test, Timeout(2000)]
        public void TestCloseConsumer()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                TestLinkProcessor testLinkProcessor = new TestLinkProcessor();
                testAmqpPeer.RegisterLinkProcessor(testLinkProcessor);

                testAmqpPeer.Open();
                IConnection connection = EstablishConnection();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);

                IQueue queue = session.GetQueue("myQueue");
                IMessageConsumer consumer = session.CreateConsumer(queue);

                Assert.NotNull(testLinkProcessor.Consumer);
                consumer.Close();
                Assert.That(() => testLinkProcessor.Consumer, Is.Null.After(500));
                session.Close();
                connection.Close();
            }
        }

        [Test, Timeout(2000)]
        public void TestRemotelyCloseConsumer()
        {
            Mock<INmsConnectionListener> mockConnectionListener = new Mock<INmsConnectionListener>();
            bool exceptionFired = false;

            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                TestLinkProcessor testLinkProcessor = new TestLinkProcessor();
                testAmqpPeer.RegisterLinkProcessor(testLinkProcessor);
                testAmqpPeer.Open();

                NmsConnection connection = (NmsConnection) EstablishConnection();
                connection.Start();
                connection.AddConnectionListener(mockConnectionListener.Object);
                connection.ExceptionListener += exception => { exceptionFired = true; };

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");

                // Create a consumer, then remotely end it afterwards.
                IMessageConsumer consumer = session.CreateConsumer(queue);
                testLinkProcessor.CloseConsumer();

                Assert.That(() => exceptionFired, Is.False.After(200), "Exception listener shouldn't fire with no MessageListener");
                // Verify the consumer gets marked closed
                mockConnectionListener.Verify(listener => listener.OnConsumerClosed(It.Is<IMessageConsumer>(x => x == consumer), It.IsAny<Exception>()), Times.Once, "consumer never closed.");

                session.Close();
                connection.Close();
            }
        }

        [Test, Timeout(2000)]
        public void TestRemotelyCloseConsumerWithMessageListenerFiresExceptionListener()
        {
            Mock<INmsConnectionListener> mockConnectionListener = new Mock<INmsConnectionListener>();
            bool exceptionFired = false;

            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                var testLinkProcessor = new TestLinkProcessor();
                testAmqpPeer.RegisterLinkProcessor(testLinkProcessor);
                testAmqpPeer.Open();

                NmsConnection connection = (NmsConnection) EstablishConnection();
                connection.Start();
                connection.AddConnectionListener(mockConnectionListener.Object);
                connection.ExceptionListener += exception => { exceptionFired = true; };

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");

                // Create a consumer, then remotely end it afterwards.
                IMessageConsumer consumer = session.CreateConsumer(queue);
                consumer.Listener += message => { };

                testLinkProcessor.CloseConsumerWithError();

                Assert.That(() => exceptionFired, Is.True.After(200));

                // Verify the consumer gets marked closed
                mockConnectionListener.Verify(listener => listener.OnConsumerClosed(It.Is<IMessageConsumer>(x => x == consumer), It.IsAny<Exception>()), Times.Once, "consumer never closed.");

                // Try closing it explicitly, should effectively no-op in client.
                // The test peer will throw during close if it sends anything.
                consumer.Close();
                session.Close();
                connection.Close();
            }
        }

        [Test, Timeout(2000)]
        public void TestReceiveMessageWithReceiveZeroTimeout()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                testAmqpPeer.SendMessage("myQueue", "test");

                NmsConnection connection = (NmsConnection) EstablishConnection();
                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");
                IMessageConsumer consumer = session.CreateConsumer(queue);
                IMessage message = consumer.Receive();
                Assert.NotNull(message, "A message should have been received");

                session.Close();
                connection.Close();
            }
        }

        [Test, Timeout(2000)]
        public void TestExceptionInOnMessageReleasesInAutoAckMode()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                testAmqpPeer.SendMessage("myQueue", "test");

                NmsConnection connection = (NmsConnection) EstablishConnection();
                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");
                IMessageConsumer consumer = session.CreateConsumer(queue);
                consumer.Listener += message => throw new Exception();

                Assert.That(() => testAmqpPeer.ReleasedMessages.Count(), Is.EqualTo(1).After(2000, 100));

                session.Close();
                connection.Close();
            }
        }

        [Test, Timeout(2000)]
        public void TestCloseDurableTopicSubscriberDetachesWithCloseFalse()
        {
            using (var testListener = new TestListener(IPEndPoint))
            {
                testListener.Open();
                Amqp.Types.List result = null;
                ManualResetEvent manualResetEvent = new ManualResetEvent(false);

                testListener.RegisterTarget(TestPoint.Detach, (stream, channel, fields) =>
                {
                    TestListener.FRM(stream, 0x16UL, 0, channel, fields[0], false);
                    result = fields;
                    manualResetEvent.Set();
                    return TestOutcome.Stop;
                });

                IConnection connection = EstablishConnection();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);

                string topicName = "myTopic";
                string subscriptionName = "mySubscription";
                ITopic topic = session.GetTopic(topicName);
                IMessageConsumer durableConsumer = session.CreateDurableConsumer(topic, subscriptionName, null, false);
                durableConsumer.Close();

                manualResetEvent.WaitOne(TimeSpan.FromMilliseconds(100));

                // Assert that closed field is set to false
                Assert.IsFalse((bool) result[1]);
            }
        }


        [Test, Timeout(2000), Ignore("It doesn't work for now as we do not have access to messages buffered by amqplite")]
        public void TestCloseDurableSubscriberWithUnackedAnUnconsumedPrefetchedMessages()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                string topicName = "myTopic";
                testAmqpPeer.Open();

                IConnection connection = EstablishConnection();
                ISession session = connection.CreateSession(AcknowledgementMode.ClientAcknowledge);

                string subscriptionName = "mySubscription";
                ITopic topic = session.GetTopic(topicName);


                // Create a consumer and fill the prefetch with some messages,
                // which we will consume some of but ack none of.
                for (int i = 0; i < 5; i++)
                {
                    testAmqpPeer.SendMessage(topicName, "test" + i);
                }

                IMessageConsumer durableConsumer = session.CreateDurableConsumer(topic, subscriptionName, null, false);

                int consumeCount = 2;
                IMessage receivedMessage = null;
                for (int i = 1; i <= consumeCount; i++)
                {
                    receivedMessage = durableConsumer.Receive();
                    Assert.NotNull(receivedMessage);
                    Assert.IsInstanceOf<NmsTextMessage>(receivedMessage);
                }

                Assert.NotNull(receivedMessage);
                receivedMessage.Acknowledge();
                durableConsumer.Close();

                // Expect the messages that were not delivered to be released.
                Assert.AreEqual(2, testAmqpPeer.ReleasedMessages.Count());
            }
        }

        [Test, Timeout(2000)]
        public void TestConsumerReceiveThrowsIfConnectionLost()
        {
            DoTestConsumerReceiveThrowsIfConnectionLost(false);
        }

        [Test, Timeout(2000)]
        public void TestConsumerTimedReceiveThrowsIfConnectionLost()
        {
            DoTestConsumerReceiveThrowsIfConnectionLost(true);
        }

        private void DoTestConsumerReceiveThrowsIfConnectionLost(bool useTimeout)
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                string topicName = "myTopic";
                testAmqpPeer.Open();
                ManualResetEvent disconnected = new ManualResetEvent(false);

                NmsConnection connection = (NmsConnection) EstablishConnection();

                Mock<INmsConnectionListener> connectionListener = new Mock<INmsConnectionListener>();

                connectionListener
                    .Setup(listener => listener.OnConnectionFailure(It.IsAny<NMSException>()))
                    .Callback(() => { disconnected.Set(); });

                connection.AddConnectionListener(connectionListener.Object);

                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.ClientAcknowledge);
                ITopic topic = session.GetTopic(topicName);
                testAmqpPeer.SendMessage(topicName, "test");
                IMessageConsumer consumer = session.CreateConsumer(topic);

                testAmqpPeer.Close();

                Assert.True(disconnected.WaitOne(), "Connection should be disconnected");

                try
                {
                    if (useTimeout)
                    {
                        consumer.Receive(TimeSpan.FromMilliseconds(1000));
                    }
                    else
                    {
                        consumer.Receive();
                    }


                    Assert.Fail("An exception should have been thrown");
                }
                catch (NMSException)
                {
                    // Expected
                }
            }
        }

        [Test, Timeout(2000)]
        public void TestConsumerReceiveNoWaitThrowsIfConnectionLost()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                string topicName = "myTopic";
                testAmqpPeer.Open();

                ManualResetEvent disconnected = new ManualResetEvent(false);

                NmsConnection connection = (NmsConnection) EstablishConnection();

                Mock<INmsConnectionListener> connectionListener = new Mock<INmsConnectionListener>();

                connectionListener
                    .Setup(listener => listener.OnConnectionFailure(It.IsAny<NMSException>()))
                    .Callback(() => { disconnected.Set(); });

                connection.AddConnectionListener(connectionListener.Object);

                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.ClientAcknowledge);
                ITopic topic = session.GetTopic(topicName);
                testAmqpPeer.SendMessage(topicName, "test");
                IMessageConsumer consumer = session.CreateConsumer(topic);

                testAmqpPeer.Close();

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

        [Test, Timeout(2000)]
        public void TestSetMessageListenerAfterStartAndSend()
        {
            int messageCount = 4;
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                for (int i = 0; i < messageCount; i++)
                {
                    testAmqpPeer.SendMessage("myQueue", "test" + i);
                }

                NmsConnection connection = (NmsConnection) EstablishConnection();
                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");
                IMessageConsumer consumer = session.CreateConsumer(queue);

                consumer.Listener += message => { };

                Assert.That(() => testAmqpPeer.AcceptedMessages.Count(), Is.EqualTo(messageCount).After(2000, 100));

                consumer.Close();
                session.Close();
                connection.Close();
            }
        }

        [Test, Timeout(2000)]
        public void TestNoReceivedMessagesWhenConnectionNotStarted()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                testAmqpPeer.SendMessage("myQueue", "test");

                NmsConnection connection = (NmsConnection) EstablishConnection();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");
                IMessageConsumer consumer = session.CreateConsumer(queue);

                Assert.Null(consumer.Receive(TimeSpan.FromMilliseconds(100)));

                consumer.Close();
                session.Close();
                connection.Close();
            }
        }

        [Test, Timeout(2000)]
        public void TestNoReceivedNoWaitMessagesWhenConnectionNotStarted()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                testAmqpPeer.SendMessage("myQueue", "test");

                NmsConnection connection = (NmsConnection) EstablishConnection();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");
                IMessageConsumer consumer = session.CreateConsumer(queue);

                Assert.Null(consumer.ReceiveNoWait());

                consumer.Close();
                session.Close();
                connection.Close();
            }
        }

        [Test, Timeout(2000)]
        public void TestSyncReceiveFailsWhenListenerSet()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.RegisterLinkProcessor(new TestLinkProcessor());
                testAmqpPeer.Open();

                NmsConnection connection = (NmsConnection) EstablishConnection();
                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");
                IMessageConsumer consumer = session.CreateConsumer(queue);

                consumer.Listener += message => { };

                try
                {
                    consumer.Receive();
                    Assert.Fail("Should have thrown an exception.");
                }
                catch (NMSException)
                {
                }

                try
                {
                    consumer.Receive(TimeSpan.FromMilliseconds(1000));
                    Assert.Fail("Should have thrown an exception.");
                }
                catch (NMSException)
                {
                }

                try
                {
                    consumer.ReceiveNoWait();
                    Assert.Fail("Should have thrown an exception.");
                }
                catch (NMSException)
                {
                }

                consumer.Close();
                session.Close();
                connection.Close();
            }
        }

        [Test, Timeout(2000)]
        public void TestCreateProducerInOnMessage()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.RegisterLinkProcessor(new TestLinkProcessor());
                testAmqpPeer.Open();

                NmsConnection connection = (NmsConnection) EstablishConnection();
                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IQueue outbound = session.GetQueue("ForwardDest");
                IMessageConsumer consumer = session.CreateConsumer(destination);

                consumer.Listener += message =>
                {
                    IMessageProducer producer = session.CreateProducer(outbound);
                    producer.Send(message);
                    producer.Close();
                };

                consumer.Close();
                session.Close();
                connection.Close();
            }
        }

        [Test]
        public void TestMessageListenerCallsConnectionCloseThrowsIllegalStateException()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                testAmqpPeer.SendMessage("myQueue", "test");

                NmsConnection connection = (NmsConnection) EstablishConnection();
                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageConsumer consumer = session.CreateConsumer(destination);

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
                };

                Assert.That(() => exception, Is.Not.Null.After(5000, 100));
                consumer.Close();
                session.Close();
                connection.Close();
            }
        }

        [Test]
        public void TestMessageListenerCallsConnectionStopThrowsIllegalStateException()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                testAmqpPeer.SendMessage("myQueue", "test");

                NmsConnection connection = (NmsConnection) EstablishConnection();
                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageConsumer consumer = session.CreateConsumer(destination);

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
                };

                Assert.That(() => exception, Is.Not.Null.After(5000, 100));
                consumer.Close();
                session.Close();
                connection.Close();
            }
        }

        [Test]
        public void TestMessageListenerCallsSessionCloseThrowsIllegalStateException()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                testAmqpPeer.SendMessage("myQueue", "test");

                NmsConnection connection = (NmsConnection) EstablishConnection();
                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageConsumer consumer = session.CreateConsumer(destination);

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
                };

                Assert.That(() => exception, Is.Not.Null.After(5000, 100));
                consumer.Close();
                session.Close();
                connection.Close();
            }
        }

        [Test, Timeout(2000)]
        public void TestMessageListenerClosesItsConsumer()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                TestLinkProcessor testLinkProcessor = new TestLinkProcessor();
                testAmqpPeer.RegisterLinkProcessor(testLinkProcessor);
                testAmqpPeer.SendMessage("myQueue", "test");

                NmsConnection connection = (NmsConnection) EstablishConnection();
                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageConsumer consumer = session.CreateConsumer(destination);

                ManualResetEvent latch = new ManualResetEvent(false);
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

                Assert.True(latch.WaitOne(TimeSpan.FromMilliseconds(1000)));
                Assert.That(() => testLinkProcessor.Consumer, Is.Null.After(1000, 50));
                consumer.Close();
                session.Close();
                connection.Close();
            }
        }

        [Test]
        public void TestRecoverOrderingWithAsyncConsumer()
        {
            int recoverCount = 5;
            int messageCount = 8;

            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                TestLinkProcessor testLinkProcessor = new TestLinkProcessor();
                testAmqpPeer.RegisterLinkProcessor(testLinkProcessor);

                for (int i = 0; i < messageCount; i++)
                {
                    testAmqpPeer.SendMessage("myQueue", i.ToString());
                }

                ManualResetEvent latch = new ManualResetEvent(false);
                IConnection connection = EstablishConnection();
                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.ClientAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageConsumer consumer = session.CreateConsumer(destination);

                bool complete = false;
                int messageSeen = 0;
                int expectedIndex = 0;
                consumer.Listener += message =>
                {
                    if (complete)
                    {
                        return;
                    }

                    int actualIndex = int.Parse((message as ITextMessage).Text);
                    Assert.AreEqual(expectedIndex, actualIndex);

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

                        message.Acknowledge();

                        if (expectedIndex == messageCount)
                        {
                            complete = true;
                            latch.Set();
                        }
                    }
                };


                consumer.Close();
                session.Close();
                connection.Close();
            }
        }

        [Test]
        public void TestConsumerCloseWaitsForAsyncDeliveryToComplete()
        {
            ManualResetEvent latch = new ManualResetEvent(false);

            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                testAmqpPeer.RegisterLinkProcessor(new TestLinkProcessor());
                testAmqpPeer.SendMessage("myQueue", "test");

                IConnection connection = EstablishConnection();
                connection.Start();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageConsumer consumer = session.CreateConsumer(destination);

                consumer.Listener += _ =>
                {
                    latch.Set();
                    Task.Delay(TimeSpan.FromMilliseconds(1200)).GetAwaiter().GetResult();
                };

                Assert.True(latch.WaitOne(TimeSpan.FromMilliseconds(30000)), "Messages not received within given timeout.");

                Task delay = Task.Delay(TimeSpan.FromMilliseconds(1000));
                Task closeTask = Task.Run(() => consumer.Close());

                Task resultTask = Task.WhenAny(delay, closeTask).GetAwaiter().GetResult();

                Assert.AreEqual(delay, resultTask, "Consumer was closed before callback returned.");

                // make sure that consumer was closed
                closeTask.GetAwaiter().GetResult();

                session.Close();
                connection.Close();
            }
        }

        [Test]
        public void TestSessionCloseWaitsForAsyncDeliveryToComplete()
        {
            ManualResetEvent latch = new ManualResetEvent(false);

            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                testAmqpPeer.RegisterLinkProcessor(new TestLinkProcessor());
                testAmqpPeer.SendMessage("myQueue", "test");

                IConnection connection = EstablishConnection();
                connection.Start();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageConsumer consumer = session.CreateConsumer(destination);

                consumer.Listener += _ =>
                {
                    latch.Set();
                    Task.Delay(TimeSpan.FromMilliseconds(1200)).GetAwaiter().GetResult();
                };

                Assert.True(latch.WaitOne(TimeSpan.FromMilliseconds(30000)), "Messages not received within given timeout.");

                Task delay = Task.Delay(TimeSpan.FromMilliseconds(1000));
                Task closeTask = Task.Run(() => session.Close());

                Task resultTask = Task.WhenAny(delay, closeTask).GetAwaiter().GetResult();

                Assert.AreEqual(delay, resultTask, "Consumer was closed before callback returned.");

                // make sure that consumer was closed
                closeTask.GetAwaiter().GetResult();

                connection.Close();
            }
        }

        [Test]
        public void TestConnectionCloseWaitsForAsyncDeliveryToComplete()
        {
            ManualResetEvent latch = new ManualResetEvent(false);

            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                testAmqpPeer.RegisterLinkProcessor(new TestLinkProcessor());
                testAmqpPeer.SendMessage("myQueue", "test");

                IConnection connection = EstablishConnection();
                connection.Start();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageConsumer consumer = session.CreateConsumer(destination);

                consumer.Listener += _ =>
                {
                    latch.Set();
                    Task.Delay(TimeSpan.FromMilliseconds(1200)).GetAwaiter().GetResult();
                };

                Assert.True(latch.WaitOne(TimeSpan.FromMilliseconds(30000)), "Messages not received within given timeout.");

                Task delay = Task.Delay(TimeSpan.FromMilliseconds(1000));
                Task closeTask = Task.Run(() => connection.Close());

                Task resultTask = Task.WhenAny(delay, closeTask).GetAwaiter().GetResult();

                Assert.AreEqual(delay, resultTask, "Consumer was closed before callback returned.");

                // make sure that consumer was closed
                closeTask.GetAwaiter().GetResult();
            }
        }

        [Test]
        public void TestRecoveredMessageShouldNotBeMutated()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                string originalPayload = "testMessage";
                
                testAmqpPeer.Open();
                testAmqpPeer.RegisterLinkProcessor(new TestLinkProcessor());
                testAmqpPeer.SendMessage("myQueue", originalPayload);

                NmsConnection connection = (NmsConnection) EstablishConnection();
                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.ClientAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageConsumer consumer = session.CreateConsumer(destination);

                NmsTextMessage message = consumer.Receive() as NmsTextMessage;
                Assert.NotNull(message);
                message.IsReadOnlyBody = false;
                message.Text = message.Text + "Received";
                
                session.Recover();

                ITextMessage recoveredMessage = consumer.Receive() as ITextMessage;
                Assert.IsNotNull(recoveredMessage);
                Assert.AreNotEqual(message.Text, recoveredMessage.Text);
                Assert.AreEqual(originalPayload, recoveredMessage.Text);
                Assert.AreNotSame(message, recoveredMessage);

                consumer.Close();
                session.Close();
                connection.Close();
            }
        }

        private IConnection EstablishConnection()
        {
            NmsConnectionFactory factory = new NmsConnectionFactory(Address);
            return factory.CreateConnection(User, Password);
        }
    }
}