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
using System.Diagnostics;
using System.Threading;
using Amqp.Framing;
using Amqp.Types;
using Apache.NMS;
using Apache.NMS.AMQP;
using Moq;
using NMS.AMQP.Test.TestAmqp;
using NUnit.Framework;

namespace NMS.AMQP.Test.Integration
{
    [TestFixture]
    public class FailoverIntegrationTest
    {
        private static readonly string User = "USER";
        private static readonly string Password = "PASSWORD";
        private static readonly string Address1 = "amqp://127.0.0.1:5672";
        private static readonly string Address2 = "amqp://127.0.0.1:5673";
        private static readonly string Address3 = "amqp://127.0.0.1:5674";
        private static readonly string Address4 = "amqp://127.0.0.1:5675";

        [Test, Timeout(20000)]
        public void TestFailoverHandlesDropThenRejectionCloseAfterConnect()
        {
            using (TestAmqpPeer originalPeer = new TestAmqpPeer(Address1, User, Password))
            using (TestAmqpPeer rejectingPeer = new TestAmqpPeer(Address2, User, Password))
            using (TestAmqpPeer finalPeer = new TestAmqpPeer(Address3, User, Password))
            {
                ManualResetEvent originalConnected = new ManualResetEvent(false);
                ManualResetEvent finalConnected = new ManualResetEvent(false);

                // Create a peer to connect to, one to fail to reconnect to, and a final one to reconnect to

                originalPeer.Open();
                finalPeer.Open();

                long ird = 0;
                long rd = 2000;

                NmsConnection connection = EstablishConnection("failover.initialReconnectDelay=" + ird + "&failover.reconnectDelay=" + rd + "&failover.maxReconnectAttempts=10", originalPeer, rejectingPeer, finalPeer);

                Mock<INmsConnectionListener> connectionListener = new Mock<INmsConnectionListener>();

                connectionListener
                    .Setup(listener => listener.OnConnectionEstablished(It.Is<Uri>(uri => originalPeer.Address == uri)))
                    .Callback(() => { originalConnected.Set(); });

                connectionListener
                    .Setup(listener => listener.OnConnectionRestored(It.Is<Uri>(uri => finalPeer.Address == uri)))
                    .Callback(() => { finalConnected.Set(); });

                connection.AddConnectionListener(connectionListener.Object);

                connection.Start();

                Assert.True(originalConnected.WaitOne(TimeSpan.FromSeconds(5)), "Should connect to original peer");
                Assert.False(finalConnected.WaitOne(TimeSpan.FromMilliseconds(100)), "Should not yet have connected to final peer");

                // Close the original peer and wait for things to shake out.
                originalPeer.Close();

                Assert.True(finalConnected.WaitOne(TimeSpan.FromSeconds(5)), "Should connect to final peer");

                connection.Close();
            }
        }

        [Test, Timeout(20000)]
        public void TestFailoverHandlesDropWithModifiedInitialReconnectDelay()
        {
            using (TestAmqpPeer originalPeer = new TestAmqpPeer(Address1, User, Password))
            using (TestAmqpPeer finalPeer = new TestAmqpPeer(Address3, User, Password))
            {
                ManualResetEvent originalConnected = new ManualResetEvent(false);
                ManualResetEvent finalConnected = new ManualResetEvent(false);

                // Create a peer to connect to, then one to reconnect to
                originalPeer.Open();
                finalPeer.Open();

                NmsConnection connection = EstablishConnection("failover.initialReconnectDelay=" + 1 + "&failover.reconnectDelay=" + 600 + "&failover.maxReconnectAttempts=10", originalPeer, finalPeer);

                Mock<INmsConnectionListener> connectionListener = new Mock<INmsConnectionListener>();

                connectionListener
                    .Setup(listener => listener.OnConnectionEstablished(It.Is<Uri>(uri => originalPeer.Address == uri)))
                    .Callback(() => { originalConnected.Set(); });

                connectionListener
                    .Setup(listener => listener.OnConnectionRestored(It.Is<Uri>(uri => finalPeer.Address == uri)))
                    .Callback(() => { finalConnected.Set(); });

                connection.AddConnectionListener(connectionListener.Object);

                connection.Start();

                Assert.True(originalConnected.WaitOne(TimeSpan.FromSeconds(5)), "Should connect to original peer");

                // Close the original peer
                originalPeer.Close();

                connection.CreateSession(AcknowledgementMode.AutoAcknowledge);

                Assert.True(finalConnected.WaitOne(TimeSpan.FromSeconds(5)), "Should connect to final peer");

                connection.Close();
            }
        }

        [Test, Timeout(20000)]
        public void TestFailoverInitialReconnectDelayDoesNotApplyToInitialConnect()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer(Address1, User, Password))
            {
                testPeer.Open();

                int delay = 20000;

                Stopwatch watch = new Stopwatch();
                watch.Start();
                NmsConnection connection = EstablishConnection("failover.initialReconnectDelay=" + delay + "&failover.maxReconnectAttempts=1", testPeer);
                connection.Start();
                watch.Stop();

                Assert.True(watch.ElapsedMilliseconds < delay,
                    "Initial connect should not have delayed for the specified initialReconnectDelay." + "Elapsed=" + watch.ElapsedMilliseconds + ", delay=" + delay);

                connection.Close();
            }
        }

        [Test]
        public void TestFailoverHandlesDropAfterSessionCloseRequested()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer(Address1, User, Password))
            {
                ManualResetEvent connected = new ManualResetEvent(false);

                testPeer.RegisterLinkProcessor(new MockLinkProcessor(context => context.Complete(new Error(new Symbol("error")))));
                testPeer.Open();

                NmsConnection connection = EstablishConnection(testPeer);

                Mock<INmsConnectionListener> connectionListener = new Mock<INmsConnectionListener>();

                connectionListener
                    .Setup(listener => listener.OnConnectionEstablished(It.IsAny<Uri>()))
                    .Callback(() => { connected.Set(); });

                connection.AddConnectionListener(connectionListener.Object);

                connection.Start();

                Assert.True(connected.WaitOne(TimeSpan.FromSeconds(5)), "Should connect to peer");

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);

                testPeer.Close();

                try
                {
                    session.Close();
                }
                catch (NMSException)
                {
                }
                catch (Exception)
                {
                    Assert.Fail("Session close should have completed normally.");
                }

                connection.Close();
            }
        }

        [Test, Ignore("It won't pass because amqp lite first accepts attach, and then fires detach error. Underlying async task" +
                      "is already resolved and we cannot cancel it.")]
        public void TestCreateConsumerFailsWhenLinkRefused()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer(Address1, User, Password))
            {
                testPeer.RegisterLinkProcessor(new MockLinkProcessor(context => context.Complete(new Error(new Symbol("error")))));
                testPeer.Open();

                NmsConnection connection = EstablishConnection(testPeer);
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                ITopic topic = session.GetTopic("myTopic");

                Assert.Catch<NMSException>(() => session.CreateConsumer(topic));
                
                connection.Close();
            }
        }

        [Test, Timeout(2000)]
        public void TestFailoverEnforcesRequestTimeoutSession()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer(Address1, User, Password))
            {
                ManualResetEvent connected = new ManualResetEvent(false);
                ManualResetEvent disconnected = new ManualResetEvent(false);

                testPeer.RegisterLinkProcessor(new TestLinkProcessor());

                testPeer.Open();

                NmsConnection connection = EstablishConnection("nms.requestTimeout=1000&failover.reconnectDelay=2000&failover.maxReconnectAttempts=60", testPeer);

                Mock<INmsConnectionListener> connectionListener = new Mock<INmsConnectionListener>();

                connectionListener
                    .Setup(listener => listener.OnConnectionEstablished(It.IsAny<Uri>()))
                    .Callback(() => { connected.Set(); });

                connectionListener
                    .Setup(listener => listener.OnConnectionInterrupted(It.IsAny<Uri>()))
                    .Callback(() => { disconnected.Set(); });

                connection.AddConnectionListener(connectionListener.Object);

                connection.Start();

                Assert.True(connected.WaitOne(TimeSpan.FromSeconds(5)), "Should connect to peer");

                testPeer.Close();

                Assert.True(disconnected.WaitOne(TimeSpan.FromSeconds(5)), "Should lose connection to peer");

                Assert.Catch<NMSException>(() => connection.CreateSession(AcknowledgementMode.AutoAcknowledge));
                connection.Close();
            }
        }

        [Test, Timeout(2000)]
        public void TestFailoverEnforcesSendTimeout()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer(Address1, User, Password))
            {
                ManualResetEvent connected = new ManualResetEvent(false);
                ManualResetEvent disconnected = new ManualResetEvent(false);

                testPeer.RegisterLinkProcessor(new TestLinkProcessor());

                testPeer.Open();

                NmsConnection connection = EstablishConnection("nms.sendTimeout=1000&failover.reconnectDelay=2000&failover.maxReconnectAttempts=60", testPeer);

                Mock <INmsConnectionListener> connectionListener = new Mock<INmsConnectionListener>();

                connectionListener
                    .Setup(listener => listener.OnConnectionEstablished(It.IsAny<Uri>()))
                    .Callback(() => { connected.Set(); });

                connectionListener
                    .Setup(listener => listener.OnConnectionInterrupted(It.IsAny<Uri>()))
                    .Callback(() => { disconnected.Set(); });

                connection.AddConnectionListener(connectionListener.Object);

                connection.Start();

                Assert.True(connected.WaitOne(TimeSpan.FromSeconds(5)), "Should connect to peer");

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(queue);

                testPeer.Close();

                Assert.True(disconnected.WaitOne(TimeSpan.FromSeconds(5)), "Should lose connection to peer");

                Assert.Catch<NMSException>(() => producer.Send(producer.CreateTextMessage("test")));

                connection.Close();
            }
        }

        [Test]
        public void TestFailoverPassthroughOfCompletedSyncSend()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer(Address1, User, Password))
            {
                int messagesReceived = 0;
                testPeer.RegisterMessageProcessor("myQueue", context =>
                {
                    messagesReceived++;
                    context.Complete();
                });

                testPeer.Open();

                NmsConnection connection = EstablishConnection(testPeer);
                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(queue);

                //Do a warmup that succeeds
                producer.Send(producer.CreateTextMessage("first"));
                producer.Send(producer.CreateTextMessage("second"));

                Assert.That(() => messagesReceived, Is.EqualTo(2).After(1000, 100));

                connection.Close();
            }
        }

        [Test]
        public void TestFailoverPassthroughOfRejectedSyncSend()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer(Address1, User, Password))
            {
                int counter = 1;
                testPeer.RegisterMessageProcessor("myQueue", context =>
                {
                    switch (counter)
                    {
                        // accept first and third
                        case 1:
                        case 3:
                            context.Complete();
                            break;
                        // fail second
                        case 2:
                            context.Complete(new Error(new Symbol("error")));
                            break;
                    }

                    counter++;
                });

                testPeer.Open();

                NmsConnection connection = EstablishConnection(testPeer);
                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(queue);

                //Do a warmup that succeeds
                producer.Send(producer.CreateTextMessage("first"));

                Assert.Catch(() =>
                {
                    producer.Send(producer.CreateTextMessage("second"));
                });

                //Do a final send that succeeds
                producer.Send(producer.CreateTextMessage("third"));

                connection.Close();
            }
        }

        [Test]
        public void TestCreateSessionAfterConnectionDrops()
        {
            using (TestAmqpPeer originalPeer = new TestAmqpPeer(Address1, User, Password))
            using (TestAmqpPeer finalPeer = new TestAmqpPeer(Address2, User, Password))
            {
                originalPeer.RegisterLinkProcessor(new TestLinkProcessor());
                finalPeer.RegisterLinkProcessor(new TestLinkProcessor());

                ManualResetEvent originalConnected = new ManualResetEvent(false);
                ManualResetEvent finalConnected = new ManualResetEvent(false);

                // Create a peer to connect to, then one to reconnect to
                originalPeer.Open();
                finalPeer.Open();

                NmsConnection connection = EstablishConnection(originalPeer, finalPeer);

                Mock<INmsConnectionListener> connectionListener = new Mock<INmsConnectionListener>();

                connectionListener
                    .Setup(listener => listener.OnConnectionEstablished(It.Is<Uri>(uri => originalPeer.Address == uri)))
                    .Callback(() => { originalConnected.Set(); });

                connectionListener
                    .Setup(listener => listener.OnConnectionRestored(It.Is<Uri>(uri => finalPeer.Address == uri)))
                    .Callback(() => { finalConnected.Set(); });

                connection.AddConnectionListener(connectionListener.Object);

                connection.Start();

                Assert.True(originalConnected.WaitOne(TimeSpan.FromSeconds(5)), "Should connect to original peer");

                originalPeer.Close();

                ISession session = connection.CreateSession();

                Assert.True(finalConnected.WaitOne(TimeSpan.FromSeconds(5)), "Should connect to final peer");

                session.Close();
                connection.Close();
            }
        }

        [Test]
        public void TestCreateConsumerAfterConnectionDrops()
        {
            using (TestAmqpPeer originalPeer = new TestAmqpPeer(Address1, User, Password))
            using (TestAmqpPeer finalPeer = new TestAmqpPeer(Address2, User, Password))
            {
                originalPeer.RegisterLinkProcessor(new TestLinkProcessor());
                finalPeer.RegisterLinkProcessor(new TestLinkProcessor());

                ManualResetEvent originalConnected = new ManualResetEvent(false);
                ManualResetEvent finalConnected = new ManualResetEvent(false);

                // Create a peer to connect to, then one to reconnect to
                originalPeer.Open();
                finalPeer.Open();

                NmsConnection connection = EstablishConnection(originalPeer, finalPeer);

                Mock<INmsConnectionListener> connectionListener = new Mock<INmsConnectionListener>();

                connectionListener
                    .Setup(listener => listener.OnConnectionEstablished(It.Is<Uri>(uri => originalPeer.Address == uri)))
                    .Callback(() => { originalConnected.Set(); });

                connectionListener
                    .Setup(listener => listener.OnConnectionRestored(It.Is<Uri>(uri => finalPeer.Address == uri)))
                    .Callback(() => { finalConnected.Set(); });

                connection.AddConnectionListener(connectionListener.Object);

                connection.Start();

                Assert.True(originalConnected.WaitOne(TimeSpan.FromSeconds(5)), "Should connect to original peer");

                originalPeer.Close();

                ISession session = connection.CreateSession();
                IQueue queue = session.GetQueue("myQueue");
                IMessageConsumer consumer = session.CreateConsumer(queue);

                Assert.True(finalConnected.WaitOne(TimeSpan.FromSeconds(5)), "Should connect to final peer");

                consumer.Close();
                connection.Close();
            }
        }

        [Test]
        public void TestCreateProducerAfterConnectionDrops()
        {
            using (TestAmqpPeer originalPeer = new TestAmqpPeer(Address1, User, Password))
            using (TestAmqpPeer finalPeer = new TestAmqpPeer(Address2, User, Password))
            {
                originalPeer.RegisterLinkProcessor(new TestLinkProcessor());
                finalPeer.RegisterLinkProcessor(new TestLinkProcessor());

                ManualResetEvent originalConnected = new ManualResetEvent(false);
                ManualResetEvent finalConnected = new ManualResetEvent(false);

                // Create a peer to connect to, then one to reconnect to
                originalPeer.Open();
                finalPeer.Open();

                NmsConnection connection = EstablishConnection(originalPeer, finalPeer);

                Mock<INmsConnectionListener> connectionListener = new Mock<INmsConnectionListener>();

                connectionListener
                    .Setup(listener => listener.OnConnectionEstablished(It.Is<Uri>(uri => originalPeer.Address == uri)))
                    .Callback(() => { originalConnected.Set(); });

                connectionListener
                    .Setup(listener => listener.OnConnectionRestored(It.Is<Uri>(uri => finalPeer.Address == uri)))
                    .Callback(() => { finalConnected.Set(); });

                connection.AddConnectionListener(connectionListener.Object);

                connection.Start();

                Assert.True(originalConnected.WaitOne(TimeSpan.FromSeconds(5)), "Should connect to original peer");

                originalPeer.Close();

                ISession session = connection.CreateSession();
                IQueue queue = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(queue);

                Assert.True(finalConnected.WaitOne(TimeSpan.FromSeconds(5)), "Should connect to final peer");

                producer.Close();
                connection.Close();
            }
        }

        [Test, Ignore("Won't pass because we cannot connect to provider without establishing amqp lite connection," +
                      "as a result, next attempt is recorded after we try to connect to all available peers. It is implemented in qpid" +
                      "in the same way, but they are able to connect to provider and then reject the connection.")]
        public void TestStartMaxReconnectAttemptsTriggeredWhenRemotesAreRejecting()
        {
            using (TestAmqpPeer firstPeer = new TestAmqpPeer(Address1, User, Password))
            using (TestAmqpPeer secondPeer = new TestAmqpPeer(Address2, User, Password))
            using (TestAmqpPeer thirdPeer = new TestAmqpPeer(Address3, User, Password))
            using (TestAmqpPeer fourthPeer = new TestAmqpPeer(Address4, User, Password))
            {
                TestLinkProcessor linkProcessor = new TestLinkProcessor();
                fourthPeer.RegisterLinkProcessor(linkProcessor);
                fourthPeer.Open();

                ManualResetEvent failedConnection = new ManualResetEvent(false);

                NmsConnection connection = EstablishConnection(
                    "failover.startupMaxReconnectAttempts=3&failover.reconnectDelay=15&failover.useReconnectBackOff=false",
                    firstPeer, secondPeer, thirdPeer, fourthPeer);

                Mock<INmsConnectionListener> connectionListener = new Mock<INmsConnectionListener>();

                connectionListener
                    .Setup(listener => listener.OnConnectionFailure(It.IsAny<NMSException>()))
                    .Callback(() => { failedConnection.Set(); });

                connection.AddConnectionListener(connectionListener.Object);

                try
                {
                    connection.Start();
                    Assert.Fail("Should not be able to connect");
                }
                catch (Exception) { }

                Assert.True(failedConnection.WaitOne(TimeSpan.FromSeconds(5)));

                // Verify that no connection made to the last peer
                Assert.IsNull(linkProcessor.Consumer);
            }
        }

        [Test, Timeout(20000)]
        public void TestRemotelyCloseConsumerWithMessageListenerWithoutErrorFiresNMSExceptionListener()
        {
            DoRemotelyCloseConsumerWithMessageListenerFiresNMSExceptionListenerTestImpl(false);
        }

        [Test, Timeout(20000)]
        public void TestRemotelyCloseConsumerWithMessageListenerFiresNMSExceptionListener()
        {
            DoRemotelyCloseConsumerWithMessageListenerFiresNMSExceptionListenerTestImpl(true);
        }

        private void DoRemotelyCloseConsumerWithMessageListenerFiresNMSExceptionListenerTestImpl(bool closeWithError)
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer(Address1, User, Password))
            {
                ManualResetEvent consumerClosed = new ManualResetEvent(false);
                ManualResetEvent exceptionListenerFired = new ManualResetEvent(false);

                TestLinkProcessor linkProcessor = new TestLinkProcessor();
                testPeer.RegisterLinkProcessor(linkProcessor);
                testPeer.Open();

                NmsConnection connection = EstablishConnection("failover.maxReconnectAttempts=1", testPeer);
                connection.ExceptionListener += exception =>
                {
                    exceptionListenerFired.Set();
                };

                Mock<INmsConnectionListener> connectionListener = new Mock<INmsConnectionListener>();

                connectionListener
                    .Setup(listener => listener.OnConsumerClosed(It.IsAny<IMessageConsumer>(), It.IsAny<Exception>()))
                    .Callback(() => { consumerClosed.Set(); });

                connection.AddConnectionListener(connectionListener.Object);

                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");

                // Create a consumer, then remotely end it afterwards.
                IMessageConsumer consumer = session.CreateConsumer(queue);
                consumer.Listener += message => { };

                if (closeWithError)
                    linkProcessor.CloseConsumerWithError();
                else
                    linkProcessor.CloseConsumer();

                Assert.True(consumerClosed.WaitOne(TimeSpan.FromMilliseconds(2000)), "Consumer closed callback didn't trigger");
                Assert.True(exceptionListenerFired.WaitOne(TimeSpan.FromMilliseconds(2000)), "JMS Exception listener should have fired with a MessageListener");

                // Try closing it explicitly, should effectively no-op in client.
                // The test peer will throw during close if it sends anything.
                consumer.Close();

                connection.Close();
            }
        }

        [Test, Timeout(20000)]
        public void TestFailoverDoesNotFailPendingSend()
        {
            using (TestAmqpPeer originalPeer = new TestAmqpPeer(Address1, User, Password))
            using (TestAmqpPeer finalPeer = new TestAmqpPeer(Address3, User, Password))
            {
                ManualResetEvent messageReceived = new ManualResetEvent(false);

                finalPeer.RegisterMessageProcessor("q1", context =>
                {
                    messageReceived.Set();
                    context.Complete();
                });

                originalPeer.Open();
                finalPeer.Open();

                NmsConnection connection = EstablishConnection("failover.initialReconnectDelay=10000", finalPeer);

                connection.Start();
                ISession session = connection.CreateSession();
                IQueue queue = session.GetQueue("q1");
                IMessageProducer producer = session.CreateProducer(queue);

                originalPeer.Close();


                Assert.DoesNotThrow(() =>
                {
                    ITextMessage message = session.CreateTextMessage("test");
                    producer.Send(message);
                });

                Assert.True(messageReceived.WaitOne(TimeSpan.FromSeconds(5)), "Message should be delivered to final peer.");

                connection.Close();
            }
        }

        [Test, Timeout(20000)]
        public void TestTempDestinationRecreatedAfterConnectionFailsOver()
        {
            using (TestAmqpPeer originalPeer = new TestAmqpPeer(Address1, User, Password))
            using (TestAmqpPeer finalPeer = new TestAmqpPeer(Address2, User, Password))
            {
                originalPeer.RegisterLinkProcessor(new TestLinkProcessor());
                finalPeer.RegisterLinkProcessor(new TestLinkProcessor());

                ManualResetEvent originalConnected = new ManualResetEvent(false);
                ManualResetEvent finalConnected = new ManualResetEvent(false);

                // Create a peer to connect to, then one to reconnect to
                originalPeer.Open();
                finalPeer.Open();

                NmsConnection connection = EstablishConnection(originalPeer, finalPeer);

                Mock<INmsConnectionListener> connectionListener = new Mock<INmsConnectionListener>();

                connectionListener
                    .Setup(listener => listener.OnConnectionEstablished(It.Is<Uri>(uri => originalPeer.Address == uri)))
                    .Callback(() => { originalConnected.Set(); });

                connectionListener
                    .Setup(listener => listener.OnConnectionRestored(It.Is<Uri>(uri => finalPeer.Address == uri)))
                    .Callback(() => { finalConnected.Set(); });

                connection.AddConnectionListener(connectionListener.Object);

                connection.Start();

                Assert.True(originalConnected.WaitOne(TimeSpan.FromSeconds(5)), "Should connect to original peer");
                
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                ITemporaryTopic temporaryTopic = session.CreateTemporaryTopic();

                originalPeer.Close();

                Assert.True(finalConnected.WaitOne(TimeSpan.FromSeconds(5)), "Should connect to final peer");
                
                temporaryTopic.Delete();
                
                connection.Close();
            }
        }

        private NmsConnection EstablishConnection(params TestAmqpPeer[] peers)
        {
            return EstablishConnection(null, null, peers);
        }

        private NmsConnection EstablishConnection(string failoverParams, params TestAmqpPeer[] peers)
        {
            return EstablishConnection(null, failoverParams, peers);
        }

        private NmsConnection EstablishConnection(string connectionParams, string failoverParams, params TestAmqpPeer[] peers)
        {
            if (peers.Length == 0)
            {
                throw new ArgumentException("No test peers were given, at least 1 required");
            }

            string remoteUri = "failover:(";
            bool first = true;
            foreach (TestAmqpPeer peer in peers)
            {
                if (!first)
                {
                    remoteUri += ",";
                }
                remoteUri += CreatePeerUri(peer, connectionParams);
                first = false;
            }

            if (failoverParams == null)
            {
                remoteUri += ")?failover.maxReconnectAttempts=10";
            }
            else
            {
                remoteUri += ")?" + failoverParams;
            }

            NmsConnectionFactory factory = new NmsConnectionFactory(remoteUri);
            return (NmsConnection)factory.CreateConnection(User, Password);
        }

        private string CreatePeerUri(TestAmqpPeer peer, string parameters = null)
        {
            return peer.Address + (parameters != null ? "?" + parameters : "");
        }
    }
}