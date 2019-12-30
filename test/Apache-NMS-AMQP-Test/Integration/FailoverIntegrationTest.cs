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
using System.Threading.Tasks;
using Amqp.Framing;
using Amqp.Types;
using Apache.NMS;
using Apache.NMS.AMQP;
using Moq;
using NLog;
using NMS.AMQP.Test.TestAmqp;
using NMS.AMQP.Test.TestAmqp.BasicTypes;
using NUnit.Framework;

namespace NMS.AMQP.Test.Integration
{
    [TestFixture]
    public class FailoverIntegrationTest : IntegrationTestFixture
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();
        
        [Test, Timeout(20_000), Category("Windows")]
        public void TestFailoverHandlesDropThenRejectionCloseAfterConnect()
        {
            using (TestAmqpPeer originalPeer = new TestAmqpPeer())
            using (TestAmqpPeer rejectingPeer = new TestAmqpPeer())
            using (TestAmqpPeer finalPeer = new TestAmqpPeer())
            {
                ManualResetEvent originalConnected = new ManualResetEvent(false);
                ManualResetEvent finalConnected = new ManualResetEvent(false);

                // Create a peer to connect to, one to fail to reconnect to, and a final one to reconnect to
                var originalUri = CreatePeerUri(originalPeer);
                var rejectingUri = CreatePeerUri(rejectingPeer);
                var finalUri = CreatePeerUri(finalPeer);
                
                Logger.Info($"Original peer is at: {originalUri}");
                Logger.Info($"Rejecting peer is at: {rejectingUri}");
                Logger.Info($"Final peer is at: {finalUri}");

                // Connect to the first
                originalPeer.ExpectSaslAnonymous();
                originalPeer.ExpectOpen();
                originalPeer.ExpectBegin();

                long ird = 0;
                long rd = 2000;

                NmsConnection connection = EstablishAnonymousConnection("failover.initialReconnectDelay=" + ird + "&failover.reconnectDelay=" + rd + "&failover.maxReconnectAttempts=10", originalPeer,
                    rejectingPeer, finalPeer);

                Mock<INmsConnectionListener> connectionListener = new Mock<INmsConnectionListener>();

                connectionListener
                    .Setup(listener => listener.OnConnectionEstablished(It.Is<Uri>(uri => originalUri == uri.ToString())))
                    .Callback(() => { originalConnected.Set(); });

                connectionListener
                    .Setup(listener => listener.OnConnectionRestored(It.Is<Uri>(uri => finalUri == uri.ToString())))
                    .Callback(() => { finalConnected.Set(); });

                connection.AddConnectionListener(connectionListener.Object);

                connection.Start();

                Assert.True(originalConnected.WaitOne(TimeSpan.FromSeconds(5)), "Should connect to original peer");
                Assert.False(finalConnected.WaitOne(TimeSpan.FromMilliseconds(100)), "Should not yet have connected to final peer");

                // Set expectations on rejecting and final peer
                rejectingPeer.RejectConnect(AmqpError.NOT_FOUND, "Resource could not be located");

                finalPeer.ExpectSaslAnonymous();
                finalPeer.ExpectOpen();
                finalPeer.ExpectBegin();

                // Close the original peer and wait for things to shake out.
                originalPeer.Close(sendClose: true);

                rejectingPeer.WaitForAllMatchersToComplete(2000);

                Assert.True(finalConnected.WaitOne(TimeSpan.FromSeconds(10)), "Should connect to final peer");

                finalPeer.ExpectClose();
                connection.Close();

                finalPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestFailoverHandlesDropWithModifiedInitialReconnectDelay()
        {
            using (TestAmqpPeer originalPeer = new TestAmqpPeer())
            using (TestAmqpPeer finalPeer = new TestAmqpPeer())
            {
                ManualResetEvent originalConnected = new ManualResetEvent(false);
                ManualResetEvent finalConnected = new ManualResetEvent(false);

                // Create a peer to connect to, then one to reconnect to
                var originalUri = CreatePeerUri(originalPeer);
                var finalUri = CreatePeerUri(finalPeer);

                // Connect to the first peer
                originalPeer.ExpectSaslAnonymous();
                originalPeer.ExpectOpen();
                originalPeer.ExpectBegin();
                originalPeer.ExpectBegin();
                originalPeer.DropAfterLastMatcher();

                NmsConnection connection = EstablishAnonymousConnection("failover.initialReconnectDelay=1&failover.reconnectDelay=600&failover.maxReconnectAttempts=10", originalPeer, finalPeer);
                Mock<INmsConnectionListener> connectionListener = new Mock<INmsConnectionListener>();

                connectionListener
                    .Setup(listener => listener.OnConnectionEstablished(It.Is<Uri>(uri => originalUri == uri.ToString())))
                    .Callback(() => { originalConnected.Set(); });

                connectionListener
                    .Setup(listener => listener.OnConnectionRestored(It.Is<Uri>(uri => finalUri == uri.ToString())))
                    .Callback(() => { finalConnected.Set(); });

                connection.AddConnectionListener(connectionListener.Object);

                connection.Start();

                Assert.True(originalConnected.WaitOne(TimeSpan.FromSeconds(5)), "Should connect to original peer");

                // Post Failover Expectations of FinalPeer
                finalPeer.ExpectSaslAnonymous();
                finalPeer.ExpectOpen();
                finalPeer.ExpectBegin();
                finalPeer.ExpectBegin();

                connection.CreateSession(AcknowledgementMode.AutoAcknowledge);

                Assert.True(finalConnected.WaitOne(TimeSpan.FromSeconds(5)), "Should connect to final peer");

                // Shut it down
                finalPeer.ExpectClose();
                connection.Close();

                finalPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestFailoverInitialReconnectDelayDoesNotApplyToInitialConnect()
        {
            using (TestAmqpPeer originalPeer = new TestAmqpPeer())
            {
                // Connect to the first peer
                originalPeer.ExpectSaslAnonymous();
                originalPeer.ExpectOpen();
                originalPeer.ExpectBegin();

                int delay = 20000;
                Stopwatch watch = new Stopwatch();
                watch.Start();

                NmsConnection connection = EstablishAnonymousConnection("failover.initialReconnectDelay=" + delay + "&failover.maxReconnectAttempts=1", originalPeer);
                connection.Start();

                watch.Stop();

                Assert.True(watch.ElapsedMilliseconds < delay,
                    "Initial connect should not have delayed for the specified initialReconnectDelay." + "Elapsed=" + watch.ElapsedMilliseconds + ", delay=" + delay);
                Assert.True(watch.ElapsedMilliseconds < 5000, $"Connection took longer than reasonable: {watch.ElapsedMilliseconds}");

                // Shut it down
                originalPeer.ExpectClose();
                connection.Close();

                originalPeer.WaitForAllMatchersToComplete(2000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestFailoverHandlesDropAfterSessionCloseRequested()
        {
            using (TestAmqpPeer originalPeer = new TestAmqpPeer())
            {
                ManualResetEvent originalConnected = new ManualResetEvent(false);

                // Create a peer to connect to
                var originalUri = CreatePeerUri(originalPeer);

                // Connect to the first peer
                originalPeer.ExpectSaslAnonymous();
                originalPeer.ExpectOpen();
                originalPeer.ExpectBegin();

                Mock<INmsConnectionListener> connectionListener = new Mock<INmsConnectionListener>();

                connectionListener
                    .Setup(listener => listener.OnConnectionEstablished(It.Is<Uri>(uri => originalUri == uri.ToString())))
                    .Callback(() => { originalConnected.Set(); });

                NmsConnection connection = EstablishAnonymousConnection(originalPeer);
                connection.AddConnectionListener(connectionListener.Object);

                connection.Start();

                Assert.True(originalConnected.WaitOne(TimeSpan.FromSeconds(5)), "Should connect to peer");

                originalPeer.ExpectBegin();
                originalPeer.ExpectEnd(sendResponse: false);
                originalPeer.DropAfterLastMatcher();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);

                ManualResetEvent sessionCloseCompleted = new ManualResetEvent(false);
                Exception sessionClosedThrew = null;

                Task.Run(() =>
                {
                    try
                    {
                        session.Close();
                    }
                    catch (Exception e)
                    {
                        sessionClosedThrew = e;
                    }
                    finally
                    {
                        sessionCloseCompleted.Set();
                    }
                });

                originalPeer.WaitForAllMatchersToComplete(2000);

                Assert.IsTrue(sessionCloseCompleted.WaitOne(TimeSpan.FromSeconds(3)), "Session close should have completed by now");
                Assert.IsNull(sessionClosedThrew, "Session close should have completed normally");

                connection.Close();
            }
        }

        [Test, Timeout(20_000)]
        public void TestCreateConsumerFailsWhenLinkRefused()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                testPeer.ExpectSaslAnonymous();
                testPeer.ExpectOpen();
                testPeer.ExpectBegin();

                NmsConnection connection = EstablishAnonymousConnection(testPeer);
                connection.Start();

                testPeer.ExpectBegin();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);

                string topicName = "myTopic";
                ITopic topic = session.GetTopic(topicName);

                // Expect a link to a topic node, which we will then refuse
                testPeer.ExpectReceiverAttach(sourceMatcher: source =>
                {
                    Assert.AreEqual(topicName, source.Address);
                    Assert.IsFalse(source.Dynamic);
                    Assert.AreEqual((uint) TerminusDurability.NONE, source.Durable);
                }, targetMatcher: Assert.NotNull, linkNameMatcher: Assert.NotNull, refuseLink: true);

                //Expect the detach response to the test peer closing the consumer link after refusal.
                testPeer.ExpectDetach(expectClosed: true, sendResponse: false, replyClosed: false);

                Assert.Catch<NMSException>(() => session.CreateConsumer(topic));

                // Shut it down
                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestFailoverEnforcesRequestTimeoutSession()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                ManualResetEvent connected = new ManualResetEvent(false);
                ManualResetEvent disconnected = new ManualResetEvent(false);

                // Connect to the test peer
                testPeer.ExpectSaslAnonymous();
                testPeer.ExpectOpen();
                testPeer.ExpectBegin();
                testPeer.DropAfterLastMatcher(delay: 10);

                NmsConnection connection = EstablishAnonymousConnection("nms.requestTimeout=1000&failover.reconnectDelay=2000&failover.maxReconnectAttempts=60", testPeer);

                Mock<INmsConnectionListener> connectionListener = new Mock<INmsConnectionListener>();

                connectionListener
                    .Setup(listener => listener.OnConnectionInterrupted(It.IsAny<Uri>()))
                    .Callback(() => { disconnected.Set(); });

                connectionListener
                    .Setup(listener => listener.OnConnectionEstablished(It.IsAny<Uri>()))
                    .Callback(() => { connected.Set(); });


                connection.AddConnectionListener(connectionListener.Object);

                connection.Start();

                Assert.True(connected.WaitOne(TimeSpan.FromSeconds(5)), "Should connect to peer");
                Assert.True(disconnected.WaitOne(TimeSpan.FromSeconds(5)), "Should lose connection to peer");

                Assert.Catch<NMSException>(() => connection.CreateSession(AcknowledgementMode.AutoAcknowledge));

                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestFailoverEnforcesSendTimeout()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                ManualResetEvent connected = new ManualResetEvent(false);
                ManualResetEvent disconnected = new ManualResetEvent(false);

                // Connect to the test peer
                testPeer.ExpectSaslAnonymous();
                testPeer.ExpectOpen();
                testPeer.ExpectBegin();
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();
                testPeer.DropAfterLastMatcher();

                NmsConnection connection = EstablishAnonymousConnection("nms.sendTimeout=1000&failover.reconnectDelay=2000&failover.maxReconnectAttempts=60", testPeer);

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

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(queue);

                Assert.True(disconnected.WaitOne(TimeSpan.FromSeconds(5)), "Should lose connection to peer");

                Assert.Catch<NMSException>(() => producer.Send(producer.CreateTextMessage("test")));

                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestFailoverPassthroughOfCompletedSyncSend()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                NmsConnection connection = EstablishAnonymousConnection((testPeer));

                testPeer.ExpectSaslAnonymous();
                testPeer.ExpectOpen();
                testPeer.ExpectBegin();
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(queue);

                // Do a warm up
                string messageContent1 = "myMessage1";
                testPeer.ExpectTransfer(messageMatcher: m => { Assert.AreEqual(messageContent1, (m.BodySection as AmqpValue).Value); });

                ITextMessage message1 = session.CreateTextMessage(messageContent1);
                producer.Send(message1);

                testPeer.WaitForAllMatchersToComplete(1000);

                // Create and send a new message, which is accepted
                String messageContent2 = "myMessage2";
                int delay = 15;
                testPeer.ExpectTransfer(messageMatcher: m => Assert.AreEqual(messageContent2, (m.BodySection as AmqpValue).Value),
                    settled: false,
                    sendResponseDisposition: true,
                    responseState: new Accepted(),
                    responseSettled: true,
                    stateMatcher: Assert.IsNull,
                    dispositionDelay: delay);
                testPeer.ExpectClose();

                ITextMessage message2 = session.CreateTextMessage(messageContent2);

                DateTime start = DateTime.UtcNow;
                producer.Send(message2);

                TimeSpan elapsed = DateTime.UtcNow - start;
                Assert.That(elapsed.TotalMilliseconds, Is.GreaterThanOrEqualTo(delay));

                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }


        [Test, Timeout(20_000)]
        public void TestFailoverPassthroughOfRejectedSyncSend()
        {
            DoFailoverPassthroughOfFailingSyncSendTestImpl(new Rejected());
        }

        [Test, Timeout(20_000)]
        public void TestFailoverPassthroughOfReleasedSyncSend()
        {
            DoFailoverPassthroughOfFailingSyncSendTestImpl(new Released());
        }

        [Test, Timeout(20_000), Ignore("TODO: It should be fixed.")]
        public void TestFailoverPassthroughOfModifiedFailedSyncSend()
        {
            var modified = new Modified()
            {
                DeliveryFailed = true
            };
            DoFailoverPassthroughOfFailingSyncSendTestImpl(modified);
        }

        private void DoFailoverPassthroughOfFailingSyncSendTestImpl(Outcome failingState)
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                NmsConnection connection = EstablishAnonymousConnection((testPeer));

                testPeer.ExpectSaslAnonymous();
                testPeer.ExpectOpen();
                testPeer.ExpectBegin();
                testPeer.ExpectBegin();
                testPeer.ExpectSenderAttach();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(queue);

                // Do a warm up that succeeds
                string messageContent1 = "myMessage1";
                testPeer.ExpectTransfer(messageMatcher: m => { Assert.AreEqual(messageContent1, (m.BodySection as AmqpValue).Value); });

                ITextMessage message1 = session.CreateTextMessage(messageContent1);
                producer.Send(message1);

                testPeer.WaitForAllMatchersToComplete(1000);

                // Create and send a new message, which fails as it is not accepted
                Assert.False(failingState is Accepted);

                String messageContent2 = "myMessage2";
                int delay = 15;
                testPeer.ExpectTransfer(messageMatcher: m => Assert.AreEqual(messageContent2, (m.BodySection as AmqpValue).Value),
                    settled: false,
                    sendResponseDisposition: true,
                    responseState: failingState,
                    responseSettled: true,
                    stateMatcher: Assert.IsNull,
                    dispositionDelay: delay);

                ITextMessage message2 = session.CreateTextMessage(messageContent2);

                DateTime start = DateTime.UtcNow;
                Assert.Catch(() => producer.Send(message2), "Expected an exception for this send.");

                testPeer.WaitForAllMatchersToComplete(1000);

                //Do a final send that succeeds
                string messageContent3 = "myMessage3";
                testPeer.ExpectTransfer(messageMatcher: m => { Assert.AreEqual(messageContent3, (m.BodySection as AmqpValue).Value); });

                ITextMessage message3 = session.CreateTextMessage(messageContent3);
                producer.Send(message3);

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestCreateSessionAfterConnectionDrops()
        {
            using (TestAmqpPeer originalPeer = new TestAmqpPeer())
            using (TestAmqpPeer finalPeer = new TestAmqpPeer())
            {
                ManualResetEvent originalConnected = new ManualResetEvent(false);
                ManualResetEvent finalConnected = new ManualResetEvent(false);

                // Create a peer to connect to, then one to reconnect to
                var originalUri = CreatePeerUri(originalPeer);
                var finalUri = CreatePeerUri(finalPeer);

                // Connect to the first peer
                originalPeer.ExpectSaslAnonymous();
                originalPeer.ExpectOpen();
                originalPeer.ExpectBegin();
                originalPeer.ExpectBegin(sendResponse: false);
                originalPeer.DropAfterLastMatcher();

                NmsConnection connection = EstablishAnonymousConnection(originalPeer, finalPeer);

                Mock<INmsConnectionListener> connectionListener = new Mock<INmsConnectionListener>();

                connectionListener
                    .Setup(listener => listener.OnConnectionEstablished(It.Is<Uri>(uri => originalUri == uri.ToString())))
                    .Callback(() => { originalConnected.Set(); });

                connectionListener
                    .Setup(listener => listener.OnConnectionRestored(It.Is<Uri>(uri => finalUri == uri.ToString())))
                    .Callback(() => { finalConnected.Set(); });

                connection.AddConnectionListener(connectionListener.Object);

                connection.Start();

                Assert.True(originalConnected.WaitOne(TimeSpan.FromSeconds(5)), "Should connect to original peer");

                // Post Failover Expectations of FinalPeer

                finalPeer.ExpectSaslAnonymous();
                finalPeer.ExpectOpen();
                finalPeer.ExpectBegin();
                finalPeer.ExpectBegin();
                finalPeer.ExpectEnd();
                finalPeer.ExpectClose();

                ISession session = connection.CreateSession();

                Assert.True(finalConnected.WaitOne(TimeSpan.FromSeconds(5)), "Should connect to final peer");

                session.Close();
                connection.Close();

                finalPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestCreateConsumerAfterConnectionDrops()
        {
            using (TestAmqpPeer originalPeer = new TestAmqpPeer())
            using (TestAmqpPeer finalPeer = new TestAmqpPeer())
            {
                ManualResetEvent originalConnected = new ManualResetEvent(false);
                ManualResetEvent finalConnected = new ManualResetEvent(false);

                // Create a peer to connect to, then one to reconnect to
                var originalUri = CreatePeerUri(originalPeer);
                var finalUri = CreatePeerUri(finalPeer);

                // Connect to the first peer
                originalPeer.ExpectSaslAnonymous();
                originalPeer.ExpectOpen();
                originalPeer.ExpectBegin();
                originalPeer.ExpectBegin();
                originalPeer.DropAfterLastMatcher();

                NmsConnection connection = EstablishAnonymousConnection(originalPeer, finalPeer);

                Mock<INmsConnectionListener> connectionListener = new Mock<INmsConnectionListener>();

                connectionListener
                    .Setup(listener => listener.OnConnectionEstablished(It.Is<Uri>(uri => originalUri == uri.ToString())))
                    .Callback(() => { originalConnected.Set(); });

                connectionListener
                    .Setup(listener => listener.OnConnectionRestored(It.Is<Uri>(uri => finalUri == uri.ToString())))
                    .Callback(() => { finalConnected.Set(); });

                connection.AddConnectionListener(connectionListener.Object);

                connection.Start();

                Assert.True(originalConnected.WaitOne(TimeSpan.FromSeconds(5)), "Should connect to original peer");

                // Post Failover Expectations of FinalPeer
                finalPeer.ExpectSaslAnonymous();
                finalPeer.ExpectOpen();
                finalPeer.ExpectBegin();
                finalPeer.ExpectBegin();
                finalPeer.ExpectReceiverAttach();
                finalPeer.ExpectLinkFlow(drain: false, sendDrainFlowResponse: false, creditMatcher: credit => Assert.AreEqual(credit, 200));
                finalPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);
                finalPeer.ExpectClose();

                ISession session = connection.CreateSession();
                IQueue queue = session.GetQueue("myQueue");
                IMessageConsumer consumer = session.CreateConsumer(queue);

                Assert.IsNull(consumer.Receive(TimeSpan.FromMilliseconds(500)));

                Assert.True(finalConnected.WaitOne(TimeSpan.FromSeconds(5)), "Should connect to final peer");

                consumer.Close();

                // Shut it down
                connection.Close();

                finalPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestCreateProducerAfterConnectionDrops()
        {
            using (TestAmqpPeer originalPeer = new TestAmqpPeer())
            using (TestAmqpPeer finalPeer = new TestAmqpPeer())
            {
                ManualResetEvent originalConnected = new ManualResetEvent(false);
                ManualResetEvent finalConnected = new ManualResetEvent(false);

                // Create a peer to connect to, then one to reconnect to
                var originalUri = CreatePeerUri(originalPeer);
                var finalUri = CreatePeerUri(finalPeer);

                // Connect to the first peer
                originalPeer.ExpectSaslAnonymous();
                originalPeer.ExpectOpen();
                originalPeer.ExpectBegin();
                originalPeer.ExpectBegin();
                originalPeer.DropAfterLastMatcher();

                NmsConnection connection = EstablishAnonymousConnection(originalPeer, finalPeer);

                Mock<INmsConnectionListener> connectionListener = new Mock<INmsConnectionListener>();

                connectionListener
                    .Setup(listener => listener.OnConnectionEstablished(It.Is<Uri>(uri => originalUri == uri.ToString())))
                    .Callback(() => { originalConnected.Set(); });

                connectionListener
                    .Setup(listener => listener.OnConnectionRestored(It.Is<Uri>(uri => finalUri == uri.ToString())))
                    .Callback(() => { finalConnected.Set(); });

                connection.AddConnectionListener(connectionListener.Object);

                connection.Start();

                Assert.True(originalConnected.WaitOne(TimeSpan.FromSeconds(5)), "Should connect to original peer");

                // Post Failover Expectations of FinalPeer
                finalPeer.ExpectSaslAnonymous();
                finalPeer.ExpectOpen();
                finalPeer.ExpectBegin();
                finalPeer.ExpectBegin();
                finalPeer.ExpectSenderAttach();
                finalPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);
                finalPeer.ExpectClose();

                ISession session = connection.CreateSession();
                IQueue queue = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(queue);

                Assert.True(finalConnected.WaitOne(TimeSpan.FromSeconds(5)), "Should connect to final peer");

                producer.Close();

                connection.Close();

                finalPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000), Ignore("TODO: Fix")]
        public void TestStartMaxReconnectAttemptsTriggeredWhenRemotesAreRejecting()
        {
            using (TestAmqpPeer firstPeer = new TestAmqpPeer())
            using (TestAmqpPeer secondPeer = new TestAmqpPeer())
            using (TestAmqpPeer thirdPeer = new TestAmqpPeer())
            using (TestAmqpPeer fourthPeer = new TestAmqpPeer())
            {
                ManualResetEvent failedConnection = new ManualResetEvent(false);

                firstPeer.RejectConnect(AmqpError.NOT_FOUND, "Resource could not be located");
                secondPeer.RejectConnect(AmqpError.NOT_FOUND, "Resource could not be located");
                thirdPeer.RejectConnect(AmqpError.NOT_FOUND, "Resource could not be located");

                // This shouldn't get hit, but if it does accept the connect so we don't pass the failed
                // to connect assertion.
                fourthPeer.ExpectSaslAnonymous();
                fourthPeer.ExpectOpen();
                fourthPeer.ExpectBegin();
                fourthPeer.ExpectClose();

                NmsConnection connection = EstablishAnonymousConnection("failover.startupMaxReconnectAttempts=3&failover.reconnectDelay=15&failover.useReconnectBackOff=false",
                    firstPeer, secondPeer, thirdPeer, fourthPeer);

                Mock<INmsConnectionListener> connectionListener = new Mock<INmsConnectionListener>();

                connectionListener
                    .Setup(listener => listener.OnConnectionFailure(It.IsAny<NMSException>()))
                    .Callback(() => { failedConnection.Set(); });

                connection.AddConnectionListener(connectionListener.Object);

                Assert.Catch<NMSException>(() => connection.Start(), "Should not be able to connect");

                Assert.True(failedConnection.WaitOne(TimeSpan.FromSeconds(5)));

                try
                {
                    connection.Close();
                }
                catch (NMSException e)
                {
                }
                
                firstPeer.WaitForAllMatchersToComplete(2000);
                secondPeer.WaitForAllMatchersToComplete(2000);
                thirdPeer.WaitForAllMatchersToComplete(2000);
                
                // Shut down last peer and verify no connection made to it
                fourthPeer.PurgeExpectations();
                fourthPeer.Close();
                Assert.NotNull(firstPeer.ClientSocket, "Peer 1 should have accepted a TCP connection");
                Assert.NotNull(secondPeer.ClientSocket, "Peer 2 should have accepted a TCP connection");
                Assert.NotNull(thirdPeer.ClientSocket, "Peer 3 should have accepted a TCP connection");
                Assert.IsNull(fourthPeer.ClientSocket, "Peer 4 should not have accepted any TCP connection");
            }
        }

        [Test, Timeout(20_000)]
        public void TestRemotelyCloseConsumerWithMessageListenerFiresNMSExceptionListener()
        {
            Symbol errorCondition = AmqpError.RESOURCE_DELETED;
            string errorDescription = nameof(TestRemotelyCloseConsumerWithMessageListenerFiresNMSExceptionListener);
            
            DoRemotelyCloseConsumerWithMessageListenerFiresNMSExceptionListenerTestImpl(errorCondition, errorDescription);
        }

        [Test, Timeout(20_000)]
        public void TestRemotelyCloseConsumerWithMessageListenerWithoutErrorFiresNMSExceptionListener()
        {
            DoRemotelyCloseConsumerWithMessageListenerFiresNMSExceptionListenerTestImpl(null, null);
        }

        private void DoRemotelyCloseConsumerWithMessageListenerFiresNMSExceptionListenerTestImpl(Symbol errorType, string errorMessage)
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                ManualResetEvent consumerClosed = new ManualResetEvent(false);
                ManualResetEvent exceptionListenerFired = new ManualResetEvent(false);
                
                testPeer.ExpectSaslAnonymous();
                testPeer.ExpectOpen();
                testPeer.ExpectBegin();
                
                NmsConnection connection = EstablishAnonymousConnection("failover.maxReconnectAttempts=1", testPeer);
                
                connection.ExceptionListener += exception => { exceptionListenerFired.Set(); };

                Mock<INmsConnectionListener> connectionListener = new Mock<INmsConnectionListener>();

                connectionListener
                    .Setup(listener => listener.OnConsumerClosed(It.IsAny<IMessageConsumer>(), It.IsAny<Exception>()))
                    .Callback(() => { consumerClosed.Set(); });
                
                connection.AddConnectionListener(connectionListener.Object);
                
                testPeer.ExpectBegin();
                testPeer.ExpectBegin();

                ISession session1 = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                ISession session2 = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session2.GetQueue("myQueue");
                
                // Create a consumer, then remotely end it afterwards.
                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();
                testPeer.ExpectEnd();
                testPeer.RemotelyDetachLastOpenedLinkOnLastOpenedSession(expectDetachResponse: true, closed: true,  errorType: errorType, errorMessage: errorMessage, delayBeforeSend: 10);

                IMessageConsumer consumer = session2.CreateConsumer(queue);
                consumer.Listener += message => { };
                
                // Close first session to allow the receiver remote close timing to be deterministic
                session1.Close();
                
                // Verify the consumer gets marked closed
                testPeer.WaitForAllMatchersToComplete(1000);
                
                Assert.True(consumerClosed.WaitOne(TimeSpan.FromMilliseconds(2000)), "Consumer closed callback didn't trigger");
                Assert.True(exceptionListenerFired.WaitOne(TimeSpan.FromMilliseconds(2000)), "NMS Exception listener should have fired with a MessageListener");
                
                // Try closing it explicitly, should effectively no-op in client.
                // The test peer will throw during close if it sends anything.
                consumer.Close();
                
                // Shut the connection down
                testPeer.ExpectClose();
                connection.Close();
                
                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestFailoverDoesNotFailPendingSend()
        {
            using (TestAmqpPeer originalPeer = new TestAmqpPeer())
            using (TestAmqpPeer finalPeer = new TestAmqpPeer())
            {
                originalPeer.ExpectSaslAnonymous();
                originalPeer.ExpectOpen();
                originalPeer.ExpectBegin();
                originalPeer.ExpectBegin();
                
                // Ensure our send blocks in the provider waiting for credit so that on failover
                // the message will actually get sent from the Failover bits once we grant some
                // credit for the recovered sender.
                originalPeer.ExpectSenderAttachWithoutGrantingCredit();
                originalPeer.DropAfterLastMatcher(delay: 10); // Wait for sender to get into wait state
                
                // Post Failover Expectations of sender
                finalPeer.ExpectSaslAnonymous();
                finalPeer.ExpectOpen();
                finalPeer.ExpectBegin();
                finalPeer.ExpectBegin();
                finalPeer.ExpectSenderAttach();
                finalPeer.ExpectTransfer(messageMatcher: Assert.IsNotNull);
                finalPeer.ExpectClose();
                
                NmsConnection connection = EstablishAnonymousConnection("failover.initialReconnectDelay=25", originalPeer, finalPeer);
                ISession session = connection.CreateSession();
                IQueue queue = session.GetQueue("myQueue");

                IMessageProducer producer = session.CreateProducer(queue);
                
                // Create and transfer a new message
                string text = "myMessage";
                ITextMessage message = session.CreateTextMessage(text);
                
                Assert.DoesNotThrow(() =>
                {
                    producer.Send(message);
                });
                
                connection.Close();
                
                finalPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestTempDestinationRecreatedAfterConnectionFailsOver()
        {
            using (TestAmqpPeer originalPeer = new TestAmqpPeer())
            using (TestAmqpPeer finalPeer = new TestAmqpPeer())
            {
                ManualResetEvent originalConnected = new ManualResetEvent(false);
                ManualResetEvent finalConnected = new ManualResetEvent(false);
                
                // Create a peer to connect to, then one to reconnect to
                var originalUri = CreatePeerUri(originalPeer);
                var finalUri = CreatePeerUri(finalPeer);
                
                originalPeer.ExpectSaslAnonymous();
                originalPeer.ExpectOpen();
                originalPeer.ExpectBegin();
                originalPeer.ExpectBegin();
                string dynamicAddress1 = "myTempTopicAddress";
                originalPeer.ExpectTempTopicCreationAttach(dynamicAddress1);
                originalPeer.DropAfterLastMatcher();
                
                NmsConnection connection = EstablishAnonymousConnection(originalPeer, finalPeer);

                Mock<INmsConnectionListener> connectionListener = new Mock<INmsConnectionListener>();

                connectionListener
                    .Setup(listener => listener.OnConnectionEstablished(It.Is<Uri>(uri => originalUri == uri.ToString())))
                    .Callback(() => { originalConnected.Set(); });

                connectionListener
                    .Setup(listener => listener.OnConnectionRestored(It.Is<Uri>(uri => finalUri == uri.ToString())))
                    .Callback(() => { finalConnected.Set(); });

                connection.AddConnectionListener(connectionListener.Object);

                connection.Start();
                
                Assert.True(originalConnected.WaitOne(TimeSpan.FromSeconds(5)), "Should connect to original peer");
                
                // Post Failover Expectations of FinalPeer
                finalPeer.ExpectSaslAnonymous();
                finalPeer.ExpectOpen();
                finalPeer.ExpectBegin();
                String dynamicAddress2 = "myTempTopicAddress2";
                finalPeer.ExpectTempTopicCreationAttach(dynamicAddress2);
                
                // Session is recreated after previous temporary destinations are recreated on failover.
                finalPeer.ExpectBegin();
                
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                ITemporaryTopic temporaryTopic = session.CreateTemporaryTopic();
                
                Assert.True(finalConnected.WaitOne(TimeSpan.FromSeconds(5)), "Should connect to final peer");
                
                // Delete the temporary Topic and close the session.
                finalPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);
                finalPeer.ExpectEnd();
                
                temporaryTopic.Delete();
                
                session.Close();
                
                // Shut it down
                finalPeer.ExpectClose();
                connection.Close();
                
                originalPeer.WaitForAllMatchersToComplete(2000);
                finalPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestConsumerCanReceivesMessagesWhenConnectionLostDuringAutoAck()
        {
            using (TestAmqpPeer originalPeer = new TestAmqpPeer())
            using (TestAmqpPeer finalPeer = new TestAmqpPeer())
            {
                ManualResetEvent consumerReady = new ManualResetEvent(false);
                ManualResetEvent originalConnected = new ManualResetEvent(false);
                ManualResetEvent finalConnected = new ManualResetEvent(false);

                // Connect to the first peer
                originalPeer.ExpectSaslAnonymous();
                originalPeer.ExpectOpen();
                originalPeer.ExpectBegin();
                originalPeer.ExpectBegin();

                NmsConnection connection = EstablishAnonymousConnection(originalPeer, finalPeer);

                Mock<INmsConnectionListener> connectionListener = new Mock<INmsConnectionListener>();

                connectionListener
                    .Setup(listener => listener.OnConnectionEstablished(It.IsAny<Uri>()))
                    .Callback(() => { originalConnected.Set(); });

                connectionListener
                    .Setup(listener => listener.OnConnectionRestored(It.IsAny<Uri>()))
                    .Callback(() => { finalConnected.Set(); });

                connection.AddConnectionListener(connectionListener.Object);

                connection.Start();

                Assert.True(originalConnected.WaitOne(TimeSpan.FromSeconds(5)), "Should connect to original peer");

                originalPeer.ExpectReceiverAttach();
                originalPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithContent(), 1);
                originalPeer.RunAfterLastHandler(() => consumerReady.WaitOne(TimeSpan.FromSeconds(2)));
                originalPeer.DropAfterLastMatcher();

                // Post Failover Expectations of FinalPeer
                finalPeer.ExpectSaslAnonymous();
                finalPeer.ExpectOpen();
                finalPeer.ExpectBegin();
                finalPeer.ExpectBegin();
                finalPeer.ExpectReceiverAttach();
                finalPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithContent(), 1);
                finalPeer.ExpectDispositionThatIsAcceptedAndSettled();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");
                IMessageConsumer messageConsumer = session.CreateConsumer(queue);
                CountdownEvent msgReceivedLatch = new CountdownEvent(2);
                messageConsumer.Listener += message =>
                {
                    if (msgReceivedLatch.CurrentCount == 2)
                    {
                        consumerReady.Set();
                        finalConnected.WaitOne(2000);
                    }

                    msgReceivedLatch.Signal();
                };

                finalPeer.WaitForAllMatchersToComplete(5000);

                Assert.IsTrue(msgReceivedLatch.Wait(TimeSpan.FromSeconds(10)), $"Expected 2 messages, but got {2 - msgReceivedLatch.CurrentCount}");
            }
        }

        [Test, Timeout(20_000)]
        public void TestCreateProducerFailsWhenLinkRefused()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                testPeer.ExpectSaslAnonymous();
                testPeer.ExpectOpen();
                testPeer.ExpectBegin();

                NmsConnection connection = EstablishAnonymousConnection(testPeer);
                connection.Start();

                testPeer.ExpectBegin();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);

                string topicName = "myTopic";
                ITopic topic = session.GetTopic(topicName);

                // Expect a link to a topic node, which we will then refuse
                testPeer.ExpectSenderAttach(targetMatcher: x =>
                {
                    Target target = (Target) x;

                    Assert.AreEqual(topicName, target.Address);
                    Assert.IsFalse(target.Dynamic);
                    Assert.AreEqual((uint) TerminusDurability.NONE, target.Durable);
                }, sourceMatcher: Assert.NotNull, refuseLink: true);

                //Expect the detach response to the test peer closing the producer link after refusal.
                testPeer.ExpectDetach(expectClosed: true, sendResponse: false, replyClosed: false);

                Assert.Catch<NMSException>(() => session.CreateProducer(topic));

                // Shut it down
                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000), Category("Windows")]
        public void TestConnectionInterruptedInvokedWhenConnectionToBrokerLost()
        {
            using (TestAmqpPeer originalPeer = new TestAmqpPeer())
            {
                ManualResetEvent connectionInterruptedInvoked = new ManualResetEvent(false);

                originalPeer.ExpectSaslAnonymous();
                originalPeer.ExpectOpen();
                originalPeer.ExpectBegin();
                originalPeer.ExpectBegin();

                NmsConnection connection = EstablishAnonymousConnection(originalPeer);

                connection.ConnectionInterruptedListener += () => connectionInterruptedInvoked.Set();

                connection.Start();
                
                originalPeer.Close();

                Assert.IsTrue(connectionInterruptedInvoked.WaitOne(TimeSpan.FromSeconds(10)));
            }
        }
        
        [Test, Timeout(20_000), Category("Windows")]
        public void TestConnectionResumedInvokedWhenConnectionToBrokerLost()
        {
            using (TestAmqpPeer originalPeer = new TestAmqpPeer())
            using (TestAmqpPeer finalPeer = new TestAmqpPeer())
            {
                ManualResetEvent connectionResumedInvoked = new ManualResetEvent(false);

                originalPeer.ExpectSaslAnonymous();
                originalPeer.ExpectOpen();
                originalPeer.ExpectBegin();
                originalPeer.ExpectBegin();

                finalPeer.ExpectSaslAnonymous();
                finalPeer.ExpectOpen();
                finalPeer.ExpectBegin();
                finalPeer.ExpectBegin();

                NmsConnection connection = EstablishAnonymousConnection(originalPeer, finalPeer);

                connection.ConnectionResumedListener += () => connectionResumedInvoked.Set();

                connection.Start();
                
                originalPeer.Close();
                Assert.IsTrue(connectionResumedInvoked.WaitOne(TimeSpan.FromSeconds(10)));
            }
        }

        private NmsConnection EstablishAnonymousConnection(params TestAmqpPeer[] peers)
        {
            return EstablishAnonymousConnection(null, null, peers);
        }

        private NmsConnection EstablishAnonymousConnection(string failoverParams, params TestAmqpPeer[] peers)
        {
            return EstablishAnonymousConnection(null, failoverParams, peers);
        }

        private NmsConnection EstablishAnonymousConnection(string connectionParams, string failoverParams, params TestAmqpPeer[] peers)
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
            return (NmsConnection) factory.CreateConnection();
        }

        private string CreatePeerUri(TestAmqpPeer peer, string parameters = null)
        {
            return $"amqp://127.0.0.1:{peer.ServerPort}/{(parameters != null ? "?" + parameters : "")}";
        }
    }
}