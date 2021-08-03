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
using System.Threading.Tasks;
using Apache.NMS;
using Apache.NMS.AMQP;
using NMS.AMQP.Test.TestAmqp;
using NMS.AMQP.Test.TestAmqp.BasicTypes;
using NUnit.Framework;

namespace NMS.AMQP.Test.Integration.Async
{
    [TestFixture]
    public class ConnectionIntegrationTestAsync : IntegrationTestFixture
    {
        [Test, Timeout(20_000)]
        public async Task TestCreateAndCloseConnection()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);
                testPeer.ExpectClose();
                await connection.CloseAsync();
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestExplicitConnectionCloseListenerIsNotInvoked()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                ManualResetEvent exceptionFired = new ManualResetEvent(false);
                IConnection connection = await EstablishConnectionAsync(testPeer);
                connection.ExceptionListener += exception => { exceptionFired.Set(); };
                
                testPeer.ExpectClose();
                await connection.CloseAsync();
                
                Assert.IsFalse(exceptionFired.WaitOne(TimeSpan.FromMilliseconds(100)));
            }
        }
        
        [Test, Timeout(20_000)]
        public async Task TestCreateAutoAckSession()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);
                testPeer.ExpectBegin();
                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                Assert.NotNull(session, "Session should not be null");
                testPeer.ExpectClose();
                Assert.AreEqual(AcknowledgementMode.AutoAcknowledge, session.AcknowledgementMode);
                await connection.CloseAsync();
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestCreateAutoAckSessionByDefault()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);
                testPeer.ExpectBegin();
                ISession session = await connection.CreateSessionAsync();
                Assert.NotNull(session, "Session should not be null");
                Assert.AreEqual(AcknowledgementMode.AutoAcknowledge, session.AcknowledgementMode);
                testPeer.ExpectClose();
                await connection.CloseAsync();
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestRemotelyCloseConnectionDuringSessionCreation()
        {
            string errorMessage = "buba";

            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);

                // Expect the begin, then explicitly close the connection with an error
                testPeer.ExpectBegin(sendResponse: false);
                testPeer.RemotelyCloseConnection(expectCloseResponse: true, errorCondition: AmqpError.NOT_ALLOWED, errorMessage: errorMessage);

                try
                {
                    await connection.CreateSessionAsync();
                    Assert.Fail("Expected exception to be thrown");
                }
                catch (NMSException e)
                {
                    Assert.True(e.Message.Contains(errorMessage));
                }

                await connection.CloseAsync();
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestRemotelyEndConnectionListenerInvoked()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                ManualResetEvent done = new ManualResetEvent(false);

                // Don't set a ClientId, so that the underlying AMQP connection isn't established yet
                IConnection connection = await EstablishConnectionAsync(testPeer: testPeer, setClientId: false);

                // Tell the test peer to close the connection when executing its last handler
                testPeer.RemotelyCloseConnection(expectCloseResponse: true, errorCondition: ConnectionError.CONNECTION_FORCED, errorMessage: "buba");

                connection.ExceptionListener += exception => done.Set();

                // Trigger the underlying AMQP connection
                await connection.StartAsync();

                Assert.IsTrue(done.WaitOne(TimeSpan.FromSeconds(5)), "Connection should report failure");

                await connection.CloseAsync();
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestRemotelyEndConnectionWithSessionWithConsumer()
        {
            string errorMessage = "buba";

            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);

                testPeer.ExpectBegin();
                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);

                // Create a consumer, then remotely end the connection afterwards.
                testPeer.ExpectReceiverAttach();
 
                testPeer.ExpectLinkFlow();
                testPeer.RemotelyCloseConnection(expectCloseResponse: true, errorCondition: AmqpError.RESOURCE_LIMIT_EXCEEDED, errorMessage: errorMessage);

                IQueue queue = await session.GetQueueAsync("myQueue");
                IMessageConsumer consumer = await session.CreateConsumerAsync(queue);

                Assert.That(() => ((NmsConnection) connection).IsConnected, Is.False.After(10_000, 100), "Connection never closes.");

                try
                {
                    await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                    Assert.Fail("Expected ISE to be thrown due to being closed");
                }
                catch (NMSConnectionException e)
                {
                    Assert.True(e.ToString().Contains(AmqpError.RESOURCE_LIMIT_EXCEEDED));
                    Assert.True(e.ToString().Contains(errorMessage));
                }

                // Verify the session is now marked closed
                try
                {
                    var _ = session.AcknowledgementMode;
                    Assert.Fail("Expected ISE to be thrown due to being closed");
                }
                catch (IllegalStateException e)
                {
                    Assert.True(e.ToString().Contains(AmqpError.RESOURCE_LIMIT_EXCEEDED));
                    Assert.True(e.ToString().Contains(errorMessage));
                }

                // Verify the consumer is now marked closed
                try
                {
                    consumer.Listener += message => { };
                }
                catch (IllegalStateException e)
                {
                    Assert.True(e.ToString().Contains(AmqpError.RESOURCE_LIMIT_EXCEEDED));
                    Assert.True(e.ToString().Contains(errorMessage));
                }
                
                // Try closing them explicitly, should effectively no-op in client.
                // The test peer will throw during close if it sends anything.
                await consumer.CloseAsync();
                await session.CloseAsync();
                await connection.CloseAsync();
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestConnectionStartStop()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                int msgCount = 10;

                IConnection connection = await EstablishConnectionAsync(testPeer);
                await connection.StartAsync();

                testPeer.ExpectBegin();
                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.ClientAcknowledge);

                IQueue queue = await session.GetQueueAsync("myQueue");

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithNullContent(), count: msgCount);
                
                var consumer = await session.CreateConsumerAsync(queue);

                for (int i = 0; i < 5; i++)
                {
                    IMessage message = await consumer.ReceiveAsync(TimeSpan.FromMilliseconds(1000));
                    Assert.IsNotNull(message);
                }
                
                // stop the connection, consumers shouldn't receive any more messages
                await connection.StopAsync();
                
                // No messages should arrive to consumer as connection has been stopped
                Assert.IsNull(await consumer.ReceiveAsync(TimeSpan.FromMilliseconds(100)), "Message arrived despite the fact, that the connection was stopped.");
                
                // restart the connection
                await connection.StartAsync();
                
                // The second batch of messages should be delivered
                for (int i = 0; i < 5; i++)
                {
                    IMessage message = await consumer.ReceiveAsync(TimeSpan.FromMilliseconds(1000));
                    Assert.IsNotNull(message);
                }
                
                testPeer.ExpectClose();
                await connection.CloseAsync();
                
                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }
    }
}