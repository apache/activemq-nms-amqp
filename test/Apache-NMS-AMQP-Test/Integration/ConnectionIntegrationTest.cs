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
using Apache.NMS;
using Apache.NMS.AMQP;
using Apache.NMS.Util;
using NMS.AMQP.Test.TestAmqp;
using NUnit.Framework;

namespace NMS.AMQP.Test.Integration
{
    [TestFixture]
    public class ConnectionIntegrationTest
    {
        private static readonly string User = "USER";
        private static readonly string Password = "PASSWORD";
        private static readonly string Address = "amqp://127.0.0.1:5672";

        [Test, Timeout(2000)]
        public void TestCreateAndCloseConnection()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                NmsConnectionFactory factory = new NmsConnectionFactory(Address);
                IConnection connection = factory.CreateConnection(User, Password);
                connection.Start();
                connection.Close();
            }
        }

        [Test, Timeout(2000)]
        public void TestCreateAutoAckSession()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                NmsConnectionFactory factory = new NmsConnectionFactory(Address);
                IConnection connection = factory.CreateConnection(User, Password);
                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                Assert.NotNull(session, "Session should not be null");
                Assert.AreEqual(AcknowledgementMode.AutoAcknowledge, session.AcknowledgementMode);
                connection.Close();
            }
        }

        [Test, Timeout(2000)]
        public void TestCreateAutoAckSessionByDefault()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                NmsConnectionFactory factory = new NmsConnectionFactory(Address);
                IConnection connection = factory.CreateConnection(User, Password);
                connection.Start();
                ISession session = connection.CreateSession();
                Assert.NotNull(session, "Session should not be null");
                Assert.AreEqual(AcknowledgementMode.AutoAcknowledge, session.AcknowledgementMode);
                connection.Close();
            }
        }

        [Test, Timeout(2000)]
        public void TestRemotelyCloseConnectionDuringSessionCreation()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                NmsConnectionFactory factory = new NmsConnectionFactory(Address);
                IConnection connection = factory.CreateConnection(User, Password);

                // We need to set request timeout because there may be a deadlock when we try to 
                // create amqplite session before underlying connection changes its state do disconnected
                connection.RequestTimeout = TimeSpan.FromMilliseconds(1000);

                // Explicitly close the connection with an error
                testAmqpPeer.Close();

                try
                {
                    connection.CreateSession();
                    Assert.Fail("Expected exception to be thrown");
                }
                catch (NMSException e)
                {
                    Console.WriteLine(e);
                }

                connection.Close();
            }
        }

        [Test, Timeout(2000)]
        public void TestRemotelyEndConnectionListenerInvoked()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                NmsConnectionFactory factory = new NmsConnectionFactory(Address);
                IConnection connection = factory.CreateConnection(User, Password);

                bool done = false;
                connection.ExceptionListener += exception => { done = true; };
                connection.Start();

                // Explicitly close the connection with an error
                testAmqpPeer.Close();

                Assert.That(() => done, Is.True.After(1000));
            }
        }

        [Test, Timeout(2000)]
        public void TestRemotelyEndConnectionWithSessionWithConsumer()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                NmsConnectionFactory factory = new NmsConnectionFactory(Address);
                IConnection connection = factory.CreateConnection(User, Password);
                connection.Start();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");
                var consumer = session.CreateConsumer(queue);

                // Explicitly close the connection with an error
                testAmqpPeer.Close();

                Assert.That(() => ((NmsConnection) connection).IsConnected, Is.False.After(200, 50), "Connection never closed");
                Assert.That(() => ((NmsConnection) connection).IsClosed, Is.True.After(200, 50), "Connection never closed");

                try
                {
                    connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                    Assert.Fail("Expected ISE to be thrown due to being closed");
                }
                catch (IllegalStateException)
                {
                }

                // Verify the session is now marked closed
                try
                {
                    session.CreateConsumer(queue);
                    Assert.Fail("Expected ISE to be thrown due to being closed");
                }
                catch (IllegalStateException)
                {
                }

                // Verify the consumer is now marked closed
                try
                {
                    consumer.Listener += message => { };
                    Assert.Fail("Expected ISE to be thrown due to being closed");
                }
                catch (IllegalStateException)
                {
                }

                // Try closing them explicitly, should effectively no-op in client.
                // The test peer will throw during close if it sends anything.
                consumer.Close();
                session.Close();
                connection.Close();
            }
        }

        [Test]
        public void TestConnectionStartStop()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                testAmqpPeer.RegisterMessageSource("myQueue");
                
                NmsConnectionFactory factory = new NmsConnectionFactory(Address);
                IConnection connection = factory.CreateConnection(User, Password);

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");
                var consumer = session.CreateConsumer(queue);

                CountDownLatch firstBatch = new CountDownLatch(5);
                CountDownLatch secondBatch = new CountDownLatch(5);

                consumer.Listener += message =>
                {
                    if (firstBatch.Remaining > 0)
                        firstBatch.countDown();
                    else
                        secondBatch.countDown();
                };

                // send first batch of messages
                for (int i = 0; i < 5; i++)
                {
                    testAmqpPeer.SendMessage("myQueue", $"message{i.ToString()}");
                }

                connection.Start();
                
                Assert.True(firstBatch.@await(TimeSpan.FromMilliseconds(1000)));
                
                // stop the connection, consumers shouldn't receive any more messages
                connection.Stop();
                
                // send second batch of messages
                for (int i = 5; i < 10; i++)
                {
                    testAmqpPeer.SendMessage("myQueue", $"message{i.ToString()}");
                }
                
                // No messages should arrive to consumer as connection has been stopped 
                Assert.False(secondBatch.@await(TimeSpan.FromMilliseconds(1000)), "Message arrived despite the fact, that the connection was stopped.");
                
                // restart the connection
                connection.Start();
                
                // The second batch of messages should be delivered
                Assert.True(secondBatch.@await(TimeSpan.FromMilliseconds(1000)), "No messages arrived.");
                
                // Try closing them explicitly, should effectively no-op in client.
                // The test peer will throw during close if it sends anything.
                consumer.Close();
                session.Close();
                connection.Close();
            }
        }
    }
}