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
using System.Collections.Generic;
using System.Linq;
using Apache.NMS;
using Apache.NMS.AMQP;
using Apache.NMS.Util;
using NMS.AMQP.Test.TestAmqp;
using NUnit.Framework;

namespace NMS.AMQP.Test.Integration
{
    [TestFixture]
    public class AmqpAcknowledgmentsIntegrationTest
    {
        private static readonly string User = "USER";
        private static readonly string Password = "PASSWORD";
        private static readonly string Address = "amqp://127.0.0.1:5672";

        [Test, Timeout(2000)]
        public void TestAcknowledgeFailsAfterSessionIsClosed()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                testAmqpPeer.SendMessage("myQueue", "test");

                IConnection connection = EstablishConnection();
                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.ClientAcknowledge);
                IQueue queue = session.GetQueue("myQueue");
                IMessageConsumer consumer = session.CreateConsumer(queue);

                IMessage receivedMessage = consumer.Receive();
                Assert.NotNull(receivedMessage, "Message was not received");
                session.Close();

                try
                {
                    receivedMessage.Acknowledge();
                    Assert.Fail("Should not be able to acknowledge the message after session closed");
                }
                catch (NMSException) { }
            }
        }

        [Test, Timeout(2000)]
        public void TestClientAcknowledgeMessages()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();

                int msgCount = 3;
                for (int i = 0; i < msgCount; i++)
                {
                    testAmqpPeer.SendMessage("myQueue", "test" + i);
                }

                IConnection connection = EstablishConnection();
                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.ClientAcknowledge);

                IQueue queue = session.GetQueue("myQueue");
                IMessageConsumer consumer = session.CreateConsumer(queue);

                IMessage lastReceivedMessage = null;
                for (int i = 0; i < msgCount; i++)
                {
                    lastReceivedMessage = consumer.Receive();
                    Assert.NotNull(lastReceivedMessage, "Message " + i + " was not received");
                }

                lastReceivedMessage.Acknowledge();

                Assert.That(() => testAmqpPeer.AcceptedMessages.Count(), Is.EqualTo(3).After(200, 50));
            }
        }

        [Test, Timeout(2000)]
        public void TestClientAcknowledgeMessagesAsync()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();

                int msgCount = 3;
                for (int i = 0; i < msgCount; i++)
                {
                    testAmqpPeer.SendMessage("myQueue", "test" + i);
                }

                IConnection connection = EstablishConnection();
                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.ClientAcknowledge);

                IQueue queue = session.GetQueue("myQueue");
                IMessageConsumer consumer = session.CreateConsumer(queue);

                CountDownLatch latch = new CountDownLatch(3);
                IMessage lastReceivedMessage = null;
                consumer.Listener += message =>
                {
                    lastReceivedMessage = message;
                    latch.countDown();
                };

                latch.await(TimeSpan.FromMilliseconds(100));

                lastReceivedMessage.Acknowledge();

                Assert.That(() => testAmqpPeer.AcceptedMessages.Count(), Is.EqualTo(3).After(200, 50));

                connection.Close();
            }
        }

        [Test, Timeout(2000)]
        public void TestAcknowledgeIndividualMessages()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();

                int msgCount = 6;
                for (int i = 0; i < msgCount; i++)
                {
                    testAmqpPeer.SendMessage("myQueue", "test" + i);
                }

                IConnection connection = EstablishConnection();
                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.IndividualAcknowledge);

                IQueue queue = session.GetQueue("myQueue");
                IMessageConsumer consumer = session.CreateConsumer(queue);

                List<ITextMessage> messages = new List<ITextMessage>();
                for (int i = 0; i < msgCount; i++)
                {
                    ITextMessage message = consumer.Receive() as ITextMessage;
                    Assert.NotNull(message);
                    messages.Add(message);
                }

                // Acknowledge the messages in a random order and verify the individual dispositions have expected delivery state.
                Random random = new Random();
                for (int i = 0; i < msgCount; i++)
                {
                    var message = messages[random.Next(msgCount - i)];
                    messages.Remove(message);
                    message.Acknowledge();

                    Assert.That(() => testAmqpPeer.AcceptedMessages.Any(x => x.Body.ToString() == message.Text), Is.True.After(200, 50));
                    Assert.That(() => testAmqpPeer.AcceptedMessages.Count(), Is.EqualTo(i + 1).After(200, 50));
                }

                connection.Close();
            }
        }

        [Test, Timeout(2000)]
        public void TestAcknowledgeIndividualMessagesAsync()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();

                int msgCount = 6;
                for (int i = 0; i < msgCount; i++)
                {
                    testAmqpPeer.SendMessage("myQueue", "test" + i);
                }

                IConnection connection = EstablishConnection();
                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.IndividualAcknowledge);

                IQueue queue = session.GetQueue("myQueue");
                IMessageConsumer consumer = session.CreateConsumer(queue);

                CountDownLatch latch = new CountDownLatch(msgCount);
                List<ITextMessage> messages = new List<ITextMessage>();
                consumer.Listener += message =>
                {
                    messages.Add((ITextMessage)message);
                    latch.countDown();
                };

                Assert.True(latch.await(TimeSpan.FromMilliseconds(1000)), $"Should receive: {msgCount}, but received: {messages.Count}");

                // Acknowledge the messages in a random order and verify the individual dispositions have expected delivery state.
                Random random = new Random();
                for (int i = 0; i < msgCount; i++)
                {
                    var message = messages[random.Next(msgCount - i)];
                    messages.Remove(message);
                    message.Acknowledge();

                    Assert.That(() => testAmqpPeer.AcceptedMessages.Any(x => x.Body.ToString() == message.Text), Is.True.After(200, 50));
                    Assert.That(() => testAmqpPeer.AcceptedMessages.Count(), Is.EqualTo(i + 1).After(200, 50), "Wrong number of messages acknowledged.");
                }

                connection.Close();
            }
        }

        [Test, Timeout(2000)]
        public void TestAutoAcknowledgeMessages()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();

                int msgCount = 6;
                for (int i = 0; i < msgCount; i++)
                {
                    testAmqpPeer.SendMessage("myQueue", "test" + i);
                }

                IConnection connection = EstablishConnection();
                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);

                IQueue queue = session.GetQueue("myQueue");
                IMessageConsumer consumer = session.CreateConsumer(queue);
                for (int i = 0; i < msgCount; i++)
                {
                    ITextMessage message = consumer.Receive() as ITextMessage;
                    Assert.NotNull(message);
                    Assert.That(() => testAmqpPeer.AcceptedMessages.Any(x => x.Body.ToString() == message.Text), Is.True.After(200, 50));
                }

                connection.Close();
            }
        }

        [Test, Timeout(2000)]
        public void TestAutoAcknowledgeMessagesAsync()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();

                int msgCount = 6;
                for (int i = 0; i < msgCount; i++)
                {
                    testAmqpPeer.SendMessage("myQueue", "test" + i);
                }

                IConnection connection = EstablishConnection();
                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);

                IQueue queue = session.GetQueue("myQueue");
                IMessageConsumer consumer = session.CreateConsumer(queue);
                consumer.Listener += (message) => { };
                Assert.That(() => testAmqpPeer.AcceptedMessages.Count(), Is.EqualTo(6).After(200, 50));

                connection.Close();
            }
        }

        private IConnection EstablishConnection()
        {
            NmsConnectionFactory factory = new NmsConnectionFactory(Address);
            IConnection connection = factory.CreateConnection(User, Password);
            connection.Start();
            return connection;
        }
    }
}