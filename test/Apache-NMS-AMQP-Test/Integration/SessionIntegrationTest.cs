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

using System.Linq;
using Amqp.Framing;
using Amqp.Listener;
using Amqp.Types;
using Apache.NMS;
using Apache.NMS.AMQP;
using NMS.AMQP.Test.TestAmqp;
using NUnit.Framework;

namespace NMS.AMQP.Test.Integration
{
    [TestFixture]
    public class SessionIntegrationTest
    {
        private static readonly string User = "USER";
        private static readonly string Password = "PASSWORD";
        private static readonly string Address = "amqp://127.0.0.1:5672";

        [Test]
        public void TestCloseSession()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                IConnection connection = EstablishConnection();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                Assert.NotNull(session, "Session should not be null");

                session.Close();

                // Should send nothing and throw no error.
                session.Close();
                connection.Close();
            }
        }

        [Test]
        public void TestCreateProducer()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                IConnection connection = EstablishConnection();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);

                IQueue queue = session.GetQueue("myQueue");
                session.CreateProducer(queue);

                connection.Close();
            }
        }

        [Test]
        public void TestCreateConsumer()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                IConnection connection = EstablishConnection();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);

                IQueue queue = session.GetQueue("myQueue");
                session.CreateConsumer(queue);

                connection.Close();
            }
        }

        [Test]
        public void TestCreateConsumerWithEmptySelector()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                IConnection connection = EstablishConnection();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);

                IQueue queue = session.GetQueue("myQueue");
                session.CreateConsumer(queue, "");

                connection.Close();
            }
        }

        [Test]
        public void TestCreateConsumerWithNullSelector()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                IConnection connection = EstablishConnection();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);

                IQueue queue = session.GetQueue("myQueue");

                session.CreateConsumer(queue, null);

                connection.Close();
            }
        }

        [Test]
        public void TestCreateDurableConsumer()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                TestLinkProcessor testLinkProcessor = new TestLinkProcessor();
                testAmqpPeer.RegisterLinkProcessor(testLinkProcessor);

                NmsConnection connection = (NmsConnection)EstablishConnection();
                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);

                string topicName = "myTopic";
                string subscriptionName = "mySubscription";
                ITopic topic = session.GetTopic(topicName);
                IMessageConsumer durableConsumer = session.CreateDurableConsumer(topic, subscriptionName, null, false);

                Assert.NotNull(durableConsumer);

                // Expect That Durable Subscriber Attach
                Assert.That(() => testLinkProcessor.Consumer, Is.Not.Null.After(200));
                Assert.AreEqual(subscriptionName, testLinkProcessor.Consumer.Link.Name);;
                Source source = (Source)testLinkProcessor.Consumer.Attach.Source;
                Assert.AreEqual((uint)TerminusDurability.UNSETTLED_STATE, source.Durable);
                Assert.AreEqual(new Symbol("never"), source.ExpiryPolicy);
                Assert.AreEqual(topicName, source.Address);
                Assert.IsFalse(source.Dynamic);

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