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

using Apache.NMS;
using Apache.NMS.AMQP;
using NMS.AMQP.Test.TestAmqp;
using NUnit.Framework;

namespace NMS.AMQP.Test.Integration
{
    [TestFixture]
    public class TemporaryTopicIntegrationTest
    {
        private static readonly string User = "USER";
        private static readonly string Password = "PASSWORD";
        private static readonly string Address = "amqp://127.0.0.1:5672";
        
        [Test]
        public void TestCreateTemporaryTopic()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.RegisterLinkProcessor(new TestLinkProcessor());
                
                testAmqpPeer.Open();
                IConnection connection = EstablishConnection();
                connection.Start();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                ITemporaryTopic topic = session.CreateTemporaryTopic();

                session.CreateConsumer(topic);

                connection.Close();
            }
        }
        
        [Test]
        public void TestCantConsumeFromTemporaryTopicCreatedOnAnotherConnection()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.RegisterLinkProcessor(new TestLinkProcessor());
                
                testAmqpPeer.Open();
                IConnection connection = EstablishConnection();
                connection.Start();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                ITemporaryTopic topic = session.CreateTemporaryTopic();

                IConnection connection2 = EstablishConnection();
                ISession session2 = connection2.CreateSession(AcknowledgementMode.AutoAcknowledge);

                Assert.Catch<InvalidDestinationException>(() => session2.CreateConsumer(topic), "Should not be able to consumer from temporary topic from another connection");

                session.CreateConsumer(topic);

                connection.Close();
            }
        }
        
        [Test]
        public void TestCantDeleteTemporaryQueueWithConsumers()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.RegisterLinkProcessor(new TestLinkProcessor());
                
                testAmqpPeer.Open();
                IConnection connection = EstablishConnection();
                connection.Start();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                ITemporaryTopic topic = session.CreateTemporaryTopic();

                IMessageConsumer consumer = session.CreateConsumer(topic);

                Assert.Catch<IllegalStateException>(() => topic.Delete(), "should not be able to delete temporary queue with active consumers");
                
                consumer.Close();
                
                // Now it should be allowed
                topic.Delete();

                connection.Start();
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