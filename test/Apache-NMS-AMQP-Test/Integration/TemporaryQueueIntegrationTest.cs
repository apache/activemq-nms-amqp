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
    public class TemporaryQueueIntegrationTest
    {
        private static readonly string User = "USER";
        private static readonly string Password = "PASSWORD";
        private static readonly string Address = "amqp://127.0.0.1:5672";

        [Test]
        public void TestCreateTemporaryQueue()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.RegisterLinkProcessor(new TestLinkProcessor());
                
                testAmqpPeer.Open();
                IConnection connection = EstablishConnection();
                connection.Start();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                ITemporaryQueue queue = session.CreateTemporaryQueue();

                session.CreateConsumer(queue);

                connection.Close();
            }
        }

        [Test]
        public void TestCantConsumeFromTemporaryQueueCreatedOnAnotherConnection()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.RegisterLinkProcessor(new TestLinkProcessor());
                
                testAmqpPeer.Open();
                IConnection connection = EstablishConnection();
                connection.Start();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                ITemporaryQueue queue = session.CreateTemporaryQueue();

                IConnection connection2 = EstablishConnection();
                ISession session2 = connection2.CreateSession(AcknowledgementMode.AutoAcknowledge);

                Assert.Catch<InvalidDestinationException>(() => session2.CreateConsumer(queue), "Should not be able to consumer from temporary queue from another connection");

                session.CreateConsumer(queue);

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
                ITemporaryQueue queue = session.CreateTemporaryQueue();

                IMessageConsumer consumer = session.CreateConsumer(queue);

                Assert.Catch<IllegalStateException>(() => queue.Delete(), "should not be able to delete temporary queue with active consumers");
                
                consumer.Close();
                
                // Now it should be allowed
                queue.Delete();

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