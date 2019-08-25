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
using NMS.AMQP.Test.TestAmqp;
using NUnit.Framework;

namespace NMS.AMQP.Test.Integration
{
    [TestFixture]
    public class SessionIntegrationTest : IntegrationTestFixture
    {
        [Test, Timeout(20_000)]
        public void TestCloseSession()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                testPeer.ExpectBegin();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                Assert.NotNull(session, "Session should not be null");
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                session.Close();

                // Should send nothing and throw no error.
                session.Close();

                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestCreateProducer()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                testPeer.ExpectBegin();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);

                testPeer.ExpectSenderAttach();
                testPeer.ExpectClose();

                IQueue queue = session.GetQueue("myQueue");
                session.CreateProducer(queue);

                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestCreateConsumer()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                connection.Start();

                testPeer.ExpectBegin();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();
                testPeer.ExpectClose();

                IQueue queue = session.GetQueue("myQueue");
                session.CreateConsumer(queue);

                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestCreateConsumerWithEmptySelector()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                connection.Start();

                testPeer.ExpectBegin();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();
                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();
                testPeer.ExpectClose();

                IQueue queue = session.GetQueue("myQueue");
                session.CreateConsumer(queue, "");
                session.CreateConsumer(queue, "", noLocal: false);
                
                connection.Close();
                
                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestCreateConsumerWithNullSelector()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                connection.Start();

                testPeer.ExpectBegin();

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();
                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();
                testPeer.ExpectClose();

                IQueue queue = session.GetQueue("myQueue");
                session.CreateConsumer(queue, null);
                session.CreateConsumer(queue, null, noLocal: false);
                
                connection.Close();
                
                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestCreateDurableConsumer()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                connection.Start();

                testPeer.ExpectBegin();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                
                string topicName = "myTopic";
                ITopic topic = session.GetTopic(topicName);
                string subscriptionName = "mySubscription";

                testPeer.ExpectDurableSubscriberAttach(topicName, subscriptionName);
                testPeer.ExpectLinkFlow();
                
                IMessageConsumer durableConsumer = session.CreateDurableConsumer(topic, subscriptionName, null, false);
                Assert.NotNull(durableConsumer, "MessageConsumer object was null");
                
                testPeer.ExpectClose();
                connection.Close();
                
                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }
        
        [Test, Timeout(20_000)]
        public void TestCreateTemporaryQueue()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                
                testPeer.ExpectBegin();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                
                string dynamicAddress = "myTempQueueAddress";
                testPeer.ExpectTempQueueCreationAttach(dynamicAddress);
                
                ITemporaryQueue temporaryQueue = session.CreateTemporaryQueue();
                Assert.NotNull(temporaryQueue, "TemporaryQueue object was null");
                Assert.NotNull(temporaryQueue.QueueName, "TemporaryQueue queue name was null");
                Assert.AreEqual(dynamicAddress, temporaryQueue.QueueName, "TemporaryQueue name not as expected");
                
                testPeer.ExpectClose();
                connection.Close();
                
                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }
        
        [Test, Timeout(20_000)]
        public void TestCreateTemporaryTopic()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                
                testPeer.ExpectBegin();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                
                string dynamicAddress = "myTempTopicAddress";
                testPeer.ExpectTempTopicCreationAttach(dynamicAddress);
                
                ITemporaryTopic temporaryTopic = session.CreateTemporaryTopic();
                Assert.NotNull(temporaryTopic, "TemporaryTopic object was null");
                Assert.NotNull(temporaryTopic.TopicName, "TemporaryTopic name was null");
                Assert.AreEqual(dynamicAddress, temporaryTopic.TopicName, "TemporaryTopic name not as expected");
                
                testPeer.ExpectClose();
                connection.Close();
                
                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }
    }
}