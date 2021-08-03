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

using System.Threading.Tasks;
using Apache.NMS;
using NMS.AMQP.Test.TestAmqp;
using NUnit.Framework;

namespace NMS.AMQP.Test.Integration.Async
{
    [TestFixture]
    public class SessionIntegrationTestAsync : IntegrationTestFixture
    {
        [Test, Timeout(20_000)]
        public async Task TestCloseSession()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);
                testPeer.ExpectBegin();
                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                Assert.NotNull(session, "Session should not be null");
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                await session.CloseAsync();

                // Should send nothing and throw no error.
                await session.CloseAsync();

                await connection.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestCreateProducer()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);
                testPeer.ExpectBegin();

                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);

                testPeer.ExpectSenderAttach();
                testPeer.ExpectClose();

                IQueue queue = await session.GetQueueAsync("myQueue");
                await session.CreateProducerAsync(queue);

                await connection.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestCreateConsumer()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);
                await connection.StartAsync();

                testPeer.ExpectBegin();

                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();
                testPeer.ExpectClose();

                IQueue queue = await session.GetQueueAsync("myQueue");
                await session.CreateConsumerAsync(queue);

                await connection.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestCreateConsumerWithEmptySelector()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);
                await connection.StartAsync();

                testPeer.ExpectBegin();

                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();
                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();
                testPeer.ExpectClose();

                IQueue queue = await session.GetQueueAsync("myQueue");
                await session.CreateConsumerAsync(queue, "");
                await session.CreateConsumerAsync(queue, "", noLocal: false);
                
                await connection.CloseAsync();
                
                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestCreateConsumerWithNullSelector()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);
                await connection.StartAsync();

                testPeer.ExpectBegin();

                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();
                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();
                testPeer.ExpectClose();

                IQueue queue = await session.GetQueueAsync("myQueue");
                await session.CreateConsumerAsync(queue, null);
                await session.CreateConsumerAsync(queue, null, noLocal: false);
                
                await connection.CloseAsync();
                
                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestCreateDurableConsumer()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);
                await connection.StartAsync();

                testPeer.ExpectBegin();
                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                
                string topicName = "myTopic";
                ITopic topic = await session.GetTopicAsync(topicName);
                string subscriptionName = "mySubscription";

                testPeer.ExpectDurableSubscriberAttach(topicName, subscriptionName);
                testPeer.ExpectLinkFlow();
                
                IMessageConsumer durableConsumer = await session.CreateDurableConsumerAsync(topic, subscriptionName, null, false);
                Assert.NotNull(durableConsumer, "MessageConsumer object was null");
                
                testPeer.ExpectClose();
                await connection.CloseAsync();
                
                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }
        
        [Test, Timeout(20_000)]
        public async Task TestCreateTemporaryQueue()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);
                
                testPeer.ExpectBegin();
                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                
                string dynamicAddress = "myTempQueueAddress";
                testPeer.ExpectTempQueueCreationAttach(dynamicAddress);
                
                ITemporaryQueue temporaryQueue = await session.CreateTemporaryQueueAsync();
                Assert.NotNull(temporaryQueue, "TemporaryQueue object was null");
                Assert.NotNull(temporaryQueue.QueueName, "TemporaryQueue queue name was null");
                Assert.AreEqual(dynamicAddress, temporaryQueue.QueueName, "TemporaryQueue name not as expected");
                
                testPeer.ExpectClose();
                await connection.CloseAsync();
                
                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }
        
        [Test, Timeout(20_000)]
        public async Task TestCreateTemporaryTopic()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);
                
                testPeer.ExpectBegin();
                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                
                string dynamicAddress = "myTempTopicAddress";
                testPeer.ExpectTempTopicCreationAttach(dynamicAddress);
                
                ITemporaryTopic temporaryTopic = await session.CreateTemporaryTopicAsync();
                Assert.NotNull(temporaryTopic, "TemporaryTopic object was null");
                Assert.NotNull(temporaryTopic.TopicName, "TemporaryTopic name was null");
                Assert.AreEqual(dynamicAddress, temporaryTopic.TopicName, "TemporaryTopic name not as expected");
                
                testPeer.ExpectClose();
                await connection.CloseAsync();
                
                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }
       
        [Test, Timeout(20_000)]
        public async Task TestCreateSharedConsumer()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);
                await connection.StartAsync();

                testPeer.ExpectBegin();
                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                
                string topicName = "myTopic";
                ITopic topic = await session.GetTopicAsync(topicName);
                string subscriptionName = "mySubscription";

                testPeer.ExpectSharedSubscriberAttach(topicName, subscriptionName);
                testPeer.ExpectLinkFlow();
                
                IMessageConsumer durableConsumer = await session.CreateSharedConsumerAsync(topic, subscriptionName, null);//, false);
                // IMessageConsumer durableConsumer = session.CreateDurableConsumer(topic, subscriptionName, null, false);
                Assert.NotNull(durableConsumer, "MessageConsumer object was null");
                
                testPeer.ExpectClose();
                await connection.CloseAsync();
                
                testPeer.WaitForAllMatchersToComplete(20000);
            }
        }
        
        [Test, Timeout(20_000)]
        public async Task TestCreateSharedDurableConsumer()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);
                await connection.StartAsync();

                testPeer.ExpectBegin();
                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                
                string topicName = "myTopic";
                ITopic topic = await session.GetTopicAsync(topicName);
                string subscriptionName = "mySubscription";

                testPeer.ExpectSharedDurableSubscriberAttach(topicName, subscriptionName);
                testPeer.ExpectLinkFlow();
                
                IMessageConsumer durableConsumer = await session.CreateSharedDurableConsumerAsync(topic, subscriptionName, null); //, false);
                Assert.NotNull(durableConsumer, "MessageConsumer object was null");
                
                testPeer.ExpectClose();
                await connection.CloseAsync();
                
                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }
    }
}