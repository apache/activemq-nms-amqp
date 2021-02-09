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
    // Adapted from SessionIntegrationTest to use NMSContext
    [TestFixture]
    public class NMSContextIntegrationTestAsync : IntegrationTestFixture
    {
        [Test, Timeout(20_000)]
        public async Task TestClose()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                INMSContext context = await EstablishNMSContextAsync(testPeer);

                testPeer.ExpectClose();

                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestCreateProducer()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);
                testPeer.ExpectBegin();

                testPeer.ExpectSenderAttach();

                var producer = await context.CreateProducerAsync();

                testPeer.ExpectDetach(true, true, true);
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                await producer.CloseAsync();
                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestCreateConsumer()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);
                await context.StartAsync();

                testPeer.ExpectBegin();

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                var consumer = await context.CreateConsumerAsync(await context.GetQueueAsync("myQueue"));

                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestCreateConsumerWithEmptySelector()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);
                await context.StartAsync();

                testPeer.ExpectBegin();

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();
                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                IQueue queue = await context.GetQueueAsync("myQueue");
                await context.CreateConsumerAsync(queue, "");
                await context.CreateConsumerAsync(queue, "", noLocal: false);

                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestCreateConsumerWithNullSelector()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);
                await context.StartAsync();

                testPeer.ExpectBegin();

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();
                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                IQueue queue = await context.GetQueueAsync("myQueue");
                await context.CreateConsumerAsync(queue, null);
                await context.CreateConsumerAsync(queue, null, noLocal: false);

                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestCreateDurableConsumer()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);
                await context.StartAsync();

                testPeer.ExpectBegin();

                string topicName = "myTopic";
                ITopic topic = await context.GetTopicAsync(topicName);
                string subscriptionName = "mySubscription";

                testPeer.ExpectDurableSubscriberAttach(topicName, subscriptionName);
                testPeer.ExpectLinkFlow();

                var durableConsumer = await context.CreateDurableConsumerAsync(topic, subscriptionName, null, false);
                Assert.NotNull(durableConsumer, "MessageConsumer object was null");

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }


        [Test, Timeout(20_000)]
        public async Task TestCreateTemporaryQueue()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);

                testPeer.ExpectBegin();

                string dynamicAddress = "myTempQueueAddress";
                testPeer.ExpectTempQueueCreationAttach(dynamicAddress);

                ITemporaryQueue temporaryQueue = await context.CreateTemporaryQueueAsync();
                Assert.NotNull(temporaryQueue, "TemporaryQueue object was null");
                Assert.NotNull(temporaryQueue.QueueName, "TemporaryQueue queue name was null");
                Assert.AreEqual(dynamicAddress, temporaryQueue.QueueName, "TemporaryQueue name not as expected");

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestCreateTemporaryTopic()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);

                testPeer.ExpectBegin();

                string dynamicAddress = "myTempTopicAddress";
                testPeer.ExpectTempTopicCreationAttach(dynamicAddress);

                ITemporaryTopic temporaryTopic = await context.CreateTemporaryTopicAsync();
                Assert.NotNull(temporaryTopic, "TemporaryTopic object was null");
                Assert.NotNull(temporaryTopic.TopicName, "TemporaryTopic name was null");
                Assert.AreEqual(dynamicAddress, temporaryTopic.TopicName, "TemporaryTopic name not as expected");

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestCreateSharedConsumer()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);
                await context.StartAsync();

                testPeer.ExpectBegin();

                string topicName = "myTopic";
                ITopic topic = await context.GetTopicAsync(topicName);
                string subscriptionName = "mySubscription";

                testPeer.ExpectSharedSubscriberAttach(topicName, subscriptionName);
                testPeer.ExpectLinkFlow();

                var durableConsumer = await context.CreateSharedConsumerAsync(topic, subscriptionName, null); //, false);
                Assert.NotNull(durableConsumer, "MessageConsumer object was null");

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(20000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestCreateSharedDurableConsumer()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = await EstablishNMSContextAsync(testPeer);
                await context.StartAsync();

                testPeer.ExpectBegin();

                string topicName = "myTopic";
                ITopic topic = await context.GetTopicAsync(topicName);
                string subscriptionName = "mySubscription";

                testPeer.ExpectSharedDurableSubscriberAttach(topicName, subscriptionName);
                testPeer.ExpectLinkFlow();

                var durableConsumer = await context.CreateSharedDurableConsumerAsync(topic, subscriptionName, null); //, false);
                Assert.NotNull(durableConsumer, "MessageConsumer object was null");

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                await context.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }
    }
}