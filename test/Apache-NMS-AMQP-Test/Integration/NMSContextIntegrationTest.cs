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
    // Adapted from SessionIntegrationTest to use NMSContext
    [TestFixture]
    public class NMSContextIntegrationTest : IntegrationTestFixture
    {
        [Test, Timeout(20_000)]
        public void TestClose()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                INMSContext context = EstablishNMSContext(testPeer);

                testPeer.ExpectClose();

                context.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestCreateProducer()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = EstablishNMSContext(testPeer);
                testPeer.ExpectBegin();

                testPeer.ExpectSenderAttach();

                var producer = context.CreateProducer();

                testPeer.ExpectDetach(true, true, true);
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                producer.Close();
                context.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestCreateConsumer()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = EstablishNMSContext(testPeer);
                context.Start();

                testPeer.ExpectBegin();

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                var consumer = context.CreateConsumer(context.GetQueue("myQueue"));

                context.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestCreateConsumerWithEmptySelector()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = EstablishNMSContext(testPeer);
                context.Start();

                testPeer.ExpectBegin();

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();
                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                IQueue queue = context.GetQueue("myQueue");
                context.CreateConsumer(queue, "");
                context.CreateConsumer(queue, "", noLocal: false);

                context.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestCreateConsumerWithNullSelector()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = EstablishNMSContext(testPeer);
                context.Start();

                testPeer.ExpectBegin();

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();
                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();
                testPeer.ExpectEnd();
                testPeer.ExpectClose();

                IQueue queue = context.GetQueue("myQueue");
                context.CreateConsumer(queue, null);
                context.CreateConsumer(queue, null, noLocal: false);

                context.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestCreateDurableConsumer()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = EstablishNMSContext(testPeer);
                context.Start();

                testPeer.ExpectBegin();

                string topicName = "myTopic";
                ITopic topic = context.GetTopic(topicName);
                string subscriptionName = "mySubscription";

                testPeer.ExpectDurableSubscriberAttach(topicName, subscriptionName);
                testPeer.ExpectLinkFlow();

                var durableConsumer = context.CreateDurableConsumer(topic, subscriptionName, null, false);
                Assert.NotNull(durableConsumer, "MessageConsumer object was null");

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                context.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }


        [Test, Timeout(20_000)]
        public void TestCreateTemporaryQueue()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = EstablishNMSContext(testPeer);

                testPeer.ExpectBegin();

                string dynamicAddress = "myTempQueueAddress";
                testPeer.ExpectTempQueueCreationAttach(dynamicAddress);

                ITemporaryQueue temporaryQueue = context.CreateTemporaryQueue();
                Assert.NotNull(temporaryQueue, "TemporaryQueue object was null");
                Assert.NotNull(temporaryQueue.QueueName, "TemporaryQueue queue name was null");
                Assert.AreEqual(dynamicAddress, temporaryQueue.QueueName, "TemporaryQueue name not as expected");

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                context.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestCreateTemporaryTopic()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = EstablishNMSContext(testPeer);

                testPeer.ExpectBegin();

                string dynamicAddress = "myTempTopicAddress";
                testPeer.ExpectTempTopicCreationAttach(dynamicAddress);

                ITemporaryTopic temporaryTopic = context.CreateTemporaryTopic();
                Assert.NotNull(temporaryTopic, "TemporaryTopic object was null");
                Assert.NotNull(temporaryTopic.TopicName, "TemporaryTopic name was null");
                Assert.AreEqual(dynamicAddress, temporaryTopic.TopicName, "TemporaryTopic name not as expected");

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                context.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestCreateSharedConsumer()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = EstablishNMSContext(testPeer);
                context.Start();

                testPeer.ExpectBegin();

                string topicName = "myTopic";
                ITopic topic = context.GetTopic(topicName);
                string subscriptionName = "mySubscription";

                testPeer.ExpectSharedSubscriberAttach(topicName, subscriptionName);
                testPeer.ExpectLinkFlow();

                var durableConsumer = context.CreateSharedConsumer(topic, subscriptionName, null); //, false);
                Assert.NotNull(durableConsumer, "MessageConsumer object was null");

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                context.Close();

                testPeer.WaitForAllMatchersToComplete(20000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestCreateSharedDurableConsumer()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var context = EstablishNMSContext(testPeer);
                context.Start();

                testPeer.ExpectBegin();

                string topicName = "myTopic";
                ITopic topic = context.GetTopic(topicName);
                string subscriptionName = "mySubscription";

                testPeer.ExpectSharedDurableSubscriberAttach(topicName, subscriptionName);
                testPeer.ExpectLinkFlow();

                var durableConsumer = context.CreateSharedDurableConsumer(topic, subscriptionName, null); //, false);
                Assert.NotNull(durableConsumer, "MessageConsumer object was null");

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                context.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }
    }
}