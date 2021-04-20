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
using System.Threading.Tasks;
using Apache.NMS;
using NMS.AMQP.Test.TestAmqp;
using NUnit.Framework;

namespace NMS.AMQP.Test.Integration.Async
{
    [TestFixture]
    public class SubscriptionsIntegrationTestAsync : IntegrationTestFixture
    {
        [Test, Timeout(20_000)]
        public async Task TestUnsubscribeExclusiveDurableSubWhileActiveThenInactive()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);
                await connection.StartAsync();

                testPeer.ExpectBegin();
                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);

                String topicName = "myTopic";
                ITopic dest = await session.GetTopicAsync("myTopic");
                String subscriptionName = "mySubscription";
                
                // Attach the durable exclusive receiver
                testPeer.ExpectDurableSubscriberAttach(topicName: topicName, subscriptionName: subscriptionName);
                testPeer.ExpectLinkFlow();
                
                IMessageConsumer consumer = await session.CreateDurableConsumerAsync(dest, subscriptionName, null, false);
                Assert.NotNull(consumer, "TopicSubscriber object was null");
                
                // Now try to unsubscribe, should fail
                Assert.CatchAsync<NMSException>(async () => session.DeleteDurableConsumer(subscriptionName));
                
                // Now close the subscriber
                testPeer.ExpectDetach(expectClosed: false, sendResponse: true, replyClosed: false);
                
                await consumer.CloseAsync();
                
                // Try to unsubscribe again, should work now
                testPeer.ExpectDurableSubUnsubscribeNullSourceLookup(failLookup: false, shared: false, subscriptionName: subscriptionName, topicName: topicName, hasClientId: true);
                testPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);
                
                session.DeleteDurableConsumer(subscriptionName);
                
                testPeer.WaitForAllMatchersToComplete(1000);
                
                testPeer.ExpectClose();
                await connection.CloseAsync();
                
                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }
    }
}