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
    public class TemporaryQueueIntegrationTestAsync : IntegrationTestFixture
    {
        [Test, Timeout(20_000)]
        public async Task TestCantConsumeFromTemporaryQueueCreatedOnAnotherConnection()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);

                testPeer.ExpectBegin();
                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);

                string dynamicAddress = "myTempQueueAddress";
                testPeer.ExpectTempQueueCreationAttach(dynamicAddress);

                ITemporaryQueue temporaryQueue = await session.CreateTemporaryQueueAsync();

                IConnection connection2 = await EstablishConnectionAsync(testPeer);
                testPeer.ExpectBegin();

                ISession session2 = await connection2.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);

                Assert.CatchAsync<InvalidDestinationException>(async () => await session2.CreateConsumerAsync(temporaryQueue), "Should not be able to create consumer from temporary queue from another connection");
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestCantDeleteTemporaryQueueWithConsumers()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);
                await connection.StartAsync();

                testPeer.ExpectBegin();
                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);

                string dynamicAddress = "myTempQueueAddress";
                testPeer.ExpectTempQueueCreationAttach(dynamicAddress);

                ITemporaryQueue temporaryQueue = await session.CreateTemporaryQueueAsync();

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();
                IMessageConsumer consumer = await session.CreateConsumerAsync(temporaryQueue);

                Assert.CatchAsync<IllegalStateException>(async () => await temporaryQueue.DeleteAsync(), "should not be able to delete temporary queue with active consumers");

                testPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);
                await consumer.CloseAsync();

                // Now it should be allowed
                testPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);
                await temporaryQueue.DeleteAsync();

                testPeer.ExpectClose();
                await connection.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }
    }
}