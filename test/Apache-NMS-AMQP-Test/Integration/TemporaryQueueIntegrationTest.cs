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
    public class TemporaryQueueIntegrationTest : IntegrationTestFixture
    {
        [Test, Timeout(20_000)]
        public void TestCantConsumeFromTemporaryQueueCreatedOnAnotherConnection()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);

                testPeer.ExpectBegin();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);

                string dynamicAddress = "myTempQueueAddress";
                testPeer.ExpectTempQueueCreationAttach(dynamicAddress);

                ITemporaryQueue temporaryQueue = session.CreateTemporaryQueue();

                IConnection connection2 = EstablishConnection(testPeer);
                testPeer.ExpectBegin();

                ISession session2 = connection2.CreateSession(AcknowledgementMode.AutoAcknowledge);

                Assert.Catch<InvalidDestinationException>(() => session2.CreateConsumer(temporaryQueue), "Should not be able to create consumer from temporary queue from another connection");
            }
        }

        [Test, Timeout(20_000)]
        public void TestCantDeleteTemporaryQueueWithConsumers()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                connection.Start();

                testPeer.ExpectBegin();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);

                string dynamicAddress = "myTempQueueAddress";
                testPeer.ExpectTempQueueCreationAttach(dynamicAddress);

                ITemporaryQueue temporaryQueue = session.CreateTemporaryQueue();

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();
                IMessageConsumer consumer = session.CreateConsumer(temporaryQueue);

                Assert.Catch<IllegalStateException>(() => temporaryQueue.Delete(), "should not be able to delete temporary queue with active consumers");

                testPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);
                consumer.Close();

                // Now it should be allowed
                testPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);
                temporaryQueue.Delete();

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }
    }
}