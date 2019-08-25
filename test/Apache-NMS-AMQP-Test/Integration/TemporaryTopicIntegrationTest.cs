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
    public class TemporaryTopicIntegrationTest : IntegrationTestFixture
    {

        [Test, Timeout(20_000)]
        public void TestCantConsumeFromTemporaryTopicCreatedOnAnotherConnection()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);

                testPeer.ExpectBegin();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);

                string dynamicAddress = "myTempTopicAddress";
                testPeer.ExpectTempTopicCreationAttach(dynamicAddress);

                ITemporaryTopic topic = session.CreateTemporaryTopic();

                IConnection connection2 = EstablishConnection(testPeer);
                testPeer.ExpectBegin();

                ISession session2 = connection2.CreateSession(AcknowledgementMode.AutoAcknowledge);

                Assert.Catch<InvalidDestinationException>(() => session2.CreateConsumer(topic), "Should not be able to create consumer from temporary topic from another connection");
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

                string dynamicAddress = "myTempTopicAddress";
                testPeer.ExpectTempTopicCreationAttach(dynamicAddress);

                ITemporaryTopic topic = session.CreateTemporaryTopic();

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();
                IMessageConsumer consumer = session.CreateConsumer(topic);

                Assert.Catch<IllegalStateException>(() => topic.Delete(), "should not be able to delete temporary topic with active consumers");

                testPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);
                consumer.Close();

                // Now it should be allowed
                testPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);
                topic.Delete();

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }
    }
}