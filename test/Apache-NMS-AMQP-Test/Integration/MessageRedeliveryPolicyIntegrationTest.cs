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
using System.Threading;
using Amqp.Framing;
using Apache.NMS;
using Apache.NMS.AMQP.Policies;
using Apache.NMS.Policies;
using NMS.AMQP.Test.TestAmqp;
using NUnit.Framework;

namespace NMS.AMQP.Test.Integration
{
    [TestFixture]
    public class MessageRedeliveryPolicyIntegrationTest : IntegrationTestFixture
    {
        [Test, Timeout(20_000)]
        public void TestIncomingDeliveryCountExceededMessageGetsRejected()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                int initialRedeliveryDelay = 1000;
                int clockResolution = 15;
                connection.RedeliveryPolicy = new DefaultRedeliveryPolicy { MaximumRedeliveries = 1, InitialRedeliveryDelay = initialRedeliveryDelay};
                connection.Start();

                testPeer.ExpectBegin();

                ISession session = connection.CreateSession(AcknowledgementMode.IndividualAcknowledge);
                IQueue queue = session.GetQueue("myQueue");

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: new Amqp.Message() { BodySection = new AmqpValue() { Value = "hello" } });
                testPeer.ExpectDispositionThatIsModifiedFailedAndSettled();

                IMessageConsumer consumer = session.CreateConsumer(queue);

                IMessage m = consumer.Receive(TimeSpan.FromMilliseconds(3000));
                Assert.NotNull(m, "Message should have been received");
                Assert.IsInstanceOf<ITextMessage>(m);
                session.Recover();

                DateTime startTimer = DateTime.UtcNow;
                m = consumer.Receive(TimeSpan.FromMilliseconds(3000));
                Assert.That(DateTime.UtcNow.Subtract(startTimer).TotalMilliseconds, Is.GreaterThanOrEqualTo(initialRedeliveryDelay - clockResolution));

                Assert.NotNull(m, "Message should have been received");
                Assert.IsInstanceOf<ITextMessage>(m);
                session.Recover();
                
                // Verify the message is no longer there. Will drain to be sure there are no messages.
                Assert.IsNull(consumer.Receive(TimeSpan.FromMilliseconds(10)), "Message should not have been received");

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(3000);
            }
        }
        
        [Test, Timeout(20_000)]
        public void TestIncomingDeliveryCountExceededMessageGetsRejectedAsync()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                int initialRedeliveryDelay = 1000;
                connection.RedeliveryPolicy = new DefaultRedeliveryPolicy { MaximumRedeliveries = 1, InitialRedeliveryDelay = initialRedeliveryDelay};
                connection.Start();

                testPeer.ExpectBegin();

                ISession session = connection.CreateSession(AcknowledgementMode.IndividualAcknowledge);
                IQueue queue = session.GetQueue("myQueue");
                
                
                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: new Amqp.Message() { BodySection = new AmqpValue() { Value = "hello" } });
                testPeer.ExpectDispositionThatIsModifiedFailedAndSettled();

                IMessageConsumer consumer = session.CreateConsumer(queue);

                CountdownEvent success = new CountdownEvent(2);

                consumer.Listener += m =>
                {
                        session.Recover();
                        success.Signal();
                };
                
                Assert.IsTrue(success.Wait(TimeSpan.FromSeconds(3)), "Didn't get expected messages");
                
                testPeer.ExpectClose();
                connection.Close();
                
                testPeer.WaitForAllMatchersToComplete(3000);
            }
        }
    }
}