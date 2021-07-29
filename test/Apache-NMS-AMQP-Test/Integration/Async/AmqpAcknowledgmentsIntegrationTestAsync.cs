﻿/*
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
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amqp.Framing;
using Apache.NMS;
using Apache.NMS.AMQP.Util;
using NMS.AMQP.Test.TestAmqp;
using NUnit.Framework;

namespace NMS.AMQP.Test.Integration.Async
{
    [TestFixture]
    public class AmqpAcknowledgmentsIntegrationTestAsync : IntegrationTestFixture
    {
        [Test, Timeout(20_000)]
        public async Task TestAcknowledgeFailsAfterSessionIsClosed()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = await EstablishConnectionAsync(testPeer);
                await connection.StartAsync();

                testPeer.ExpectBegin();
                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.ClientAcknowledge);
                IQueue queue = await session.GetQueueAsync("myQueue");

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithNullContent(), count: 1);
                testPeer.ExpectEnd();

                IMessageConsumer consumer = await session.CreateConsumerAsync(queue);

                IMessage receivedMessage = await consumer.ReceiveAsync(TimeSpan.FromSeconds(6));
                Assert.NotNull(receivedMessage, "Message was not received");
                
                await session.CloseAsync();

                Assert.CatchAsync<NMSException>(async () => await receivedMessage.AcknowledgeAsync(), "Should not be able to acknowledge the message after session closed");
                
                testPeer.ExpectClose();
                await connection.CloseAsync();
                
                testPeer.WaitForAllMatchersToComplete(3000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestClientAcknowledgeMessages()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                int msgCount = 3;
                
                IConnection connection = await EstablishConnectionAsync(testPeer);
                await connection.StartAsync();

                testPeer.ExpectBegin();
                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.ClientAcknowledge);
                IQueue queue = await session.GetQueueAsync("myQueue");
                
                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithNullContent(), count: msgCount);
                
                IMessageConsumer consumer = await session.CreateConsumerAsync(queue);
                
                IMessage lastReceivedMessage = null;
                for (int i = 0; i < msgCount; i++)
                {
                    lastReceivedMessage = await consumer.ReceiveAsync();
                    Assert.NotNull(lastReceivedMessage, "Message " + i + " was not received");
                }
                
                for (int i = 0; i < msgCount; i++)
                {
                    testPeer.ExpectDispositionThatIsAcceptedAndSettled();
                }
                
                await lastReceivedMessage.AcknowledgeAsync();
                
                testPeer.WaitForAllMatchersToComplete(2000);
                
                testPeer.ExpectClose();
                await connection.CloseAsync();
                
                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestClientAcknowledgeMessagesAsync()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                int msgCount = 3;

                IConnection connection = await EstablishConnectionAsync(testPeer);
                await connection.StartAsync();

                testPeer.ExpectBegin();
                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.ClientAcknowledge);
                IQueue queue = await session.GetQueueAsync("myQueue");
                
                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithNullContent(), count: msgCount);
                
                IMessageConsumer consumer = await session.CreateConsumerAsync(queue);

                CountdownEvent latch = new CountdownEvent(3);

                IMessage lastReceivedMessage = null;
                consumer.Listener += message =>
                {
                    lastReceivedMessage = message;
                    latch.Signal();
                };
                
                Assert.True(latch.Wait(2000));
                
                for (int i = 0; i < msgCount; i++)
                {
                    testPeer.ExpectDispositionThatIsAcceptedAndSettled();
                }
                
                await lastReceivedMessage.AcknowledgeAsync();
                
                testPeer.WaitForAllMatchersToComplete(2000);
                
                testPeer.ExpectClose();
                await connection.CloseAsync();
                
                testPeer.WaitForAllMatchersToComplete(2000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestAcknowledgeIndividualMessages()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                int msgCount = 6;

                IConnection connection = await EstablishConnectionAsync(testPeer);
                await connection.StartAsync();

                testPeer.ExpectBegin();
                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.IndividualAcknowledge);
                IQueue queue = await session.GetQueueAsync("myQueue");
                
                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(
                    message: CreateMessageWithNullContent(),
                    count: msgCount,
                    drain: false,
                    nextIncomingId: 1,
                    addMessageNumberProperty: true,
                    sendDrainFlowResponse: false,
                    sendSettled: false,
                    creditMatcher: credit => Assert.Greater(credit, msgCount));
                
                IMessageConsumer consumer = await session.CreateConsumerAsync(queue);
                
                var messages = new List<IMessage>();
                for (int i = 0; i < msgCount; i++)
                {
                    IMessage message = await consumer.ReceiveAsync(TimeSpan.FromMilliseconds(3000));
                    Assert.NotNull(message, "Message " + i + " was not received");
                    messages.Add(message);
                    
                    Assert.AreEqual(i, message.Properties.GetInt(TestAmqpPeer.MESSAGE_NUMBER), "unexpected message number property");
                }
                
                Action<DeliveryState> dispositionMatcher = state => { Assert.AreEqual(state.Descriptor.Code, MessageSupport.ACCEPTED_INSTANCE.Descriptor.Code); };
                
                // Acknowledge the messages in a random order and verify the individual dispositions have expected delivery state.
                Random random = new Random();
                for (int i = 0; i < msgCount; i++)
                {
                    var message = messages[random.Next(msgCount - i)];
                    messages.Remove(message);

                    uint deliveryNumber = (uint) message.Properties.GetInt(TestAmqpPeer.MESSAGE_NUMBER) + 1;

                    testPeer.ExpectDisposition(settled: true, stateMatcher: dispositionMatcher, firstDeliveryId: deliveryNumber, lastDeliveryId: deliveryNumber);
                    
                    await message.AcknowledgeAsync();
                    
                    testPeer.WaitForAllMatchersToComplete(3000);
                }
                
                testPeer.ExpectClose();
                await connection.CloseAsync();
                
                testPeer.WaitForAllMatchersToComplete(3000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestAcknowledgeIndividualMessagesAsync()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                int msgCount = 6;

                IConnection connection = await EstablishConnectionAsync(testPeer);
                await connection.StartAsync();

                testPeer.ExpectBegin();
                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.IndividualAcknowledge);
                IQueue queue = await session.GetQueueAsync("myQueue");

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(
                    message: CreateMessageWithNullContent(),
                    count: msgCount,
                    drain: false,
                    nextIncomingId: 1,
                    addMessageNumberProperty: true,
                    sendDrainFlowResponse: false,
                    sendSettled: false,
                    creditMatcher: credit => Assert.Greater(credit, msgCount));

                IMessageConsumer consumer = await session.CreateConsumerAsync(queue);
                
                CountdownEvent latch = new CountdownEvent(msgCount);
                List<ITextMessage> messages = new List<ITextMessage>();
                consumer.Listener += message =>
                {
                    messages.Add((ITextMessage) message);
                    latch.Signal();
                };
                
                Assert.True(latch.Wait(TimeSpan.FromMilliseconds(3000)), $"Should receive: {msgCount}, but received: {messages.Count}");
                
                Action<DeliveryState> dispositionMatcher = state => { Assert.AreEqual(state.Descriptor.Code, MessageSupport.ACCEPTED_INSTANCE.Descriptor.Code); };
                
                // Acknowledge the messages in a random order and verify the individual dispositions have expected delivery state.
                Random random = new Random();
                for (int i = 0; i < msgCount; i++)
                {
                    var message = messages[random.Next(msgCount - i)];
                    messages.Remove(message);

                    uint deliveryNumber = (uint) message.Properties.GetInt(TestAmqpPeer.MESSAGE_NUMBER) + 1;

                    testPeer.ExpectDisposition(settled: true, stateMatcher: dispositionMatcher, firstDeliveryId: deliveryNumber, lastDeliveryId: deliveryNumber);
                    
                    await message.AcknowledgeAsync();
                    
                    testPeer.WaitForAllMatchersToComplete(3000);
                }
                
                testPeer.ExpectClose();
                await connection.CloseAsync();
                
                testPeer.WaitForAllMatchersToComplete(3000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestAutoAcknowledgeMessages()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                int msgCount = 6;

                IConnection connection = await EstablishConnectionAsync(testPeer);
                await connection.StartAsync();

                testPeer.ExpectBegin();
                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = await session.GetQueueAsync("myQueue");

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithNullContent(), count: msgCount);
                
                IMessageConsumer consumer = await session.CreateConsumerAsync(queue);

                for (int i = 0; i < msgCount; i++) 
                    testPeer.ExpectDispositionThatIsAcceptedAndSettled();

                for (int i = 0; i < msgCount; i++) 
                    Assert.NotNull(await consumer.ReceiveAsync(TimeSpan.FromMilliseconds(3000)), $"Message {i} not received within given timeout.");
                
                testPeer.WaitForAllMatchersToComplete(3000);
                
                testPeer.ExpectClose();
                await connection.CloseAsync();
                
                testPeer.WaitForAllMatchersToComplete(3000);
            }
        }

        [Test, Timeout(20_000)]
        public async Task TestAutoAcknowledgeMessagesAsync()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                int msgCount = 6;

                IConnection connection = await EstablishConnectionAsync(testPeer);
                await connection.StartAsync();

                testPeer.ExpectBegin();
                ISession session = await connection.CreateSessionAsync(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = await session.GetQueueAsync("myQueue");

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithNullContent(), count: msgCount);

                IMessageConsumer consumer = await session.CreateConsumerAsync(queue);

                for (int i = 0; i < msgCount; i++)
                    testPeer.ExpectDispositionThatIsAcceptedAndSettled();
                
                consumer.Listener += (message) => { };
                
                testPeer.WaitForAllMatchersToComplete(3000);
                
                testPeer.ExpectClose();
                await connection.CloseAsync();
                
                testPeer.WaitForAllMatchersToComplete(3000);
            }
        }
    }
}