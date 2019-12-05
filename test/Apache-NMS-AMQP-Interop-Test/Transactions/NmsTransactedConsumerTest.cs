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
using System.Collections.Generic;
using Apache.NMS;
using Apache.NMS.AMQP;
using Apache.NMS.AMQP.Message;
using Apache.NMS.Util;
using Moq;
using NUnit.Framework;

namespace NMS.AMQP.Test.Transactions
{
    public class NmsTransactedConsumerTest : AmqpTestSupport
    {
        [Test, Timeout(60_000)]
        public void TestCreateConsumerFromTxSession()
        {
            Connection = CreateAmqpConnection();
            Connection.Start();

            ISession session = Connection.CreateSession(AcknowledgementMode.Transactional);
            Assert.NotNull(session);
            Assert.True(session.Transacted);

            IQueue queue = session.GetQueue(TestName);
            IMessageConsumer consumer = session.CreateConsumer(queue);
            Assert.NotNull(consumer);
        }

        [Test, Timeout(60_000)]
        public void TestConsumedInTxAreAcked()
        {
            Connection = CreateAmqpConnection();
            Connection.Start();

            SendToAmqQueue(1);

            ISession session = Connection.CreateSession(AcknowledgementMode.Transactional);
            IQueue queue = session.GetQueue(TestName);
            IMessageConsumer consumer = session.CreateConsumer(queue);
            IMessage message = consumer.Receive(TimeSpan.FromMinutes(5000));
            Assert.NotNull(message);
            session.Commit();
            session.Close();

            AssertQueueEmpty(TimeSpan.FromMilliseconds(500));
        }

        [Test, Timeout(60_000)]
        public void TestRollbackReceivedMessageAndClose()
        {
            Connection = CreateAmqpConnection();
            Connection.Start();

            ISession session = Connection.CreateSession(AcknowledgementMode.Transactional);
            IQueue queue = session.GetQueue(TestName);
            IMessageProducer producer = session.CreateProducer(queue);
            producer.Send(session.CreateTextMessage("TestMessage-0"));
            producer.Close();
            session.Commit();

            IMessageConsumer consumer = session.CreateConsumer(queue);
            IMessage msg = consumer.Receive(TimeSpan.FromSeconds(3));
            Assert.NotNull(msg);
            session.Rollback();

            Connection.Close();

            AssertQueueSize(1, TimeSpan.FromMilliseconds(500));
        }

        [Test, Timeout(60_000)]
        public void TestReceiveAndRollback()
        {
            Connection = CreateAmqpConnection();
            Connection.Start();

            SendToAmqQueue(2);

            ISession session = Connection.CreateSession(AcknowledgementMode.Transactional);
            IQueue queue = session.GetQueue(TestName);
            IMessageConsumer consumer = session.CreateConsumer(queue);

            IMessage message = consumer.Receive(TimeSpan.FromMilliseconds(3000));
            Assert.NotNull(message);
            Assert.AreEqual(1, message.Properties.GetInt(MESSAGE_NUMBER));

            session.Commit();

            // rollback so we can get that last message again.
            message = consumer.Receive(TimeSpan.FromMilliseconds(3000));
            Assert.NotNull(message);
            Assert.AreEqual(2, message.Properties.GetInt(MESSAGE_NUMBER));
            session.Rollback();

            // Consume again.. the prev message should get redelivered.
            message = consumer.Receive(TimeSpan.FromMilliseconds(3000));
            Assert.NotNull(message, "Should have re-received the message again!");
            Assert.AreEqual(2, message.Properties.GetInt(MESSAGE_NUMBER));
            session.Commit();

            AssertQueueEmpty(TimeSpan.FromMilliseconds(500));
        }

        [Test, Timeout(60_000)]
        public void TestReceiveTwoThenRollback()
        {
            Connection = CreateAmqpConnection();
            Connection.Start();

            SendToAmqQueue(2);

            ISession session = Connection.CreateSession(AcknowledgementMode.Transactional);
            IQueue queue = session.GetQueue(TestName);
            IMessageConsumer consumer = session.CreateConsumer(queue);

            IMessage message = consumer.Receive(TimeSpan.FromMilliseconds(3000));
            Assert.NotNull(message);
            message = consumer.Receive(TimeSpan.FromMilliseconds(3000));
            Assert.NotNull(message);

            session.Rollback();

            // Consume again.. the prev message should get redelivered.
            message = consumer.Receive(TimeSpan.FromMilliseconds(3000));
            Assert.NotNull(message, "Should have re-received the message again!");
            Assert.AreEqual(1, message.Properties.GetInt(MESSAGE_NUMBER));
            message = consumer.Receive(TimeSpan.FromMilliseconds(3000));
            Assert.NotNull(message, "Should have re-received the message again!");
            Assert.AreEqual(2, message.Properties.GetInt(MESSAGE_NUMBER));
            session.Commit();

            AssertQueueEmpty(TimeSpan.FromMilliseconds(500));
        }

        [Test, Timeout(60_000)]
        public void TestReceiveTwoThenCloseSessionToRollback()
        {
            Connection = CreateAmqpConnection();
            Connection.Start();

            SendToAmqQueue(2);

            ISession session = Connection.CreateSession(AcknowledgementMode.Transactional);
            IQueue queue = session.GetQueue(TestName);
            IMessageConsumer consumer = session.CreateConsumer(queue);

            IMessage message = consumer.Receive(TimeSpan.FromMilliseconds(3000));
            Assert.NotNull(message);

            message = consumer.Receive(TimeSpan.FromMilliseconds(3000));
            Assert.NotNull(message);

            session.Rollback();

            // Consume again.. the prev message should get redelivered.
            message = consumer.Receive(TimeSpan.FromMilliseconds(3000));
            Assert.NotNull(message);
            message = consumer.Receive(TimeSpan.FromMilliseconds(3000));
            Assert.NotNull(message);

            session.Close();

            AssertQueueSize(2, TimeSpan.FromMilliseconds(1000));
        }

        [Test, Timeout(60_000)]
        public void TestReceiveSomeThenRollback()
        {
            Connection = CreateAmqpConnection();
            Connection.Start();

            int totalCount = 5;
            int consumeBeforeRollback = 2;
            SendToAmqQueue(totalCount);

            AssertQueueSize(totalCount, TimeSpan.FromMilliseconds(1000));

            ISession session = Connection.CreateSession(AcknowledgementMode.Transactional);
            IQueue queue = session.GetQueue(TestName);
            IMessageConsumer consumer = session.CreateConsumer(queue);

            for (int i = 1; i <= consumeBeforeRollback; i++)
            {
                IMessage message = consumer.Receive(TimeSpan.FromMilliseconds(3000));
                Assert.NotNull(message);
                Assert.AreEqual(i, message.Properties.GetInt(MESSAGE_NUMBER), "Unexpected message number");
            }

            session.Rollback();

            // Consume again.. the previously consumed messages should get delivered
            // again after the rollback and then the remainder should follow
            List<int> messageNumbers = new List<int>();
            for (int i = 1; i <= totalCount; i++)
            {
                IMessage message = consumer.Receive(TimeSpan.FromMilliseconds(3000));
                Assert.NotNull(message, $"Failed to receive message: {i}");
                messageNumbers.Add(message.Properties.GetInt(MESSAGE_NUMBER));
            }

            session.Commit();

            Assert.AreEqual(totalCount, messageNumbers.Count, "Unexpected size of list");

            for (int i = 0; i < messageNumbers.Count; i++)
            {
                Assert.AreEqual(i + 1, messageNumbers[i], "Unexpected order of messages: ");
            }

            AssertQueueEmpty(TimeSpan.FromMilliseconds(1000));
        }

        [Test, Timeout(60_000)]
        public void TestCloseConsumerBeforeCommit()
        {
            Connection = CreateAmqpConnection();
            Connection.Start();

            SendToAmqQueue(2);

            ISession session = Connection.CreateSession(AcknowledgementMode.Transactional);
            IQueue queue = session.GetQueue(TestName);
            IMessageConsumer consumer = session.CreateConsumer(queue);

            IMessage message = consumer.Receive(TimeSpan.FromSeconds(5));
            Assert.NotNull(message);
            Assert.AreEqual(1, message.Properties.GetInt(MESSAGE_NUMBER));
            consumer.Close();

            session.Commit();

            // Create a new consumer
            consumer = session.CreateConsumer(queue);
            message = consumer.Receive(TimeSpan.FromSeconds(5));
            Assert.NotNull(message);
            Assert.AreEqual(2, message.Properties.GetInt(MESSAGE_NUMBER));

            session.Commit();

            AssertQueueEmpty(TimeSpan.FromMilliseconds(1000));
        }

        [Test, Timeout(60_000)]
        public void TestNMSRedelivered()
        {
            SendToAmqQueue(1);

            Connection = CreateAmqpConnection();
            ISession session = Connection.CreateSession(AcknowledgementMode.Transactional);
            IQueue queue = session.GetQueue(TestName);
            IMessageConsumer consumer = session.CreateConsumer(queue);
            Connection.Start();

            // we receive a message...it should be delivered once and not be Re-delivered.
            IMessage message = consumer.Receive(TimeSpan.FromSeconds(5));
            Assert.NotNull(message);

            Assert.IsFalse(message.NMSRedelivered);

            session.Rollback();

            // we receive again a message
            message = consumer.Receive(TimeSpan.FromSeconds(5));
            Assert.NotNull(message);
            Assert.IsTrue(message.NMSRedelivered);
            session.Commit();
        }

        [Test, Timeout(60_000)]
        public void TestSessionTransactedCommitWithLocalPriorityReordering()
        {
            Mock<INmsConnectionListener> mockConnectionListener = new Mock<INmsConnectionListener>();
            CountDownLatch messagesArrived = new CountDownLatch(4);
            mockConnectionListener
                .Setup(listener => listener.OnInboundMessage(It.IsAny<NmsMessage>()))
                .Callback(() => { messagesArrived.countDown(); });

            Connection = CreateAmqpConnection();
            ((NmsConnection) Connection).AddConnectionListener(mockConnectionListener.Object);
            ISession session = Connection.CreateSession(AcknowledgementMode.Transactional);
            IQueue queue = session.GetQueue(TestName);
            Connection.Start();

            IMessageProducer producer = session.CreateProducer(queue);
            for (int i = 1; i <= 2; i++)
            {
                ITextMessage msg = session.CreateTextMessage("TestMessage" + i);
                msg.Properties.SetInt(MESSAGE_NUMBER, i);
                producer.Send(msg, MsgDeliveryMode.NonPersistent, NMSConstants.defaultPriority, NMSConstants.defaultTimeToLive);
            }

            session.Commit();

            // Receive first message.
            IMessageConsumer consumer = session.CreateConsumer(queue);
            IMessage message = consumer.Receive(TimeSpan.FromSeconds(5));
            Assert.NotNull(message);
            Assert.AreEqual(1, message.Properties.GetInt(MESSAGE_NUMBER));
            Assert.AreEqual(NMSConstants.defaultPriority, message.NMSPriority);

            // Send a couple higher priority, expect them to 'overtake' upon arrival at the consumer.
            for (int i = 3; i <= 4; i++)
            {
                ITextMessage msg = session.CreateTextMessage("TestMessage" + i);
                msg.Properties.SetInt(MESSAGE_NUMBER, i);
                producer.Send(msg, MsgDeliveryMode.NonPersistent, MsgPriority.High, NMSConstants.defaultTimeToLive);
            }

            session.Commit();

            // Wait for them all to arrive at the consumer
            Assert.True(messagesArrived.@await(TimeSpan.FromSeconds(5)));

            // Receive the other messages. Expect higher priority messages first.
            message = consumer.Receive(TimeSpan.FromSeconds(5));
            Assert.NotNull(message);
            Assert.AreEqual(3, message.Properties.GetInt(MESSAGE_NUMBER));
            Assert.AreEqual(MsgPriority.High, message.NMSPriority);

            message = consumer.Receive(TimeSpan.FromSeconds(5));
            Assert.NotNull(message);
            Assert.AreEqual(4, message.Properties.GetInt(MESSAGE_NUMBER));
            Assert.AreEqual(MsgPriority.High, message.NMSPriority);

            message = consumer.Receive(TimeSpan.FromSeconds(5));
            Assert.NotNull(message);
            Assert.AreEqual(2, message.Properties.GetInt(MESSAGE_NUMBER));
            Assert.AreEqual(NMSConstants.defaultPriority, message.NMSPriority);

            session.Commit();

            // Send a couple messages to check the session still works.
            for (int i = 5; i <= 6; i++)
            {
                ITextMessage msg = session.CreateTextMessage("TestMessage" + i);
                msg.Properties.SetInt(MESSAGE_NUMBER, i);
                producer.Send(msg, MsgDeliveryMode.NonPersistent, NMSConstants.defaultPriority, NMSConstants.defaultTimeToLive);
            }

            session.Commit();
            session.Close();
        }

        [Test, Timeout(60_000)]
        public void TestSingleConsumedMessagePerTxCase()
        {
            Connection = CreateAmqpConnection();
            Connection.Start();
            ISession session = Connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
            IQueue queue = session.GetQueue(TestName);
            IMessageProducer producer = session.CreateProducer(queue);

            for (int i = 0; i < 1000; i++)
            {
                ITextMessage msg = session.CreateTextMessage("TestMessage" + i);
                producer.Send(msg, MsgDeliveryMode.Persistent, NMSConstants.defaultPriority, NMSConstants.defaultTimeToLive);
            }

            session.Close();

            int counter = 0;
            session = Connection.CreateSession(AcknowledgementMode.Transactional);
            IMessageConsumer consumer = session.CreateConsumer(queue);
            do
            {
                IMessage message = consumer.Receive(TimeSpan.FromSeconds(1));
                if (message != null)
                {
                    counter++;
                    session.Commit();
                }
            } while (counter < 1000);

            session.Close();
        }

        [Test, Timeout(60_000)]
        public void TestConsumeAllMessagesInSingleTxCase()
        {
            Connection = CreateAmqpConnection();
            Connection.Start();

            ISession session = Connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
            IQueue queue = session.GetQueue(TestName);
            IMessageProducer producer = session.CreateProducer(queue);

            for (int i = 0; i < 1000; i++)
            {
                ITextMessage msg = session.CreateTextMessage("TestMessage" + i);
                producer.Send(msg, MsgDeliveryMode.Persistent, NMSConstants.defaultPriority, NMSConstants.defaultTimeToLive);
            }

            session.Close();

            int counter = 0;
            session = Connection.CreateSession(AcknowledgementMode.Transactional);
            IMessageConsumer consumer = session.CreateConsumer(queue);
            do
            {
                IMessage message = consumer.Receive(TimeSpan.FromSeconds(1));
                if (message != null)
                {
                    counter++;
                }
            } while (counter < 1000);

            session.Commit();

            AssertQueueEmpty(TimeSpan.FromMilliseconds(1000));

            session.Close();
        }

        [Test, Timeout(60_000)]
        public void TestConsumerClosesAfterItsTXCommits()
        {
            Connection = CreateAmqpConnection();
            Connection.Start();

            ISession session = Connection.CreateSession(AcknowledgementMode.Transactional);
            IQueue queue = session.GetQueue(TestName);

            // Send a message that will be rolled back.
            ISession senderSession = Connection.CreateSession(AcknowledgementMode.Transactional);
            IMessageProducer producer = senderSession.CreateProducer(queue);
            producer.Send(senderSession.CreateMessage());
            senderSession.Commit();
            senderSession.Close();

            // Consumer the message in a transaction and then roll it back
            ISession txSession = Connection.CreateSession(AcknowledgementMode.Transactional);
            IMessageConsumer consumer = txSession.CreateConsumer(queue);
            IMessage message = consumer.Receive(TimeSpan.FromMilliseconds(1000));
            Assert.NotNull(message, "Consumer didn't receive the message");
            txSession.Rollback();
            consumer.Close();

            // Create an auto acknowledge session and consumer normally.
            ISession nonTxSession = Connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
            consumer = nonTxSession.CreateConsumer(queue);
            message = consumer.Receive(TimeSpan.FromMilliseconds(1000));
            Assert.NotNull(message, "Receiver didn't receive the message");
            consumer.Close();
        }

        [Test, Timeout(60_000)]
        public void TestConsumerMessagesInOrder()
        {
            int messageCount = 4;

            IConnection consumingConnection = CreateAmqpConnection();
            IConnection producingConnection = CreateAmqpConnection();

            ISession consumerSession = consumingConnection.CreateSession(AcknowledgementMode.Transactional);
            IQueue queue = consumerSession.GetQueue(TestName);
            IMessageConsumer consumer = consumerSession.CreateConsumer(queue);

            ISession producerSession = producingConnection.CreateSession(AcknowledgementMode.Transactional);
            IMessageProducer producer = producerSession.CreateProducer(queue);

            try
            {
                for (int j = 0; j < messageCount; j++)
                {
                    ITextMessage message = producerSession.CreateTextMessage("TestMessage + " + j);
                    message.Properties.SetInt(MESSAGE_NUMBER, j);
                    producer.Send(message);
                }

                producerSession.Commit();

                consumingConnection.Start();

                for (int j = 0; j < messageCount; j++)
                {
                    IMessage message = consumer.Receive(TimeSpan.FromMilliseconds(1000));
                    Assert.NotNull(message);
                    int messageNumber = message.Properties.GetInt(MESSAGE_NUMBER);
                    Assert.AreEqual(j, messageNumber, $"Out of order, expected {j} but got {messageNumber}");
                }

                consumerSession.Commit();
            }
            finally
            {
                consumingConnection.Close();
                producerSession.Close();
            }
        }

        [TearDown]
        public void Cleanup()
        {
            PurgeQueue(TimeSpan.FromMilliseconds(500));
        }
    }
}