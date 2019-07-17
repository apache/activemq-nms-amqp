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
using Apache.NMS;
using Apache.NMS.AMQP;
using NUnit.Framework;

namespace NMS.AMQP.Test.Transactions
{
    [TestFixture]
    public class NmsTransactedSessionTest : AmqpTestSupport
    {
        [Test, Timeout(60_000)]
        public void TestCreateTxSession()
        {
            Connection = CreateAmqpConnection();
            Assert.NotNull(Connection);
            Connection.Start();

            ISession session = Connection.CreateSession(AcknowledgementMode.Transactional);
            Assert.NotNull(session);
            Assert.True(session.Transacted);

            session.Close();
        }

        [Test, Timeout(60_000)]
        public void TestCommitOnSessionWithNoWork()
        {
            Connection = CreateAmqpConnection();
            Assert.NotNull(Connection);
            Connection.Start();

            ISession session = Connection.CreateSession(AcknowledgementMode.Transactional);
            Assert.NotNull(session);
            Assert.True(session.Transacted);

            session.Commit();
        }

        [Test, Timeout(60_000)]
        public void TestRollbackOnSessionWithNoWork()
        {
            Connection = CreateAmqpConnection();
            Assert.NotNull(Connection);
            Connection.Start();

            ISession session = Connection.CreateSession(AcknowledgementMode.Transactional);
            Assert.NotNull(session);
            Assert.True(session.Transacted);

            session.Rollback();
        }

        [Test, Timeout(60_000)]
        public void TestCloseSessionRollsBack()
        {
            Connection = CreateAmqpConnection();
            Connection.Start();

            SendToAmqQueue(2);

            ISession session = Connection.CreateSession(AcknowledgementMode.Transactional);
            IQueue queue = session.GetQueue(TestName);
            IMessageConsumer consumer = session.CreateConsumer(queue);
            IMessage message = consumer.Receive(TimeSpan.FromMinutes(5000));
            Assert.NotNull(message);
            consumer.Receive(TimeSpan.FromMinutes(5000));
            Assert.NotNull(message);

            session.Close();

            session = Connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
            queue = session.GetQueue(TestName);
            consumer = session.CreateConsumer(queue);
            
            consumer.Receive(TimeSpan.FromMinutes(5000));
            Assert.NotNull(message);
            consumer.Receive(TimeSpan.FromMinutes(5000));
            Assert.NotNull(message);
        }

        [Test, Timeout(60_000)]
        public void TestRollbackSentMessagesThenConsumeWithTopic()
        {
            DoRollbackSentMessagesThenConsumeTestImpl(true);
        }

        [Test, Timeout(60_000)]
        public void TestRollbackSentMessagesThenConsumeWithQueue()
        {
            DoRollbackSentMessagesThenConsumeTestImpl(false);
        }

        private void DoRollbackSentMessagesThenConsumeTestImpl(bool topic)
        {
            Connection = CreateAmqpConnection();
            Connection.Start();

            ISession session = Connection.CreateSession(AcknowledgementMode.Transactional);
            IDestination destination = null;
            if (topic)
                destination = new NmsTopic(nameof(DoRollbackSentMessagesThenConsumeTestImpl));
            else
                destination = new NmsQueue(nameof(DoRollbackSentMessagesThenConsumeTestImpl));

            IMessageProducer producer = session.CreateProducer(destination);
            IMessageConsumer consumer = session.CreateConsumer(destination);

            ITextMessage message = null;
            int messageCount = 3;
            for (int i = 1; i <= messageCount; i++)
            {
                message = session.CreateTextMessage("Message " + i);
                producer.Send(message);
            }

            session.Rollback();

            // Should not consume any messages since rollback() was called
            message = consumer.Receive(TimeSpan.FromMilliseconds(300)) as ITextMessage;
            Assert.IsNull(message, "Received unexpected message");

            // Send messages and call commit
            for (int i = 1; i <= messageCount; i++)
            {
                message = session.CreateTextMessage("Message " + i);
                producer.Send(message);
            }

            session.Commit();

            // consume all messages
            for (int i = 1; i <= messageCount; i++)
            {
                message = consumer.Receive(TimeSpan.FromSeconds(3)) as ITextMessage;
                Assert.IsTrue(message.Text.EndsWith(i.ToString()));
                Assert.IsNotNull(message, "Receive() returned null, message " + i + " was not received");
            }

            session.Commit();
        }
    }
}