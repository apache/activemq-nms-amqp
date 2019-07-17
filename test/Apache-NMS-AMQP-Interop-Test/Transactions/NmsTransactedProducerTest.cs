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
using NUnit.Framework;

namespace NMS.AMQP.Test.Transactions
{
    [TestFixture]
    public class NmsTransactedProducerTest : AmqpTestSupport
    {
        [Test, Timeout(60_000)]
        public void TestCreateTxSessionAndProducer()
        {
            Connection = CreateAmqpConnection();
            Assert.NotNull(Connection);
            Connection.Start();

            ISession session = Connection.CreateSession(AcknowledgementMode.Transactional);
            Assert.NotNull(session);
            Assert.True(session.Transacted);

            IQueue queue = session.GetQueue(TestName);
            IMessageProducer producer = session.CreateProducer(queue);
            Assert.NotNull(producer);
        }

        [Test, Timeout(60_000)]
        public void TestTXProducerReusesMessage()
        {
            PurgeQueue(TimeSpan.FromMilliseconds(1000));

            int msgCount = 10;

            Connection = CreateAmqpConnection();
            Connection.Start();

            ISession session = Connection.CreateSession(AcknowledgementMode.Transactional);
            ISession nonTxSession = Connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
            IQueue queue = session.GetQueue(TestName);

            IMessageConsumer consumer = nonTxSession.CreateConsumer(queue);
            IMessageProducer producer = session.CreateProducer(queue);

            ITextMessage textMessage = session.CreateTextMessage();
            for (int i = 0; i < msgCount; i++)
            {
                textMessage.Text = "Sequence: " + i;
                producer.Send(textMessage);
            }

            IMessage msg = consumer.Receive(TimeSpan.FromMilliseconds(1000));
            Assert.IsNull(msg);
            session.Commit();
            msg = consumer.Receive(TimeSpan.FromMilliseconds(1000));
            Assert.NotNull(msg);
        }

        [Test, Timeout(60_000)]
        public void TestTXProducerCommitsAreQueued()
        {
            PurgeQueue(TimeSpan.FromMilliseconds(500));

            int msgCount = 10;

            Connection = CreateAmqpConnection();
            Connection.Start();

            ISession session = Connection.CreateSession(AcknowledgementMode.Transactional);
            ISession nonTxSession = Connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
            IQueue queue = session.GetQueue(TestName);

            IMessageConsumer consumer = nonTxSession.CreateConsumer(queue);
            IMessageProducer producer = session.CreateProducer(queue);

            for (int i = 0; i < msgCount; i++)
            {
                producer.Send(session.CreateTextMessage());
            }

            IMessage msg = consumer.Receive(TimeSpan.FromMilliseconds(1000));
            Assert.IsNull(msg);
            session.Commit();
            
            Connection.Close();
            
            AssertQueueSize(msgCount, TimeSpan.FromMilliseconds(1000));
        }

        [Test, Timeout(60_000)]
        public void TestTXProducerRollbacksNotQueued()
        {
            PurgeQueue(TimeSpan.FromMilliseconds(500));

            int msgCount = 10;

            Connection = CreateAmqpConnection();
            Connection.Start();

            ISession session = Connection.CreateSession(AcknowledgementMode.Transactional);
            IQueue queue = session.GetQueue(TestName);
            
            IMessageProducer producer = session.CreateProducer(queue);

            for (int i = 0; i < msgCount; i++)
            {
                producer.Send(session.CreateTextMessage());
            }            
            session.Rollback();
            
            Connection.Close();
            
            AssertQueueEmpty(TimeSpan.FromMilliseconds(1000));
        }
        

        [TearDown]
        public void Cleanup()
        {
            PurgeQueue(TimeSpan.FromMilliseconds(500));
        }
    }
}