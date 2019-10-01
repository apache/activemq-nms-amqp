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

namespace NMS.AMQP.Test
{
    public class AmqpTestSupport
    {
        protected static string MESSAGE_NUMBER = "MessageNumber";

        protected IConnection Connection;

        protected string TestName => TestContext.CurrentContext.Test.Name;

        [TearDown]
        public void TearDown()
        {
            Connection?.Close();
        }

        protected IConnection CreateAmqpConnection()
        {
            string brokerUri = Environment.GetEnvironmentVariable("NMS_AMQP_TEST_URI") ?? "amqp://127.0.0.1:5672";
            string userName = Environment.GetEnvironmentVariable("NMS_AMQP_TEST_CU") ?? "admin";
            string password = Environment.GetEnvironmentVariable("NMS_AMQP_TEST_CPWD") ?? "admin";

            NmsConnectionFactory factory = new NmsConnectionFactory(brokerUri);
            return factory.CreateConnection(userName, password);
        }

        protected void SendToAmqQueue(int count)
        {
            IConnection amqpConnection = CreateAmqpConnection();
            ISession session = amqpConnection.CreateSession(AcknowledgementMode.AutoAcknowledge);
            IQueue queue = session.GetQueue(TestName);
            IMessageProducer producer = session.CreateProducer(queue);

            for (int i = 1; i <= count; i++)
            {
                ITextMessage message = producer.CreateTextMessage("TextMessage: " + i);
                message.Properties.SetInt(MESSAGE_NUMBER, i);
                producer.Send(message);
            }

            session.Close();
            amqpConnection.Close();
        }

        protected void AssertQueueEmpty(TimeSpan timeout)
        {
            IConnection amqpConnection = CreateAmqpConnection();
            amqpConnection.Start();
            ISession session = amqpConnection.CreateSession(AcknowledgementMode.Transactional);
            IQueue queue = session.GetQueue(TestName);
            IMessageConsumer consumer = session.CreateConsumer(queue);
            IMessage message = consumer.Receive(timeout);
            Assert.IsNull(message);

            amqpConnection.Close();
        }

        protected void AssertQueueSize(int expectedCount, TimeSpan timeout)
        {
            IConnection amqpConnection = CreateAmqpConnection();
            amqpConnection.Start();
            ISession session = amqpConnection.CreateSession(AcknowledgementMode.Transactional);
            IQueue queue = session.GetQueue(TestName);
            IMessageConsumer consumer = session.CreateConsumer(queue);

            int count = 0;
            IMessage message;
            do
            {
                message = consumer.Receive(timeout);
                if (message != null)
                    count++;
            } while (message != null);

            session.Rollback();
            amqpConnection.Close();

            Assert.AreEqual(expectedCount, count);
        }

        protected void PurgeQueue(TimeSpan timeout)
        {
            IConnection amqpConnection = CreateAmqpConnection();
            amqpConnection.Start();
            ISession session = amqpConnection.CreateSession(AcknowledgementMode.AutoAcknowledge);
            IQueue queue = session.GetQueue(TestName);
            IMessageConsumer consumer = session.CreateConsumer(queue);

            IMessage message;
            do
            {
                message = consumer.Receive(timeout);
            } while (message != null);

            amqpConnection.Close();
        }

        protected void PurgeTopic(TimeSpan timeout)
        {
            IConnection amqpConnection = CreateAmqpConnection();
            amqpConnection.Start();
            ISession session = amqpConnection.CreateSession(AcknowledgementMode.AutoAcknowledge);
            ITopic queue = session.GetTopic(TestName);
            IMessageConsumer consumer = session.CreateConsumer(queue);

            IMessage message;
            do
            {
                message = consumer.Receive(timeout);
            } while (message != null);

            amqpConnection.Close();
        }
    }
}