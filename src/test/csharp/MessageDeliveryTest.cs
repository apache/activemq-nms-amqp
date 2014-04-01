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
using Apache.NMS;
using Apache.NMS.Policies;
using Apache.NMS.Util;
using Apache.NMS.Amqp;
using Apache.NMS.Test;
using NUnit.Framework;

namespace Apache.NMS.Amqp.Test
{
    [TestFixture]
    public class MessageDeliveryTest : NMSTestSupport
    {
        private Uri uri = new Uri(NMSTestSupport.ReplaceEnvVar("amqp:localhost:5672"));
        private IConnectionFactory factory;
        private Connection connection;
        private ISession session;
        private IDestination destination;
        private IMessageProducer producer;
        private IMessageConsumer consumer;

        [SetUp]
        public override void SetUp()
        {
            factory = new NMSConnectionFactory(uri);
            this.connection = (Connection) factory.CreateConnection();
            session = connection.CreateSession();
            destination = SessionUtil.GetDestination(session, "my-dest; {create:always}");
            producer = session.CreateProducer(destination);
            consumer = session.CreateConsumer(destination);
        }

        [TearDown]
        public override void TearDown()
        {
            this.session = null;

            if(this.connection != null)
            {
                this.connection.Close();
                this.connection = null;
            }

            base.TearDown();
        }


        [Test]
        public void TestMessageDelivery()
        {
            connection.Start();

            IMessage message = session.CreateTextMessage("Test Message");
            producer.Send(message);

            IMessage resultMessage = consumer.Receive();

            AssertEquals(message, resultMessage);
        }

    }
}
