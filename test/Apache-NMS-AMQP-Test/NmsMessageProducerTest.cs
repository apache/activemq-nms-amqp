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
using Apache.NMS.AMQP.Meta;
using Apache.NMS.AMQP.Util;
using NMS.AMQP.Test.Provider.Mock;
using NUnit.Framework;

namespace NMS.AMQP.Test
{
    [TestFixture]
    public class NmsMessageProducerTest
    {
        private readonly MockRemotePeer remotePeer = new MockRemotePeer();
        private NmsConnectionInfo connectionInfo;
        private NmsConnection connection;
        private ISession session;

        [SetUp]
        public void SetUp()
        {
            remotePeer.Start();
            connectionInfo = new NmsConnectionInfo(new NmsConnectionId("ID:TEST:1"));
            connection = CreateConnectionToMockProvider();
            session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
        }

        [TearDown]
        public void TearDown()
        {
            remotePeer?.Terminate();
            connection?.Close();
        }

        [Test]
        public void TestMultipleCloseCallsNoErrors()
        {
            IMessageProducer producer = session.CreateProducer();
            producer.Close();
            producer.Close();
        }

        [Test]
        public void TestGetDisableMessageId()
        {
            IMessageProducer producer = session.CreateProducer();
            Assert.False(producer.DisableMessageID);
        }

        [Test]
        public void TestGetDisableTimeStamp()
        {
            IMessageProducer producer = session.CreateProducer(null);
            Assert.False(producer.DisableMessageTimestamp);
            producer.DisableMessageTimestamp = true;
            Assert.True(producer.DisableMessageTimestamp);
        }

        [Test]
        public void TestPriorityConfiguration()
        {
            IMessageProducer producer = session.CreateProducer(null);
            Assert.AreEqual(MsgPriority.BelowNormal, producer.Priority);
            producer.Priority = MsgPriority.Highest;
            Assert.AreEqual(MsgPriority.Highest, producer.Priority);
        }

        [Test]
        public void TestTimeToLiveConfiguration()
        {
            IMessageProducer producer = session.CreateProducer(null);
            Assert.AreEqual(NMSConstants.defaultTimeToLive, producer.TimeToLive);
            producer.TimeToLive = TimeSpan.FromHours(1);
            Assert.AreEqual(TimeSpan.FromHours(1), producer.TimeToLive);
        }

        [Test]
        public void TestDeliveryModeConfiguration()
        {
            IMessageProducer producer = session.CreateProducer(null);
            Assert.AreEqual(NMSConstants.defaultDeliveryMode, producer.DeliveryMode);
            producer.DeliveryMode = MsgDeliveryMode.NonPersistent;
            Assert.AreEqual(MsgDeliveryMode.NonPersistent, producer.DeliveryMode);
        }

        [Test]
        public void TestAnonymousProducerThrowsMFEWhenNullMessageProvided()
        {
            IDestination dest = new NmsQueue("destination");
            IMessageProducer producer = session.CreateProducer(null);

            Assert.Catch<MessageFormatException>(() => producer.Send(dest, null));
            Assert.Catch<MessageFormatException>(() => producer.Send(dest, null, NMSConstants.defaultDeliveryMode, NMSConstants.defaultPriority, NMSConstants.defaultTimeToLive));
            // Assert.Catch<MessageFormatException>(() => producer.Send(null, NMSConstants.defaultDeliveryMode, NMSConstants.defaultPriority, NMSConstants.defaultTimeToLive));
        }

        [Test]
        public void TestExplicitProducerThrowsMFEWhenNullMessageProvided()
        {
            IDestination dest = new NmsQueue("destination");
            IMessageProducer producer = session.CreateProducer(dest);
            
            Assert.Catch<MessageFormatException>(() => producer.Send(null));
            Assert.Catch<MessageFormatException>(() => producer.Send(null, NMSConstants.defaultDeliveryMode, NMSConstants.defaultPriority, NMSConstants.defaultTimeToLive));
        }

        private NmsConnection CreateConnectionToMockProvider()
        {
            return new NmsConnection(connectionInfo, CreateMockProvider());
        }

        private MockProvider CreateMockProvider()
        {
            return (MockProvider) new MockProviderFactory().CreateProvider(new Uri("mock://localhost"));
        }
    }
}