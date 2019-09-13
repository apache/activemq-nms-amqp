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
using Apache.NMS.AMQP.Message;
using Apache.NMS.AMQP.Message.Facade;
using Apache.NMS.AMQP.Provider.Amqp;
using Apache.NMS.AMQP.Provider.Amqp.Message;
using Apache.NMS.AMQP.Util;
using NUnit.Framework;
using Moq;

namespace NMS.AMQP.Test.Provider.Amqp
{
    [TestFixture]
    public class AmqpMessageFactoryTest
    {
        [Test]
        public void TestCreateMessage()
        {
            AmqpMessageFactory factory = new AmqpMessageFactory(CreateMockAmqpConnection());
            NmsMessage message = factory.CreateMessage();
            INmsMessageFacade facade = message.Facade;

            Assert.IsInstanceOf<NmsMessage>(message);
            Assert.IsInstanceOf<AmqpNmsMessageFacade>(facade);
            Assert.AreEqual(MessageSupport.JMS_TYPE_MSG, facade.JmsMsgType);
        }

        [Test]
        public void TestCreateTextMessage()
        {
            AmqpMessageFactory factory = new AmqpMessageFactory(CreateMockAmqpConnection());
            NmsTextMessage message = factory.CreateTextMessage();
            INmsMessageFacade facade = message.Facade;

            Assert.IsInstanceOf<NmsTextMessage>(message);
            Assert.IsInstanceOf<AmqpNmsTextMessageFacade>(facade);
            Assert.AreEqual(MessageSupport.JMS_TYPE_TXT, facade.JmsMsgType);

            Assert.Null(((AmqpNmsTextMessageFacade) facade).Text);
        }

        [Test]
        public void TestCreateTextMessageString()
        {
            AmqpMessageFactory factory = new AmqpMessageFactory(CreateMockAmqpConnection());
            NmsTextMessage message = factory.CreateTextMessage("SomeValue");
            INmsMessageFacade facade = message.Facade;

            Assert.IsInstanceOf<NmsTextMessage>(message);
            Assert.IsInstanceOf<AmqpNmsTextMessageFacade>(facade);
            Assert.AreEqual(MessageSupport.JMS_TYPE_TXT, facade.JmsMsgType);

            Assert.AreEqual("SomeValue", ((AmqpNmsTextMessageFacade) facade).Text);
        }

        [Test]
        public void TestCreateBytesMessage()
        {
            AmqpMessageFactory factory = new AmqpMessageFactory(CreateMockAmqpConnection());
            NmsBytesMessage message = factory.CreateBytesMessage();
            INmsMessageFacade facade = message.Facade;

            Assert.IsInstanceOf<NmsBytesMessage>(message);
            Assert.IsInstanceOf<AmqpNmsBytesMessageFacade>(facade);
            Assert.AreEqual(MessageSupport.JMS_TYPE_BYTE, facade.JmsMsgType);

            Assert.AreEqual(0, ((AmqpNmsBytesMessageFacade) facade).BodyLength);
        }

        [Test]
        public void TestCreateMapMessage()
        {
            AmqpMessageFactory factory = new AmqpMessageFactory(CreateMockAmqpConnection());
            NmsMapMessage message = factory.CreateMapMessage();
            INmsMessageFacade facade = message.Facade;

            Assert.IsInstanceOf<NmsMapMessage>(message);
            Assert.IsInstanceOf<AmqpNmsMapMessageFacade>(facade);
            Assert.AreEqual(MessageSupport.JMS_TYPE_MAP, facade.JmsMsgType);

            Assert.AreEqual(0, ((AmqpNmsMapMessageFacade) facade).Map.Keys.Count);
        }

        [Test]
        public void TestCreateStreamMessage()
        {
            AmqpMessageFactory factory = new AmqpMessageFactory(CreateMockAmqpConnection());
            NmsStreamMessage message = factory.CreateStreamMessage();
            INmsMessageFacade facade = message.Facade;

            Assert.IsInstanceOf<NmsStreamMessage>(message);
            Assert.IsInstanceOf<AmqpNmsStreamMessageFacade>(facade);
            Assert.AreEqual(MessageSupport.JMS_TYPE_STRM, facade.JmsMsgType);

            Assert.False(((AmqpNmsStreamMessageFacade) facade).HasBody());
        }

        [Test]
        public void TestCreateObjectMessage()
        {
            AmqpMessageFactory factory = new AmqpMessageFactory(CreateMockAmqpConnection());
            NmsObjectMessage message = factory.CreateObjectMessage();
            INmsMessageFacade facade = message.Facade;

            Assert.IsInstanceOf<NmsObjectMessage>(message);
            Assert.IsInstanceOf<AmqpNmsObjectMessageFacade>(facade);
            Assert.IsNull(facade.JmsMsgType);

            Assert.IsNull(((AmqpNmsObjectMessageFacade) facade).Body);
        }



        [Test]
        public void TestCreateObjectMessageSerializable()
        {
            AmqpMessageFactory factory = new AmqpMessageFactory(CreateMockAmqpConnection());
            NmsObjectMessage message = factory.CreateObjectMessage(new SerializableClass());
            INmsMessageFacade facade = message.Facade;

            Assert.IsInstanceOf<NmsObjectMessage>(message);
            Assert.IsInstanceOf<AmqpNmsObjectMessageFacade>(facade);
            Assert.IsNull(facade.JmsMsgType);

            AmqpNmsObjectMessageFacade objectMessageFacade = (AmqpNmsObjectMessageFacade) facade;

            Assert.IsNotNull(objectMessageFacade.Body);
            Assert.IsInstanceOf<SerializableClass>(objectMessageFacade.Body);
        }

        [Test]
        public void TestCreateObjectMessageWithBadTypeThrowsNMSException()
        {
            AmqpMessageFactory factory = new AmqpMessageFactory(CreateMockAmqpConnection());

            Assert.Catch<MessageFormatException>(() => factory.CreateObjectMessage(new NotSerializable()));
        }

        private IAmqpConnection CreateMockAmqpConnection(bool amqpTyped = false)
        {
            Mock<IAmqpConnection> mockConnection = new Mock<IAmqpConnection>();
            mockConnection.Setup(connection => connection.QueuePrefix).Returns("");
            mockConnection.Setup(connection => connection.TopicPrefix).Returns("");
            mockConnection.Setup(connection => connection.ObjectMessageUsesAmqpTypes).Returns(amqpTyped);
            return mockConnection.Object;
        }
        
        [Serializable]
        private class SerializableClass
        {
        }
        
        private class NotSerializable
        {
            
        }
    }
}