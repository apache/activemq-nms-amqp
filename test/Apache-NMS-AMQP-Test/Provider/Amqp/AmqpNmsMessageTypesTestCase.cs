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

using Apache.NMS.AMQP;
using Apache.NMS.AMQP.Provider.Amqp;
using Apache.NMS.AMQP.Provider.Amqp.Message;
using Moq;

namespace NMS.AMQP.Test.Provider.Amqp
{
    public class AmqpNmsMessageTypesTestCase
    {
        protected AmqpNmsMessageFacade CreateNewMessageFacade()
        {
            AmqpNmsMessageFacade facade = new AmqpNmsMessageFacade();
            facade.Initialize(CreateMockAmqpConnection());
            return facade;
        }

        protected AmqpNmsMessageFacade CreateReceivedMessageFacade(global::Amqp.Message message)
        {
            AmqpNmsMessageFacade facade = new AmqpNmsMessageFacade();
            IAmqpConnection mockAmqpConnection = CreateMockAmqpConnection();
            var mockConsumer = CreateMockConsumer(mockAmqpConnection);
            facade.Initialize(mockConsumer, message);
            return facade;
        }

        protected AmqpNmsTextMessageFacade CreateNewTextMessageFacade()
        {
            AmqpNmsTextMessageFacade facade = new AmqpNmsTextMessageFacade();
            facade.Initialize(CreateMockAmqpConnection());
            return facade;
        }

        protected AmqpNmsTextMessageFacade CreateReceivedTextMessageFacade(global::Amqp.Message message)
        {
            AmqpNmsTextMessageFacade facade = new AmqpNmsTextMessageFacade();
            facade.Initialize(CreateMockConsumer(), message);
            return facade;
        }

        protected AmqpNmsMessageFacade CreateReceivedMessageFacade(IAmqpConsumer consumer, global::Amqp.Message message)
        {
            AmqpNmsMessageFacade facade = new AmqpNmsMessageFacade();
            facade.Initialize(consumer, message);
            return facade;
        }

        protected AmqpNmsStreamMessageFacade CreateNewStreamMessageFacade()
        {
            AmqpNmsStreamMessageFacade facade = new AmqpNmsStreamMessageFacade();
            facade.Initialize(CreateMockAmqpConnection());
            return facade;
        }

        protected AmqpNmsStreamMessageFacade CreateReceivedStreamMessageFacade(global::Amqp.Message message)
        {
            AmqpNmsStreamMessageFacade facade = new AmqpNmsStreamMessageFacade();
            facade.Initialize(CreateMockConsumer(CreateMockAmqpConnection()), message);
            return facade;
        }

        protected AmqpNmsBytesMessageFacade CreateNewBytesMessageFacade()
        {
            AmqpNmsBytesMessageFacade facade = new AmqpNmsBytesMessageFacade();
            facade.Initialize(CreateMockAmqpConnection());
            return facade;
        }

        protected AmqpNmsBytesMessageFacade CreateReceivedBytesMessageFacade(global::Amqp.Message message)
        {
            AmqpNmsBytesMessageFacade facade = new AmqpNmsBytesMessageFacade();
            facade.Initialize(CreateMockConsumer(CreateMockAmqpConnection()), message);
            return facade;
        }

        protected AmqpNmsMapMessageFacade CreateNewMapMessageFacade()
        {
            AmqpNmsMapMessageFacade facade = new AmqpNmsMapMessageFacade();
            facade.Initialize(CreateMockAmqpConnection());
            return facade;
        }

        protected AmqpNmsMapMessageFacade CreateReceivedMapMessageFacade(global::Amqp.Message message)
        {
            AmqpNmsMapMessageFacade facade = new AmqpNmsMapMessageFacade();
            facade.Initialize(CreateMockConsumer(CreateMockAmqpConnection()), message);
            return facade;
        }

        protected AmqpNmsObjectMessageFacade CreateNewObjectMessageFacade(bool amqpTyped = false)
        {
            AmqpNmsObjectMessageFacade facade = new AmqpNmsObjectMessageFacade();
            facade.Initialize(CreateMockAmqpConnection(amqpTyped));
            return facade;
        }

        protected AmqpNmsObjectMessageFacade CreateReceivedObjectMessageFacade(global::Amqp.Message message)
        {
            AmqpNmsObjectMessageFacade facade = new AmqpNmsObjectMessageFacade();
            facade.Initialize(CreateMockConsumer(), message);
            return facade;
        }

        protected IAmqpConsumer CreateMockConsumer(IAmqpConnection connection)
        {
            Mock<IAmqpConsumer> mockConsumer = new Mock<IAmqpConsumer>();
            mockConsumer.Setup(consumer => consumer.Connection).Returns(connection);
            mockConsumer.Setup(consumer => consumer.Destination).Returns(new NmsTopic("TestTopic"));
            return mockConsumer.Object;
        }

        protected IAmqpConsumer CreateMockConsumer()
        {
            Mock<IAmqpConsumer> mockConsumer = new Mock<IAmqpConsumer>();
            mockConsumer.Setup(consumer => consumer.Connection).Returns(() => CreateMockAmqpConnection());
            mockConsumer.Setup(consumer => consumer.Destination).Returns(new NmsTopic("TestTopic"));
            return mockConsumer.Object;
        }

        protected IAmqpConnection CreateMockAmqpConnection(bool amqpTyped = false)
        {
            Mock<IAmqpConnection> mockConnection = new Mock<IAmqpConnection>();
            mockConnection.Setup(connection => connection.QueuePrefix).Returns("");
            mockConnection.Setup(connection => connection.TopicPrefix).Returns("");
            mockConnection.Setup(connection => connection.ObjectMessageUsesAmqpTypes).Returns(amqpTyped);
            return mockConnection.Object;
        }
    }
}