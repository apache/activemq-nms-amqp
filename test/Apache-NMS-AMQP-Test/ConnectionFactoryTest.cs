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
    [TestFixture]
    public class ConnectionFactoryTest
    {
        private static readonly string USER = "USER";
        private static readonly string PASSWORD = "PASSWORD";

        [Test]
        public void TestConnectionFactoryCreate()
        {
            var factory = new NmsConnectionFactory();
            Assert.Null(factory.UserName);
            Assert.Null(factory.Password);
            Assert.NotNull(factory.BrokerUri);
        }

        [Test]
        public void TestConnectionFactoryCreateUsernameAndPassword()
        {
            NmsConnectionFactory factory = new NmsConnectionFactory(USER, PASSWORD);

            Assert.NotNull(factory.UserName);
            Assert.That(factory.UserName, Is.EqualTo(USER));
            Assert.NotNull(factory.Password);
            Assert.That(factory.Password, Is.EqualTo(PASSWORD));
        }

        [Test]
        public void TestConnectionFactoryCreateUsernamePasswordAndBrokerUri()
        {
            NmsConnectionFactory factory = new NmsConnectionFactory(USER, PASSWORD, "mock://localhost:5000");

            Assert.NotNull(factory.UserName);
            Assert.That(factory.UserName, Is.EqualTo(USER));
            Assert.NotNull(factory.Password);
            Assert.That(factory.Password, Is.EqualTo(PASSWORD));

            var brokerUri = factory.BrokerUri;
            Assert.NotNull(brokerUri);
            Assert.That(brokerUri.Authority, Is.EqualTo("localhost:5000"));
            Assert.That(brokerUri.Scheme, Is.EqualTo("mock"));
            Assert.That(brokerUri.Port, Is.EqualTo(5000));
        }

        [Test]
        public void TestSetPropertiesFromStringUri()
        {
            string baseUri = "amqp://localhost:1234";
            string configuredUri = baseUri +
                                "?nms.username=user" +
                                "&nms.password=password" +
                                "&nms.clientId=client" +
                                "&nms.connectionIdPrefix=ID:TEST" +
                                "&nms.clientIDPrefix=clientId" +
                                "&nms.requestTimeout=1000" +
                                "&nms.sendTimeout=1000" +
                                "&nms.localMessageExpiry=false";

            NmsConnectionFactory factory = new NmsConnectionFactory(configuredUri);

            Assert.AreEqual("user", factory.UserName);
            Assert.AreEqual("password", factory.Password);
            Assert.AreEqual("client", factory.ClientId);
            Assert.AreEqual("ID:TEST", factory.ConnectionIdPrefix);
            Assert.AreEqual("clientId", factory.ClientIdPrefix);
            Assert.AreEqual(1000, factory.RequestTimeout);
            Assert.AreEqual(1000, factory.SendTimeout);
            Assert.IsFalse(factory.LocalMessageExpiry);
        }
        
        [Test]
        public void TestSetPropertiesFromUri()
        {
            string baseUri = "amqp://localhost:1234";
            string configuredUri = baseUri +
                                "?nms.username=user" +
                                "&nms.password=password" +
                                "&nms.clientId=client" +
                                "&nms.connectionIdPrefix=ID:TEST" +
                                "&nms.clientIDPrefix=clientId" +
                                "&nms.requestTimeout=1000" +
                                "&nms.sendTimeout=1000" +
                                "&nms.closeTimeout=2000" +
                                "&nms.localMessageExpiry=false";

            NmsConnectionFactory factory = new NmsConnectionFactory(new Uri(configuredUri));

            Assert.AreEqual("user", factory.UserName);
            Assert.AreEqual("password", factory.Password);
            Assert.AreEqual("client", factory.ClientId);
            Assert.AreEqual("ID:TEST", factory.ConnectionIdPrefix);
            Assert.AreEqual("clientId", factory.ClientIdPrefix);
            Assert.AreEqual(1000, factory.RequestTimeout);
            Assert.AreEqual(1000, factory.SendTimeout);
            Assert.AreEqual(2000, factory.CloseTimeout);
            Assert.IsFalse(factory.LocalMessageExpiry);
        }

        [Test]
        public void TestCreateConnectionBadBrokerUri()
        {
            NmsConnectionFactory factory = new NmsConnectionFactory
            {
                BrokerUri = new Uri("bad://127.0.0.1:5763")
            };

            Assert.Throws<NMSException>(() => factory.CreateConnection());
        }

        [Test]
        public void TestCreateConnectionBadProviderString()
        {
            NmsConnectionFactory factory = new NmsConnectionFactory("bad://127.0.0.1:5763");
            Assert.Throws<NMSException>(() => factory.CreateConnection());
        }
    }
}