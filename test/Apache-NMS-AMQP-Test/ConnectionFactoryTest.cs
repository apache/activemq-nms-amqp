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
using System.Diagnostics;
using System.Threading.Tasks;
using Apache.NMS;
using Apache.NMS.AMQP;
using Apache.NMS.AMQP.Policies;
using Apache.NMS.AMQP.Provider;
using NMS.AMQP.Test.Provider.Mock;
using NUnit.Framework;

namespace NMS.AMQP.Test
{
    [TestFixture]
    public class ConnectionFactoryTest
    {
        private static readonly string USER = "USER";
        private static readonly string PASSWORD = "PASSWORD";

        private MockRemotePeer mockPeer;

        [SetUp]
        public void SetUp()
        {
            mockPeer = new MockRemotePeer();
            mockPeer.Start();
            ProviderFactory.RegisterProviderFactory("mock", new MockProviderFactory());
        }
        
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
                                "&nms.localMessageExpiry=false" +
                                "&nms.maxNewConnectionRatePerSec=4";

            NmsConnectionFactory factory = new NmsConnectionFactory(configuredUri);

            Assert.AreEqual("user", factory.UserName);
            Assert.AreEqual("password", factory.Password);
            Assert.AreEqual("client", factory.ClientId);
            Assert.AreEqual("ID:TEST", factory.ConnectionIdPrefix);
            Assert.AreEqual("clientId", factory.ClientIdPrefix);
            Assert.AreEqual(1000, factory.RequestTimeout);
            Assert.AreEqual(1000, factory.SendTimeout);
            Assert.AreEqual(4, factory.MaxNewConnectionRatePerSec);
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
                                "&nms.localMessageExpiry=false" +
                                "&nms.prefetchPolicy.all=55";

            NmsConnectionFactory factory = new NmsConnectionFactory(new Uri(configuredUri));

            Assert.AreEqual("user", factory.UserName);
            Assert.AreEqual("password", factory.Password);
            Assert.AreEqual("client", factory.ClientId);
            Assert.AreEqual("ID:TEST", factory.ConnectionIdPrefix);
            Assert.AreEqual("clientId", factory.ClientIdPrefix);
            Assert.AreEqual(1000, factory.RequestTimeout);
            Assert.AreEqual(1000, factory.SendTimeout);
            Assert.AreEqual(2000, factory.CloseTimeout);
            Assert.AreEqual(55, factory.PrefetchPolicy.QueuePrefetch);
            Assert.AreEqual(55, factory.PrefetchPolicy.TopicPrefetch);
            Assert.AreEqual(55, factory.PrefetchPolicy.DurableTopicPrefetch);
            Assert.AreEqual(55, factory.PrefetchPolicy.QueueBrowserPrefetch);
            Assert.IsFalse(factory.LocalMessageExpiry);
        }
        
        [Test]
        public void TestSetPrefetchPolicyPropertiesFromUri()
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
                                   "&nms.localMessageExpiry=false" +
                                   "&nms.prefetchPolicy.queuePrefetch=11" +
                                   "&nms.prefetchPolicy.topicPrefetch=22" +
                                   "&nms.prefetchPolicy.durableTopicPrefetch=33" +
                                   "&nms.prefetchPolicy.queueBrowserPrefetch=44";

            NmsConnectionFactory factory = new NmsConnectionFactory(new Uri(configuredUri));

            Assert.AreEqual("user", factory.UserName);
            Assert.AreEqual("password", factory.Password);
            Assert.AreEqual("client", factory.ClientId);
            Assert.AreEqual("ID:TEST", factory.ConnectionIdPrefix);
            Assert.AreEqual("clientId", factory.ClientIdPrefix);
            Assert.AreEqual(1000, factory.RequestTimeout);
            Assert.AreEqual(1000, factory.SendTimeout);
            Assert.AreEqual(2000, factory.CloseTimeout);
            Assert.AreEqual(11, factory.PrefetchPolicy.QueuePrefetch);
            Assert.AreEqual(22, factory.PrefetchPolicy.TopicPrefetch);
            Assert.AreEqual(33, factory.PrefetchPolicy.DurableTopicPrefetch);
            Assert.AreEqual(44, factory.PrefetchPolicy.QueueBrowserPrefetch);
            Assert.IsFalse(factory.LocalMessageExpiry);
        }

        [Test]
        public void TestSetDeserializationPolicy()
        {
            string baseUri = "amqp://localhost:1234";
            string configuredUri = baseUri +
                                   "?nms.deserializationPolicy.allowList=a,b,c" +
                                   "&nms.deserializationPolicy.denyList=c,d,e";

            var factory = new NmsConnectionFactory(new Uri(configuredUri));
            var deserializationPolicy = factory.DeserializationPolicy as NmsDefaultDeserializationPolicy;
            Assert.IsNotNull(deserializationPolicy);
            Assert.AreEqual("a,b,c", deserializationPolicy.AllowList);
            Assert.AreEqual("c,d,e", deserializationPolicy.DenyList);
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
        
        [Test, Timeout(12000)]
        public void TestMaxNewConnectionRatePerSec()
        {
            double desiredRatePerSec = 5;
            
            NmsConnectionFactory factory = new NmsConnectionFactory(
                "failover:(mock://localhost?mock.failOnConnect=true)" +
                "?failover.maxReconnectAttempts=0" +
                "&nms.maxNewConnectionRatePerSec="+desiredRatePerSec);

            int testTimeMs = 5000;

            int mainCounter = 0;

            Parallel.For(0, 4, (i) =>
            {
                Stopwatch st = Stopwatch.StartNew();
                IConnection connection = null;
                int counter = -1;
                do
                {
                    try
                    {
                        counter++;
                        connection = factory.CreateConnection();
                        connection.Start();
                        Assert.Fail("Should have stopped after predefined number of retries.");
                    }
                    catch (NMSException)
                    {
                    }
                    finally
                    {
                        connection?.Close();
                    }
                } while (st.ElapsedMilliseconds < testTimeMs);

                lock (factory)
                {
                    mainCounter += counter;
                }
            });

            double ratePerSec = 1000.0 * mainCounter / testTimeMs;
            
            Assert.AreEqual(desiredRatePerSec, ratePerSec, 1);
        }
    }
}