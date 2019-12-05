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
using System.Threading.Tasks;
using Apache.NMS;
using Apache.NMS.AMQP;
using Apache.NMS.AMQP.Meta;
using Apache.NMS.AMQP.Provider;
using Apache.NMS.AMQP.Provider.Failover;
using Apache.NMS.AMQP.Util;
using Moq;
using NMS.AMQP.Test.Provider.Mock;
using NUnit.Framework;

namespace NMS.AMQP.Test.Provider
{
    /// <summary>
    /// Test behavior of the FailoverProvider
    /// </summary>
    [TestFixture]
    public class FailoverProviderTest
    {
        private List<Uri> uris;
        private NmsConnectionInfo connectionInfo;
        private FailoverProvider provider;
        private MockRemotePeer mockPeer;

        [SetUp]
        public void SetUp()
        {
            uris = new List<Uri>
            {
                new Uri("mock://192.168.2.1:5672"),
                new Uri("mock://192.168.2.2:5672"),
                new Uri("mock://192.168.2.3:5672"),
                new Uri("mock://192.168.2.4:5672"),
            };
            connectionInfo = CreateConnectionInfo();
            mockPeer = new MockRemotePeer();
            mockPeer.Start();
            ProviderFactory.RegisterProviderFactory("mock", new MockProviderFactory());
        }

        [TearDown]
        public void TearDown()
        {
            if (provider != null)
            {
                provider.Close();
                provider = null;
            }

            if (mockPeer != null)
            {
                mockPeer.Terminate();
                mockPeer = null;
            }
        }

        [Test]
        public void TestCreateProviderOnlyUris()
        {
            provider = new FailoverProvider(uris);
            Assert.IsNull(provider.RemoteUri);
        }

        [Test, Timeout(3000)]
        public async Task TestGetRemoteUri()
        {
            provider = new FailoverProvider(uris);
            provider.SetProviderListener(new Mock<IProviderListener>().Object);
            Assert.IsNull(provider.RemoteUri);
            await provider.Connect(connectionInfo);
            Assert.IsNotNull(provider.RemoteUri);
        }

        [Test, Timeout(3000)]
        public async Task TestToString()
        {
            provider = new FailoverProvider(uris);
            provider.SetProviderListener(new Mock<IProviderListener>().Object);
            Assert.IsNotNull(provider.ToString());
            await provider.Connect(connectionInfo);
            Assert.True(provider.ToString().Contains("mock://"));
        }

        [Test, Timeout(3000)]
        public async Task TestConnectToMock()
        {
            provider = new FailoverProvider(uris);
            provider.SetProviderListener(new Mock<IProviderListener>().Object);
            await provider.Connect(connectionInfo);
            provider.Close();
            Assert.That(mockPeer.ContextStats.ProvidersCreated, Is.EqualTo(1));
            Assert.That(mockPeer.ContextStats.ConnectionAttempts, Is.EqualTo(1));
        }

        [Test, Timeout(3000)]
        public void TestCannotStartWithoutListener()
        {
            provider = new FailoverProvider(uris);
            Assert.Throws<IllegalStateException>(() => provider.Start());
        }

        [Test, Timeout(3000)]
        public void TestStartupMaxReconnectAttempts()
        {
            NmsConnectionFactory factory = new NmsConnectionFactory(
                "failover:(mock://localhost:5000?mock.failOnConnect=true)" +
                "?failover.startupMaxReconnectAttempts=5" +
                "&failover.useReconnectBackOff=false");
            IConnection connection = null;
            try
            {
                connection = factory.CreateConnection();
                connection.Start();
                Assert.Fail("Should have stopped after five retries.");
            }
            catch (NMSException)
            {
            }
            finally
            {
                connection?.Close();
            }

            Assert.AreEqual(5, mockPeer.ContextStats.ProvidersCreated);
            Assert.AreEqual(5, mockPeer.ContextStats.ConnectionAttempts);
            Assert.AreEqual(5, mockPeer.ContextStats.CloseAttempts);
        }

        [Test, Timeout(3000)]
        public void TestMaxReconnectAttemptsWithMultipleUris()
        {
            NmsConnectionFactory factory = new NmsConnectionFactory(
                "failover:(mock://192.168.2.1?mock.failOnConnect=true," +
                "mock://192.168.2.2?mock.failOnConnect=true," +
                "mock://192.168.2.3?mock.failOnConnect=true)" +
                "?failover.maxReconnectAttempts=5" +
                "&failover.reconnectDelay=1" +
                "&failover.useReconnectBackOff=false");

            IConnection connection = null;
            try
            {
                connection = factory.CreateConnection();
                connection.Start();
                Assert.Fail("Should have stopped after five retries.");
            }
            catch (NMSException)
            {
            }
            finally
            {
                connection?.Close();
            }

            Assert.AreEqual(15, mockPeer.ContextStats.ProvidersCreated);
            Assert.AreEqual(15, mockPeer.ContextStats.ConnectionAttempts);
            Assert.AreEqual(15, mockPeer.ContextStats.CloseAttempts);
        }

        [Test, Timeout(3000)]
        public void TestMaxReconnectAttemptsWithBackOff()
        {
            NmsConnectionFactory factory = new NmsConnectionFactory(
                "failover:(mock://localhost?mock.failOnConnect=true)" +
                "?failover.maxReconnectAttempts=5" +
                "&failover.maxReconnectDelay=60" +
                "&failover.reconnectDelay=10" +
                "&failover.useReconnectBackOff=true");

            IConnection connection = null;
            try
            {
                connection = factory.CreateConnection();
                connection.Start();
                Assert.Fail("Should have stopped after five retries.");
            }
            catch (NMSException)
            {
            }
            finally
            {
                connection?.Close();
            }

            Assert.AreEqual(5, mockPeer.ContextStats.ProvidersCreated);
            Assert.AreEqual(5, mockPeer.ContextStats.ConnectionAttempts);
            Assert.AreEqual(5, mockPeer.ContextStats.CloseAttempts);
        }

        [Test]
        public void TestFailureOnCloseIsSwallowed()
        {
            NmsConnectionFactory factory = new NmsConnectionFactory(
                "failover:(mock://localhost?mock.failOnClose=true)");

            IConnection connection = factory.CreateConnection();
            connection.Start();
            connection.Close();

            Assert.AreEqual(1, mockPeer.ContextStats.ProvidersCreated);
            Assert.AreEqual(1, mockPeer.ContextStats.ConnectionAttempts);
            Assert.AreEqual(1, mockPeer.ContextStats.CloseAttempts);
        }

        [Test, Timeout(3000)]
        public void TestSessionLifeCyclePassthrough()
        {
            NmsConnectionFactory factory = new NmsConnectionFactory(
            "failover:(mock://localhost)");

            IConnection connection = factory.CreateConnection();
            connection.Start();
            connection.CreateSession(AcknowledgementMode.AutoAcknowledge).Close();
            connection.Close();

            Assert.AreEqual(1, mockPeer.ContextStats.GetCreateResourceCalls<NmsSessionInfo>());
            Assert.AreEqual(1, mockPeer.ContextStats.GetDestroyResourceCalls<NmsSessionInfo>());
        }

        [Test, Timeout(3000)]
        public void TestConsumerLifeCyclePassthrough()
        {
            NmsConnectionFactory factory = new NmsConnectionFactory(
                "failover:(mock://localhost)");

            IConnection connection = factory.CreateConnection();
            connection.Start();
            ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
            var topic = session.GetTopic("test");
            session.CreateConsumer(topic).Close();
            connection.Close();

            Assert.AreEqual(1, mockPeer.ContextStats.GetCreateResourceCalls<NmsConsumerInfo>());
            Assert.AreEqual(1, mockPeer.ContextStats.GetStartResourceCalls<NmsConsumerInfo>());
            Assert.AreEqual(1, mockPeer.ContextStats.GetDestroyResourceCalls<NmsConsumerInfo>());
        }

        [Test, Timeout(300000)]
        public void TestProducerLifeCyclePassthrough()
        {
            NmsConnectionFactory factory = new NmsConnectionFactory(
                "failover:(mock://localhost)");

            IConnection connection = factory.CreateConnection();
            connection.Start();
            ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
            var topic = session.GetTopic("test");
            session.CreateProducer(topic).Close();
            connection.Close();

            Assert.AreEqual(1, mockPeer.ContextStats.GetCreateResourceCalls<NmsProducerInfo>());
            Assert.AreEqual(1, mockPeer.ContextStats.GetDestroyResourceCalls<NmsProducerInfo>());
        }

        [Test, Timeout(300000)]
        public void TestSessionRecoverPassthrough()
        {
            NmsConnectionFactory factory = new NmsConnectionFactory(
                "failover:(mock://localhost)");

            IConnection connection = factory.CreateConnection();
            connection.Start();
            ISession session = connection.CreateSession(AcknowledgementMode.ClientAcknowledge);
            session.Recover();
            connection.Close();

            Assert.AreEqual(1, mockPeer.ContextStats.RecoverCalls);
        }

        private NmsConnectionInfo CreateConnectionInfo()
        {
            return new NmsConnectionInfo(new NmsConnectionId("test"));
        }
    }
}