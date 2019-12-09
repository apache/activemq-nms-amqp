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
    public class NmsConnectionTest
    {
        private NmsConnection connection;
        private NmsConnectionInfo connectionInfo;
        private MockProvider provider;

        [SetUp]
        public void SetUp()
        {
            provider = (MockProvider)new MockProviderFactory().CreateProvider(new Uri("mock://localhost"));
            connectionInfo = new NmsConnectionInfo(new NmsConnectionId("ID:TEST:1"));
        }

        [TearDown]
        public void TearDown()
        {
            connection?.Close();
        }

        [Test]
        public void TestConnectionThrowsNMSExceptionProviderStartFails()
        {
            provider.Configuration.FailOnStart = true;
            Assert.Throws<NMSException>(() => connection = new NmsConnection(connectionInfo, provider));
        }

        [Test]
        public void TestStateAfterCreate()
        {
            connection = new NmsConnection(connectionInfo, provider);

            Assert.False(connection.IsStarted);
            Assert.False(connection.IsClosed);
            Assert.False(connection.IsConnected);
        }

        [Test]
        public void TestGetConnectionId()
        {
            connection = new NmsConnection(connectionInfo, provider);
            Assert.That(connection.Id.ToString(), Is.EqualTo("ID:TEST:1"));
        }

        [Test]
        public void TestConnectionStart()
        {
            connection = new NmsConnection(connectionInfo, provider);
            Assert.False(connection.IsConnected);
            connection.Start();
            Assert.True(connection.IsConnected);
        }

        [Test]
        public void TestConnectionMulitpleStartCalls()
        {
            connection = new NmsConnection(connectionInfo, provider);
            Assert.False(connection.IsConnected);
            connection.Start();
            Assert.True(connection.IsConnected);
            connection.Start();
            Assert.True(connection.IsConnected);
        }

        [Test]
        public void TestConnectionStartAndStop()
        {
            connection = new NmsConnection(connectionInfo, provider);
            Assert.False(connection.IsConnected);
            connection.Start();
            Assert.True(connection.IsConnected);
            connection.Stop();
            Assert.True(connection.IsConnected);
        }

        [Test]
        public void TestSetClientIdFromEmptyString()
        {
            connection = new NmsConnection(connectionInfo, provider);
            Assert.False(connection.IsConnected);
            Assert.Throws<InvalidClientIDException>(() => connection.ClientId = "");
        }

        [Test]
        public void TestSetClientIdFromNull()
        {
            connection = new NmsConnection(connectionInfo, provider);
            Assert.False(connection.IsConnected);
            Assert.Throws<InvalidClientIDException>(() => connection.ClientId = null);
        }

        [Test]
        public void TestSetClientIdFailsOnSecondCall()
        {
            connection = new NmsConnection(connectionInfo, provider);
            Assert.False(connection.IsConnected);
            connection.ClientId = "TEST-ID";
            Assert.True(connection.IsConnected);
            Assert.Throws<IllegalStateException>(() => connection.ClientId = "TEST-ID");
        }

        [Test]
        public void TestSetClientIdFailsAfterStart()
        {
            connection = new NmsConnection(connectionInfo, provider);
            Assert.False(connection.IsConnected);
            connection.Start();
            Assert.True(connection.IsConnected);
            Assert.Throws<IllegalStateException>(() => connection.ClientId = "TEST-ID");
        }

        [Test]
        public void TestCreateSessionDefaultMode()
        {
            connection = new NmsConnection(connectionInfo, provider);
            connection.Start();
            ISession session = connection.CreateSession();
            Assert.AreEqual(AcknowledgementMode.AutoAcknowledge, session.AcknowledgementMode);
        }
    }
}