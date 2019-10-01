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
using Apache.NMS.AMQP.Transport;
using NUnit.Framework;

namespace NMS.AMQP.Test.Transport
{
    [TestFixture]
    public class TransportContextFactoryTest
    {
        private int customReceiveBufferSize = 32 * 1024;
        private bool customTcpNoDelay = false;
        private int customReceiveTimeout = 1000;
        private int customSendBufferSize = 32 * 1024;
        private int customSendTimeout = 2000;
        private int customTcpKeepAliveTime = 2500;
        private int customTcpKeepAliveInterval = 3000;

        [Test]
        public void TestCreateWithDefaultOptions()
        {
            Uri uri = new Uri("amqp://localhost:5672");
            ITransportContext transportContext = TransportContextFactory.CreateTransportContext(uri);

            Assert.NotNull(transportContext);
            Assert.IsFalse(transportContext.IsSecure);

            Assert.AreEqual(8192, transportContext.ReceiveBufferSize);
            Assert.AreEqual(0, transportContext.ReceiveTimeout);
            Assert.AreEqual(8192, transportContext.SendBufferSize);
            Assert.AreEqual(0, transportContext.SendTimeout);
            Assert.IsTrue(transportContext.TcpNoDelay);
        }

        [Test]
        public void TestCreateWithCustomOptions()
        {
            Uri uri = new Uri("amqp://localhost:5672" + "?" +
                              "transport.receiveBufferSize=" + customReceiveBufferSize + "&" +
                              "transport.receiveTimeout=" + customReceiveTimeout + "&" +
                              "transport.sendBufferSize=" + customSendBufferSize + "&" +
                              "transport.sendTimeout=" + customSendTimeout + "&" +
                              "transport.tcpKeepAliveTime=" + customTcpKeepAliveTime + "&" +
                              "transport.tcpKeepAliveInterval=" + customTcpKeepAliveInterval + "&" +
                              "transport.tcpNoDelay=" + customTcpNoDelay);
            ITransportContext transportContext = TransportContextFactory.CreateTransportContext(uri);

            Assert.NotNull(transportContext);
            Assert.IsFalse(transportContext.IsSecure);

            Assert.AreEqual(customReceiveBufferSize, transportContext.ReceiveBufferSize);
            Assert.AreEqual(customReceiveTimeout, transportContext.ReceiveTimeout);
            Assert.AreEqual(customSendBufferSize, transportContext.SendBufferSize);
            Assert.AreEqual(customSendTimeout, transportContext.SendTimeout);
            Assert.AreEqual(customTcpKeepAliveTime, transportContext.TcpKeepAliveTime);
            Assert.AreEqual(customTcpKeepAliveInterval, transportContext.TcpKeepAliveInterval);
            Assert.AreEqual(customTcpNoDelay, transportContext.TcpNoDelay);
        }

        [Test]
        public void TestCreateSecuredWithDefaultOptions()
        {
            Uri uri = new Uri("amqps://localhost:5672");
            ISecureTransportContext transportContext = TransportContextFactory.CreateTransportContext(uri) as ISecureTransportContext;

            Assert.NotNull(transportContext);
            Assert.IsTrue(transportContext.IsSecure);

            Assert.AreEqual(8192, transportContext.ReceiveBufferSize);
            Assert.AreEqual(0, transportContext.ReceiveTimeout);
            Assert.AreEqual(8192, transportContext.SendBufferSize);
            Assert.AreEqual(0, transportContext.SendTimeout);
            Assert.IsTrue(transportContext.TcpNoDelay);
        }
    }
}