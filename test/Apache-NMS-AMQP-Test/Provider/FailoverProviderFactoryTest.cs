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
using Apache.NMS.AMQP.Provider;
using Apache.NMS.AMQP.Provider.Failover;
using NMS.AMQP.Test.Provider.Mock;
using NUnit.Framework;

namespace NMS.AMQP.Test.Provider
{
    [TestFixture]
    public class FailoverProviderFactoryTest
    {
        private readonly Uri baseUri = new Uri("failover:(amqp://localhost:5672,amqp://localhost:567)");

        [Test]
        public void TestCreateFailoverProvider()
        {
            IProvider provider = ProviderFactory.Create(baseUri);
            Assert.That(provider, Is.InstanceOf<FailoverProvider>());
        }

        [Test]
        public void TestCreateMockProvider()
        {
            ProviderFactory.RegisterProviderFactory("mock", new MockProviderFactory());
            IProvider provider = ProviderFactory.Create(new Uri("mock://localhost:5000"));
            Assert.That(provider, Is.InstanceOf<MockProvider>());
        }

        [Test]
        public void TestCreateProviderInitializesToDefaults()
        {
            FailoverProvider failoverProvider = ProviderFactory.Create(baseUri) as FailoverProvider;
            Assert.IsNotNull(failoverProvider);
            Assert.AreEqual(FailoverProvider.DEFAULT_INITIAL_RECONNECT_DELAY, failoverProvider.InitialReconnectDelay);
            Assert.AreEqual(FailoverProvider.DEFAULT_RECONNECT_DELAY, failoverProvider.ReconnectDelay);
            Assert.AreEqual(FailoverProvider.DEFAULT_MAX_RECONNECT_DELAY, failoverProvider.MaxReconnectDelay);
            Assert.AreEqual(FailoverProvider.DEFAULT_STARTUP_MAX_RECONNECT_ATTEMPTS, failoverProvider.StartupMaxReconnectAttempts);
            Assert.AreEqual(FailoverProvider.DEFAULT_MAX_RECONNECT_ATTEMPTS, failoverProvider.MaxReconnectAttempts);
            Assert.AreEqual(FailoverProvider.DEFAULT_USE_RECONNECT_BACKOFF, failoverProvider.UseReconnectBackOff);
            Assert.AreEqual(FailoverProvider.DEFAULT_RECONNECT_BACKOFF_MULTIPLIER, failoverProvider.ReconnectBackOffMultiplier);
            Assert.AreEqual(FailoverProvider.DEFAULT_WARN_AFTER_RECONNECT_ATTEMPTS, failoverProvider.WarnAfterReconnectAttempts);
        }

        [Test]
        public void TestCreateWithOptions()
        {
            Uri configured = new Uri(baseUri +
                                    "?failover.initialReconnectDelay=" + (FailoverProvider.DEFAULT_INITIAL_RECONNECT_DELAY + 1) +
                                    "&failover.reconnectDelay=" + (FailoverProvider.DEFAULT_RECONNECT_DELAY + 2) +
                                    "&failover.maxReconnectDelay=" + (FailoverProvider.DEFAULT_MAX_RECONNECT_DELAY + 3) +
                                    "&failover.startupMaxReconnectAttempts=" + (FailoverProvider.DEFAULT_STARTUP_MAX_RECONNECT_ATTEMPTS + 4) +
                                    "&failover.maxReconnectAttempts=" + (FailoverProvider.DEFAULT_MAX_RECONNECT_ATTEMPTS + 5) +
                                    "&failover.warnAfterReconnectAttempts=" + (FailoverProvider.DEFAULT_WARN_AFTER_RECONNECT_ATTEMPTS + 6) +
                                    "&failover.useReconnectBackOff=" + (!FailoverProvider.DEFAULT_USE_RECONNECT_BACKOFF) +
                                    "&failover.reconnectBackOffMultiplier=" + (FailoverProvider.DEFAULT_RECONNECT_BACKOFF_MULTIPLIER + 1.0d));

            FailoverProvider failover = ProviderFactory.Create(configured) as FailoverProvider;
            Assert.IsNotNull(failover);

            Assert.AreEqual(FailoverProvider.DEFAULT_INITIAL_RECONNECT_DELAY + 1, failover.InitialReconnectDelay);
            Assert.AreEqual(FailoverProvider.DEFAULT_RECONNECT_DELAY + 2, failover.ReconnectDelay);
            Assert.AreEqual(FailoverProvider.DEFAULT_MAX_RECONNECT_DELAY + 3, failover.MaxReconnectDelay);
            Assert.AreEqual(FailoverProvider.DEFAULT_STARTUP_MAX_RECONNECT_ATTEMPTS + 4, failover.StartupMaxReconnectAttempts);
            Assert.AreEqual(FailoverProvider.DEFAULT_MAX_RECONNECT_ATTEMPTS + 5, failover.MaxReconnectAttempts);
            Assert.AreEqual(FailoverProvider.DEFAULT_WARN_AFTER_RECONNECT_ATTEMPTS + 6, failover.WarnAfterReconnectAttempts);
            Assert.AreEqual(!FailoverProvider.DEFAULT_USE_RECONNECT_BACKOFF, failover.UseReconnectBackOff);
            Assert.AreEqual(FailoverProvider.DEFAULT_RECONNECT_BACKOFF_MULTIPLIER + 1.0d, failover.ReconnectBackOffMultiplier, 0.0);
        }

    }
}