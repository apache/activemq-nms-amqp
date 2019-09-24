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
using Amqp;
using Apache.NMS.AMQP.Provider;
using Apache.NMS.AMQP.Provider.Amqp;
using NUnit.Framework;

namespace NMS.AMQP.Test.Provider.Amqp
{
    [TestFixture]
    public class AmqpProviderFactoryTest
    {
        private uint customMaxHandle = 2048;

        [Test]
        public void TestCreateAmqpProvider()
        {
            IProvider provider = ProviderFactory.Create(new Uri("amqp://localhost:5672"));
            Assert.That(provider, Is.InstanceOf<AmqpProvider>());
        }

        [Test]
        public void TestCreateWithDefaultOptions()
        {
            AmqpProvider provider = ProviderFactory.Create(new Uri("amqp://localhost:5672")) as AmqpProvider;

            Assert.IsNotNull(provider);
            Assert.AreEqual(AmqpProvider.DEFAULT_MAX_HANDLE, provider.MaxHandle);
            Assert.IsFalse(provider.TraceFrames);
        }

        [Test]
        public void TestCreateWithCustomOptions()
        {
            Uri uri = new Uri("amqp://localhost:5672" +
                              "?amqp.maxHandle=" + customMaxHandle +
                              "&amqp.traceFrames=true");

            AmqpProvider provider = ProviderFactory.Create(uri) as AmqpProvider;

            Assert.IsNotNull(provider);
            Assert.AreEqual(customMaxHandle, provider.MaxHandle);
            Assert.IsTrue(provider.TraceFrames);
        }

        [TearDown]
        public void TearDown()
        {
            // clean up trace level as it may interfere with other tests
            Trace.TraceLevel = 0;
        }
    }
}