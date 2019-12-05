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
using Apache.NMS.AMQP.Meta;
using Apache.NMS.AMQP.Util;
using NUnit.Framework;

namespace NMS.AMQP.Test.Meta
{
    [TestFixture]
    public class NmsConnectionInfoTest
    {
        private NmsConnectionId firstId;
        private NmsConnectionId secondId;

        [SetUp]
        public void SetUp()
        {
            var generator = new IdGenerator();
            firstId = new NmsConnectionId(generator.GenerateId());
            secondId = new NmsConnectionId(generator.GenerateId());
        }

        [Test]
        public void TestExceptionWhenCreatedWithNullConnectionId()
        {
            Assert.Catch<ArgumentNullException>(() => new NmsConnectionInfo(null));
        }

        [Test]
        public void TestCreate()
        {
            var info = new NmsConnectionInfo(firstId);
            Assert.AreSame(firstId, info.Id);
            Assert.NotNull(info.ToString());
        }

        [Test]
        public void TestHashCode()
        {
            var first = new NmsConnectionInfo(firstId);
            var second = new NmsConnectionInfo(secondId);
            
            Assert.AreEqual(first.GetHashCode(), first.GetHashCode());
            Assert.AreEqual(second.GetHashCode(), second.GetHashCode());
            Assert.AreNotEqual(first.GetHashCode(), second.GetHashCode());
        }

        [Test]
        public void TestEquals()
        {
            var first = new NmsConnectionInfo(firstId);
            var second = new NmsConnectionInfo(secondId);
            
            Assert.AreEqual(first, first);
            Assert.AreEqual(second, second);
            
            Assert.IsFalse(first.Equals(second));
            Assert.IsFalse(second.Equals(first));
        }

        [Test]
        public void TestIsExplicitClientId()
        {
            var info = new NmsConnectionInfo(firstId);
            Assert.IsFalse(info.IsExplicitClientId);
            info.SetClientId("something", true);
            Assert.IsTrue(info.IsExplicitClientId);
        }
    }
}