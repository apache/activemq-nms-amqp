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
    public class NmsTransactionIdTest
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
        public void TestNullIdThrowsException()
        {
            Assert.Catch<ArgumentException>(() => new NmsTransactionId(null, 0));
        }

        [Test]
        public void TestConstructor()
        {
            var id = new NmsTransactionId(firstId, 1);
            Assert.AreEqual(firstId, id.ConnectionId);
            Assert.AreEqual(1, id.Value);
        }

        [Test]
        public void TestToString()
        {
            var id = new NmsTransactionId(firstId, 1);
            var txKey = id.ToString();
            Assert.IsTrue(txKey.StartsWith("TX:"));
        }

        [Test]
        public void TestEquals()
        {
            Assert.AreNotEqual(new NmsTransactionId(firstId, 1), new NmsTransactionId(firstId, 2));
            Assert.AreNotEqual(new NmsTransactionId(firstId, 1), new NmsTransactionId(secondId, 1));
            Assert.AreEqual(new NmsTransactionId(firstId, 1), new NmsTransactionId(firstId, 1));
        }

        [Test]
        public void TestHashCode()
        {
            Assert.AreNotEqual(new NmsTransactionId(firstId, 1).GetHashCode(), new NmsTransactionId(firstId, 2).GetHashCode());
            Assert.AreNotEqual(new NmsTransactionId(firstId, 1).GetHashCode(), new NmsTransactionId(secondId, 1).GetHashCode());
            Assert.AreEqual(new NmsTransactionId(firstId, 1).GetHashCode(), new NmsTransactionId(firstId, 1).GetHashCode());
        }
    }
}