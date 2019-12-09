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
    public class NmsTransactionInfoTest
    {
        private NmsConnectionId firstId;
        private NmsConnectionId secondId;
        private NmsSessionId firstSessionId;
        private NmsSessionId secondSessionId;
        private NmsTransactionId firstTxId;
        private NmsTransactionId secondTxId;

        [SetUp]
        public void SetUp()
        {
            var generator = new IdGenerator();

            firstId = new NmsConnectionId(generator.GenerateId());
            secondId = new NmsConnectionId(generator.GenerateId());

            firstSessionId = new NmsSessionId(firstId, 1);
            secondSessionId = new NmsSessionId(secondId, 2);

            firstTxId = new NmsTransactionId(firstId, 1);
            secondTxId = new NmsTransactionId(secondId, 2);
        }

        [Test]
        public void TestThrowsWhenSessionIdIsNull()
        {
            Assert.Catch<ArgumentException>(() => new NmsTransactionInfo(null, firstTxId));
        }
        
        [Test]
        public void TestThrowsWhenTransactionIdIsNull()
        {
            Assert.Catch<ArgumentException>(() => new NmsTransactionInfo(firstSessionId, null));
        }

        [Test]
        public void TestCreateTransactionInfo()
        {
            var info = new NmsTransactionInfo(firstSessionId, firstTxId);
            Assert.AreSame(firstSessionId, info.SessionId);
            Assert.AreSame(firstTxId, info.Id);
            Assert.IsFalse(string.IsNullOrEmpty(info.ToString()));
        }
        
        [Test]
        public void TestHashCode()
        {
            var first = new NmsTransactionInfo(firstSessionId, firstTxId);
            var second = new NmsTransactionInfo(secondSessionId, secondTxId);

            Assert.AreEqual(first.GetHashCode(), first.GetHashCode());
            Assert.AreEqual(second.GetHashCode(), second.GetHashCode());

            Assert.AreNotEqual(first.GetHashCode(), second.GetHashCode());
        }

        [Test]
        public void TestEqualsCode()
        {
            var first = new NmsTransactionInfo(firstSessionId, firstTxId);
            var second = new NmsTransactionInfo(secondSessionId, secondTxId);

            Assert.AreEqual(first, first);
            Assert.AreEqual(second, second);

            Assert.AreNotEqual(first, second);
        }
    }
}