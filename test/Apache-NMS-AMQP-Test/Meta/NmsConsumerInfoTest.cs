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
    public class NmsConsumerInfoTest
    {
        private NmsConsumerId firstId;
        private NmsConsumerId secondId;

        private NmsSessionId firstSessionId;
        private NmsSessionId secondSessionId;

        [SetUp]
        public void SetUp()
        {
            var generator = new IdGenerator();

            var nmsConnectionId = new NmsConnectionId(generator.GenerateId());

            firstSessionId = new NmsSessionId(nmsConnectionId, 1);
            secondSessionId = new NmsSessionId(nmsConnectionId, 2);

            firstId = new NmsConsumerId(firstSessionId, 1);
            secondId = new NmsConsumerId(secondSessionId, 2);
        }

        [Test]
        public void TestExceptionWhenCreatedWithNullConsumerId()
        {
            Assert.Catch<ArgumentException>(() => new NmsConsumerInfo(null));
        }

        [Test]
        public void TestCreateFromConsumerId()
        {
            var consumerInfo = new NmsConsumerInfo(firstId);
            Assert.AreSame(firstId, consumerInfo.Id);
            Assert.AreSame(firstId.SessionId, consumerInfo.SessionId);
            Assert.IsFalse(string.IsNullOrEmpty(consumerInfo.ToString()));
        }

        [Test]
        public void TestHashCode()
        {
            var first = new NmsConsumerInfo(firstId);
            var second = new NmsConsumerInfo(secondId);

            Assert.AreEqual(first.GetHashCode(), first.GetHashCode());
            Assert.AreEqual(second.GetHashCode(), second.GetHashCode());

            Assert.AreNotEqual(first.GetHashCode(), second.GetHashCode());
        }

        [Test]
        public void TestEqualsCode()
        {
            var first = new NmsConsumerInfo(firstId);
            var second = new NmsConsumerInfo(secondId);

            Assert.AreEqual(first, first);
            Assert.AreEqual(second, second);

            Assert.AreNotEqual(first, second);
        }
    }
}