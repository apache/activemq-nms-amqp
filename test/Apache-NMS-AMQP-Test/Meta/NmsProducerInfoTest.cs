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
    public class NmsProducerInfoTest
    {
        private NmsProducerId firstId;
        private NmsProducerId secondId;

        private NmsSessionId firstSessionId;
        private NmsSessionId secondSessionId;

        [SetUp]
        public void SetUp()
        {
            var generator = new IdGenerator();

            var nmsConnectionId = new NmsConnectionId(generator.GenerateId());

            firstSessionId = new NmsSessionId(nmsConnectionId, 1);
            secondSessionId = new NmsSessionId(nmsConnectionId, 2);

            firstId = new NmsProducerId(firstSessionId, 1);
            secondId = new NmsProducerId(secondSessionId, 2);
        }

        [Test]
        public void TestExceptionWhenCreatedWithNullProducerId()
        {
            Assert.Catch<ArgumentException>(() => new NmsProducerInfo(null));
        }

        [Test]
        public void TestCreateFromProducerId()
        {
            var producerInfo = new NmsProducerInfo(firstId);
            Assert.AreSame(firstId, producerInfo.Id);
            Assert.AreSame(firstId.SessionId, producerInfo.SessionId);
            
            Assert.IsFalse(string.IsNullOrEmpty(producerInfo.ToString()));
        }

        [Test]
        public void TestHashCode()
        {
            var first = new NmsProducerInfo(firstId);
            var second = new NmsProducerInfo(secondId);

            Assert.AreEqual(first.GetHashCode(), first.GetHashCode());
            Assert.AreEqual(second.GetHashCode(), second.GetHashCode());

            Assert.AreNotEqual(first.GetHashCode(), second.GetHashCode());
        }

        [Test]
        public void TestEqualsCode()
        {
            var first = new NmsProducerInfo(firstId);
            var second = new NmsProducerInfo(secondId);

            Assert.AreEqual(first, first);
            Assert.AreEqual(second, second);

            Assert.AreNotEqual(first, second);
        }
    }
}