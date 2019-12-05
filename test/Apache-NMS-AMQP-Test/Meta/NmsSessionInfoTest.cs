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
using Apache.NMS.AMQP.Meta;
using Apache.NMS.AMQP.Util;
using NUnit.Framework;

namespace NMS.AMQP.Test.Meta
{
    public class NmsSessionInfoTest
    {
        private NmsSessionId firstId;
        private NmsSessionId secondId;

        private NmsConnectionInfo connectionInfo;

        [SetUp]
        public void SetUp() {
            var generator = new IdGenerator();
            
            var connectionId = new NmsConnectionId(generator.GenerateId());

            firstId = new NmsSessionId(connectionId, 1);
            secondId = new NmsSessionId(connectionId, 2);
            
            connectionInfo = new NmsConnectionInfo(connectionId);
        }

        [Test]
        public void TestExceptionWhenCreatedWithNullConnectionId()
        {
            Assert.Catch<ArgumentException>(() => new NmsSessionInfo(null, 1));
        }
        
        [Test]
        public void TestExceptionWhenCreatedWithNullSessionId()
        {
            Assert.Catch<ArgumentException>(() => new NmsSessionInfo(null));
        }

        [Test]
        public void TestCreateFromSessionId()
        {
            var sessionInfo = new NmsSessionInfo(firstId);
            Assert.AreSame(firstId, sessionInfo.Id);
            Assert.IsFalse(string.IsNullOrEmpty(sessionInfo.ToString()));
        }

        [Test]
        public void TestCreateFromConnectionInfo()
        {
            var sessionInfo = new NmsSessionInfo(connectionInfo, 1);
            Assert.AreEqual(connectionInfo.Id, sessionInfo.Id.ConnectionId);
        }

        [Test]
        public void TestIsTransacted()
        {
            var sessionInfo = new NmsSessionInfo(firstId);
            sessionInfo.AcknowledgementMode = AcknowledgementMode.AutoAcknowledge;
            Assert.IsFalse(sessionInfo.IsTransacted);
            sessionInfo.AcknowledgementMode = AcknowledgementMode.Transactional;
            Assert.IsTrue(sessionInfo.IsTransacted);
        }
        
        [Test]
        public void TestHashCode()
        {
            var first = new NmsSessionInfo(firstId);
            var second = new NmsSessionInfo(secondId);

            Assert.AreEqual(first.GetHashCode(), first.GetHashCode());
            Assert.AreEqual(second.GetHashCode(), second.GetHashCode());

            Assert.AreNotEqual(first.GetHashCode(), second.GetHashCode());
        }

        [Test]
        public void TestEqualsCode()
        {
            var first = new NmsSessionInfo(firstId);
            var second = new NmsSessionInfo(secondId);

            Assert.AreEqual(first, first);
            Assert.AreEqual(second, second);

            Assert.AreNotEqual(first, second);
        }
    }
}