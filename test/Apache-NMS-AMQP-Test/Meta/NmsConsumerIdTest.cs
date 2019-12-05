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
using NUnit.Framework;

namespace NMS.AMQP.Test.Meta
{
    [TestFixture]
    public class NmsConsumerIdTest : NmsResourceIdTestBase
    {
        [Test]
        public void TestCreateFromSessionIdThrowsWhenNull()
        {
            Assert.Catch<ArgumentException>(() => new NmsConsumerId(null, 0));
        }
        
        [Test]
        public void TestNmsSessionIdFromNmsConnectionId()
        {
            var connectionId = CreateNmsConnectionId();
            var sessionId = CreateSessionId(connectionId, 1);

            var consumerId = new NmsConsumerId(sessionId, 1);

            Assert.AreEqual(connectionId, consumerId.ConnectionId);
            Assert.AreEqual(1, consumerId.Value);
        }
        
        [Test]
        public void TestEquals()
        {
            var connectionId = CreateNmsConnectionId();
            var sessionId1 = new NmsSessionId(connectionId, 1);
            var sessionId2 = new NmsSessionId(connectionId, 2);

            Assert.AreNotEqual(new NmsConsumerId(sessionId1, 1), new NmsConsumerId(sessionId1, 2));
            Assert.AreNotEqual(new NmsConsumerId(sessionId1, 1), new NmsConsumerId(sessionId2, 1));
            Assert.AreEqual(new NmsConsumerId(sessionId1, 1), new NmsConsumerId(sessionId1, 1));
        }

        [Test]
        public void TestHashCode()
        {
            var connectionId = CreateNmsConnectionId();
            var sessionId1 = new NmsSessionId(connectionId, 1);
            var sessionId2 = new NmsSessionId(connectionId, 2);
            
            Assert.AreNotEqual(new NmsConsumerId(sessionId1, 1).GetHashCode(), new NmsConsumerId(sessionId1, 2).GetHashCode());
            Assert.AreNotEqual(new NmsConsumerId(sessionId1, 1).GetHashCode(), new NmsConsumerId(sessionId2, 1).GetHashCode());
            Assert.AreEqual(new NmsConsumerId(sessionId1, 1).GetHashCode(), new NmsConsumerId(sessionId1, 1).GetHashCode());
        }

        [Test]
        public void TestToString()
        {
            var connectionId = CreateNmsConnectionId();
            var sessionId = new NmsSessionId(connectionId, 1);
            var consumerId = new NmsConsumerId(sessionId, 1);
            
            Assert.AreEqual($"{connectionId}:1:1", consumerId.ToString());
        }
    }
}