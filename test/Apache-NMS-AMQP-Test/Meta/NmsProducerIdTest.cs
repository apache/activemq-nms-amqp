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
    public class NmsProducerIdTest : NmsResourceIdTestBase
    {
        [Test]
        public void TestCreateFromSessionIdThrowsWhenNull()
        {
            Assert.Catch<ArgumentException>(() => new NmsProducerId(null, 0));
        }

        [Test]
        public void TestNmsProducerIdFromNmsSessionId()
        {
            var connectionId = CreateNmsConnectionId();
            var sessionId = CreateSessionId(connectionId);
            var producerId = new NmsProducerId(sessionId, 1);

            Assert.AreEqual(1, producerId.Value);
            Assert.AreEqual(producerId.SessionId, sessionId);
            Assert.AreEqual(producerId.ConnectionId, connectionId);
        }

        [Test]
        public void TestEquals()
        {
            var connectionId = CreateNmsConnectionId();
            var sessionId1 = new NmsSessionId(connectionId, 1);
            var sessionId2 = new NmsSessionId(connectionId, 2);

            Assert.AreNotEqual(new NmsProducerId(sessionId1, 1), new NmsProducerId(sessionId1, 2));
            Assert.AreNotEqual(new NmsProducerId(sessionId1, 1), new NmsProducerId(sessionId2, 1));
            Assert.AreEqual(new NmsProducerId(sessionId1, 1), new NmsProducerId(sessionId1, 1));
        }

        [Test]
        public void TestHashCode()
        {
            var connectionId = CreateNmsConnectionId();
            var sessionId1 = new NmsSessionId(connectionId, 1);
            var sessionId2 = new NmsSessionId(connectionId, 2);
            
            Assert.AreNotEqual(new NmsProducerId(sessionId1, 1).GetHashCode(), new NmsProducerId(sessionId1, 2).GetHashCode());
            Assert.AreNotEqual(new NmsProducerId(sessionId1, 1).GetHashCode(), new NmsProducerId(sessionId2, 1).GetHashCode());
            Assert.AreEqual(new NmsProducerId(sessionId1, 1).GetHashCode(), new NmsProducerId(sessionId1, 1).GetHashCode());
        }

        [Test]
        public void TestToString()
        {
            var connectionId = CreateNmsConnectionId();
            var sessionId = new NmsSessionId(connectionId, 1);
            var producerId = new NmsProducerId(sessionId, 1);
            
            Assert.AreEqual($"{connectionId}:1:1", producerId.ToString());
        }
    }
}