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
using NUnit.Framework;

namespace NMS.AMQP.Test.Meta
{
    [TestFixture]
    public class NmsSessionIdTest : NmsResourceIdTestBase
    {
        [Test]
        public void TestCreateFromConnectionIdThrowsWhenNull()
        {
            Assert.Catch<ArgumentException>(() => CreateSessionId(null));
        }

        [Test]
        public void TestNmsSessionIdFromNmsConnectionId()
        {
            var connectionId = CreateNmsConnectionId();
            var sessionId = CreateSessionId(connectionId, 1);

            Assert.AreEqual(connectionId, sessionId.ConnectionId);
            Assert.AreEqual(1, sessionId.Value);
        }

        [Test]
        public void TestEquals()
        {
            var connectionId1 = CreateNmsConnectionId();
            var connectionId2 = CreateNmsConnectionId();

            Assert.AreNotEqual(CreateSessionId(connectionId1, 1), CreateSessionId(connectionId1, 2));
            Assert.AreNotEqual(CreateSessionId(connectionId1, 1), CreateSessionId(connectionId2, 2));
            Assert.AreEqual(CreateSessionId(connectionId1, 1), CreateSessionId(connectionId1, 1));
        }

        [Test]
        public void TestHashCode()
        {
            var connectionId1 = CreateNmsConnectionId();
            var connectionId2 = CreateNmsConnectionId();

            Assert.AreNotEqual(CreateSessionId(connectionId1, 1).GetHashCode(), CreateSessionId(connectionId1, 2).GetHashCode());
            Assert.AreNotEqual(CreateSessionId(connectionId1, 1).GetHashCode(), CreateSessionId(connectionId2, 2).GetHashCode());
            Assert.AreEqual(CreateSessionId(connectionId1, 1).GetHashCode(), CreateSessionId(connectionId1, 1).GetHashCode());
        }
        
        [Test]
        public void TestToString()
        {
            var connectionId = CreateNmsConnectionId();
            var sessionId = CreateSessionId(connectionId, 1);
            
            Assert.AreEqual($"{connectionId}:1", sessionId.ToString());
        }
    }
}