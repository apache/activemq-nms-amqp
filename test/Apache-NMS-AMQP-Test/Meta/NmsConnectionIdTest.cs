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
    public class NmsConnectionIdTest : NmsResourceIdTestBase
    {
        [Test]
        public void TestCreateFromStringThrowsWhenNull()
        {
            Assert.Catch<ArgumentException>(() => new NmsConnectionId(null));
        }

        [Test]
        public void TestCreateFromStringThrowsWhenEmpty()
        {
            Assert.Catch<ArgumentException>(() => new NmsConnectionId(string.Empty));
        }

        [Test]
        public void TestNmsConnectionIdFromNmsString()
        {
            var id = new IdGenerator().GenerateId();
            var connectionId = new NmsConnectionId(id);

            Assert.AreEqual(id, connectionId.ToString());
        }

        [Test]
        public void TestEquals()
        {
            var id1 = new IdGenerator().GenerateId();
            var id2 = new IdGenerator().GenerateId();
            var connectionId1 = new NmsConnectionId(id1);
            var connectionId2 = new NmsConnectionId(id2);
            var connectionId3 = new NmsConnectionId(id1);

            Assert.AreNotEqual(connectionId1, connectionId2);
            Assert.AreEqual(connectionId1, connectionId3);
        }

        [Test]
        public void TestHashCode()
        {
            var id1 = new IdGenerator().GenerateId();
            var id2 = new IdGenerator().GenerateId();
            var connectionId1 = new NmsConnectionId(id1);
            var connectionId2 = new NmsConnectionId(id2);
            var connectionId3 = new NmsConnectionId(id1);

            Assert.AreNotEqual(connectionId1.GetHashCode(), connectionId2.GetHashCode());
            Assert.AreEqual(connectionId1.GetHashCode(), connectionId3.GetHashCode());
        }

        [Test]
        public void TestToString()
        {
            var id = new IdGenerator().GenerateId();
            var connectionId = new NmsConnectionId(id);

            Assert.AreEqual(id, connectionId.ToString());
        }
    }
}