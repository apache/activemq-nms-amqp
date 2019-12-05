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

using Apache.NMS.AMQP;
using Apache.NMS.AMQP.Meta;
using Apache.NMS.AMQP.Util;
using NUnit.Framework;

namespace NMS.AMQP.Test
{
    [TestFixture]
    public class NmsTemporaryQueueTest
    {
        [Test]
        public void TestTwoTemporaryQueuesWithTheSameAddressesAreEqual()
        {
            NmsTemporaryQueue temporaryQueue1 = new NmsTemporaryQueue("myTemporaryQueue");
            NmsTemporaryQueue temporaryQueue2 = new NmsTemporaryQueue("myTemporaryQueue");

            Assert.AreEqual(temporaryQueue1, temporaryQueue2);
            Assert.AreNotSame(temporaryQueue1, temporaryQueue2);
            Assert.AreEqual(temporaryQueue1.GetHashCode(), temporaryQueue2.GetHashCode());
        }

        [Test]
        public void TestTwoTemporaryQueuesWithDifferentAddressesAreNotEqual()
        {
            var connectionId = new NmsConnectionId("1");
            NmsTemporaryQueue temporaryQueue1 = new NmsTemporaryQueue("myTemporaryQueue");
            NmsTemporaryQueue temporaryQueue2 = new NmsTemporaryQueue("myTemporaryQueue2");

            Assert.AreNotEqual(temporaryQueue1, temporaryQueue2);
            Assert.AreNotEqual(temporaryQueue1.GetHashCode(), temporaryQueue2.GetHashCode());
        }
    }
}