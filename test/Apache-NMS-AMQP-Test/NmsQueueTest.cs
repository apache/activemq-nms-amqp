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
using NUnit.Framework;

namespace NMS.AMQP.Test
{
    [TestFixture]
    public class NmsQueueTest
    {
        [Test]
        public void TestIsQueue()
        {
            NmsQueue queue = new NmsQueue("myQueue");
            Assert.True(queue.IsQueue, "should be a queue");
        }

        [Test]
        public void TestIsTopic()
        {
            NmsQueue queue = new NmsQueue("myQueue");
            Assert.False(queue.IsTopic, "should not be a topic");
        }

        [Test]
        public void TestIsTemporary()
        {
            NmsQueue queue = new NmsQueue("myQueue");
            Assert.False(queue.IsTemporary, "should not be temporary");
        }
        
        [Test]
        public void TestTwoQueuesWithTheSameNamesAreEqual()
        {
            NmsQueue nmsQueue1 = new NmsQueue("myQueue");
            NmsQueue nmsQueue2 = new NmsQueue("myQueue");

            Assert.AreEqual(nmsQueue1, nmsQueue2);
            Assert.AreNotSame(nmsQueue1, nmsQueue2);
            Assert.AreEqual(nmsQueue1.GetHashCode(), nmsQueue2.GetHashCode());
        }
        
        [Test]
        public void TestTwoQueuesWithDifferentNamesAreNotEqual()
        {
            NmsQueue nmsQueue1 = new NmsQueue("myQueue");
            NmsQueue nmsQueue2 = new NmsQueue("myQueue2");

            Assert.AreNotEqual(nmsQueue1, nmsQueue2);
            Assert.AreNotEqual(nmsQueue1.GetHashCode(), nmsQueue2.GetHashCode());
        }
    }
}