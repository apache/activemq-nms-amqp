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
    public class NmsTopicTest
    {
        [Test]
        public void TestIsQueue()
        {
            NmsTopic topic = new NmsTopic("myTopic");
            Assert.False(topic.IsQueue, "should not be a queue");
        }

        [Test]
        public void TestIsTopic()
        {
            NmsTopic topic = new NmsTopic("myTopic");
            Assert.True(topic.IsTopic, "should be a topic");
        }

        [Test]
        public void TestIsTemporary()
        {
            NmsTopic topic = new NmsTopic("myTopic");
            Assert.False(topic.IsTemporary, "should not be temporary");
        }
    }
}