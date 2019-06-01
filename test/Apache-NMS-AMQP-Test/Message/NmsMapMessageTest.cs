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

using Apache.NMS.AMQP.Message;
using NMS.AMQP.Test.Message.Facade;
using NUnit.Framework;

namespace NMS.AMQP.Test.Message
{
    [TestFixture]
    public class NmsMapMessageTest
    {
        private readonly INmsMessageFactory factory = new TestMessageFactory();
        
        [Test]
        public void TestToString()
        {
            NmsMapMessage mapMessage = factory.CreateMapMessage();
            Assert.True(mapMessage.ToString().StartsWith("NmsMapMessage"));
        }

        [Test]
        public void TestGetMapNamesWithNewMessageToSendReturnsEmptyEnumeration()
        {
            NmsMapMessage mapMessage = factory.CreateMapMessage();
            CollectionAssert.IsEmpty(mapMessage.Body.Keys, "Expected new message to have no map names");
        }
        
        [Test]
        public void TestMessageCopy()
        {
            NmsMapMessage message = factory.CreateMapMessage();

            NmsMapMessage copy = message.Copy() as NmsMapMessage;
            Assert.IsNotNull(copy);
        }
        
        // TODO: Add rest of the tests.
    }
}