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

using Apache.NMS;
using Apache.NMS.AMQP.Message;
using NMS.AMQP.Test.Message.Facade;
using NUnit.Framework;

namespace NMS.AMQP.Test.Message
{
    [TestFixture]
    public class NmsObjectMessageTest
    {
        private readonly INmsMessageFactory factory = new TestMessageFactory();

        [Test]
        public void TestToString()
        {
            NmsObjectMessage objectMessage = factory.CreateObjectMessage();
            Assert.True(objectMessage.ToString().StartsWith("NmsObjectMessage"));
        }

        [Test]
        public void TestReceivedObjectMessageThrowsMessageNotWriteableExceptionOnSetObject()
        {
            string content = "myStringContent";
            NmsTestObjectMessageFacade facade = new NmsTestObjectMessageFacade();
            facade.Body = content;
            NmsObjectMessage objectMessage = new NmsObjectMessage(facade);
            objectMessage.OnDispatch();

            Assert.Throws<MessageNotWriteableException>(() => objectMessage.Body = "newObject");
        }

        [Test]
        public void TestClearBodyOnReceivedObjectMessageMakesMessageWritable()
        {
            string content = "myStringContent";
            NmsTestObjectMessageFacade facade = new NmsTestObjectMessageFacade();
            facade.Body = content;
            NmsObjectMessage objectMessage = new NmsObjectMessage(facade);
            objectMessage.OnDispatch();

            Assert.True(objectMessage.IsReadOnlyBody);
            objectMessage.ClearBody();
            Assert.False(objectMessage.IsReadOnlyBody);
        }

        [Test]
        public void TestClearBodyOnReceivedObjectMessageClearsUnderlyingMessageBody()
        {
            string content = "myStringContent";
            NmsTestObjectMessageFacade facade = new NmsTestObjectMessageFacade();
            facade.Body = content;
            NmsObjectMessage objectMessage = new NmsObjectMessage(facade);
            objectMessage.OnDispatch();

            Assert.NotNull(objectMessage.Body);
            objectMessage.ClearBody();

            Assert.Null(facade.Body);
        }

        [Test]
        public void TestClearBody()
        {
            NmsObjectMessage objectMessage = factory.CreateObjectMessage();
            objectMessage.Body = "String";
            objectMessage.ClearBody();
            Assert.False(objectMessage.IsReadOnlyBody);
            Assert.Null(objectMessage.Body);
            objectMessage.Body = "String";
            object body = objectMessage.Body;
        }

        [Test]
        public void TestReadOnlyBody()
        {
            NmsObjectMessage objectMessage = factory.CreateObjectMessage();
            objectMessage.Body = "String";
            objectMessage.IsReadOnlyBody = true;
            object body = objectMessage.Body;
            try
            {
                objectMessage.Body = "test";
                Assert.Fail("should throw exception");
            }
            catch (MessageNotWriteableException)
            {
            }
        }

        [Test]
        public void TestWriteOnlyBody()
        {
            NmsObjectMessage msg = factory.CreateObjectMessage();
            msg.IsReadOnlyBody = false;

            msg.Body = "test";
            object body = msg.Body;

            msg.IsReadOnlyBody = true;
            body = msg.Body;
            
            try
            {
                msg.Body = "test";
                Assert.Fail("should throw exception");
            }
            catch (MessageNotWriteableException)
            {
            }
        }
        
        [Test]
        public void TestMessageCopy()
        {
            NmsObjectMessage message = factory.CreateObjectMessage();

            NmsObjectMessage copy = message.Copy() as NmsObjectMessage;
            Assert.IsNotNull(copy);
        }
    }
}