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
using Apache.NMS.AMQP.Provider.Amqp.Message;
using NMS.AMQP.Test.Message.Facade;
using NUnit.Framework;

namespace NMS.AMQP.Test.Message
{
    [TestFixture]
    public class NmsTextMessageTest
    {
        private readonly TestMessageFactory factory = new TestMessageFactory();

        [Test]
        public void TestSetText()
        {
            NmsTextMessage nmsTextMessage = factory.CreateTextMessage();
            string str = "testText";
            nmsTextMessage.Text = str;
            Assert.AreEqual(str, nmsTextMessage.Text);
        }

        [Test]
        public void TestClearBody()
        {
            NmsTextMessage nmsTextMessage = factory.CreateTextMessage();
            nmsTextMessage.Text = "string";
            nmsTextMessage.ClearBody();
            Assert.False(nmsTextMessage.IsReadOnly);
            Assert.IsNull(nmsTextMessage.Text);
            try
            {
                nmsTextMessage.Text = "String";
                var text = nmsTextMessage.Text;
            }
            catch (MessageNotWriteableException)
            {
                Assert.Fail("Should be writable");
            }
            catch (MessageNotReadableException)
            {
                Assert.Fail("should be readable");
            }
        }

        [Test]
        public void TestReadOnlyBody()
        {
            NmsTextMessage textMessage = factory.CreateTextMessage();
            textMessage.Text = "test";
            textMessage.IsReadOnly = true;

            try
            {
                string text = textMessage.Text;
            }
            catch (MessageNotReadableException)
            {
                Assert.Fail("should be readable");
            }
            
            try
            {
                textMessage.Text = "test";
                Assert.Fail("should throw exception");
            }
            catch (MessageNotWriteableException)
            {
            }
        }

        [Test]
        public void TestWriteOnlyBody()
        {
            NmsTextMessage textMessage = factory.CreateTextMessage();
            textMessage.IsReadOnly = false;

            try
            {
                textMessage.Text = "test";
                string text = textMessage.Text;
            }
            catch (MessageNotReadableException)
            {
                Assert.Fail("should be readable");
            }

            textMessage.IsReadOnly = true;
            try
            {
                string text = textMessage.Text;
                textMessage.Text = "test";
                Assert.Fail("should throw exception");
            }
            catch (MessageNotReadableException)
            {
                Assert.Fail("should be readable");
            }
            catch (MessageNotWriteableException) { }
        }

        [Test]
        public void TestToString()
        {
            NmsTextMessage textMessage = factory.CreateTextMessage();
            Assert.True(textMessage.ToString().StartsWith("NmsTextMessage"));
        }

        [Test]
        public void TestNullText()
        {
            NmsTextMessage textMessage = factory.CreateTextMessage();
            textMessage.Text = null;
            Assert.Null(textMessage.Text);
        }
        
        [Test]
        public void TestMessageCopy()
        {
            NmsTextMessage message = factory.CreateTextMessage();

            NmsTextMessage copy = message.Copy() as NmsTextMessage;
            Assert.IsNotNull(copy);
        }
    }
}