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
using Apache.NMS.Util;
using NUnit.Framework;
using System;
using System.Text;
using System.IO;
using Apache.NMS.Amqp;

namespace Apache.NMS.Amqp.Test
{
    [TestFixture]
    public class TextMessageTest
    {
        [Test]
        public void TestCommand()
        {
            TextMessage message = new TextMessage();

            Assert.IsNull(message.Text);

            // Test with ASCII Data.
            message.Text = "Hello World";
            Assert.IsNotNull(message.Text);
            Assert.AreEqual("Hello World", message.Text);

            String unicodeString =
                "This unicode string contains two characters " +
                "with codes outside an 8-bit code range, " +
                "Pi (\u03a0) and Sigma (\u03a3).";

            message.Text = unicodeString;
            Assert.IsNotNull(message.Text);
            Assert.AreEqual(unicodeString, message.Text);
        }

        [Test]
        public void TestShallowCopy()
        {
            TextMessage msg = new TextMessage();
            string testString = "str";
            msg.Text = testString;
            TextMessage copy = msg.Clone() as TextMessage;
            Assert.IsTrue(msg.Text == ((TextMessage) copy).Text);
        }

        [Test]
        public void TestSetText()
        {
            TextMessage msg = new TextMessage();
            string str = "testText";
            msg.Text = str;
            Assert.AreEqual(msg.Text, str);
        }

        [Test]
        public void TestClearBody()
        {
            TextMessage textMessage = new TextMessage();
            textMessage.Text = "string";
            textMessage.ClearBody();
            Assert.IsFalse(textMessage.ReadOnlyBody);
            Assert.IsNull(textMessage.Text);
            try
            {
                textMessage.Text = "String";
                Assert.IsTrue(textMessage.Text.Length > 0);
            }
            catch(MessageNotWriteableException)
            {
                Assert.Fail("should be writeable");
            }
            catch(MessageNotReadableException)
            {
                Assert.Fail("should be readable");
            }
        }

        // TODO: Support read-only/write-only
#if false
        [Test]
        public void TestReadOnlyBody()
        {
            TextMessage textMessage = new TextMessage();
            textMessage.Text = "test";
            textMessage.ReadOnlyBody = true;
            try
            {
                Assert.IsTrue(textMessage.Text.Length > 0);
            }
            catch(MessageNotReadableException)
            {
                Assert.Fail("should be readable");
            }
            try
            {
                textMessage.Text = "test";
                Assert.Fail("should throw exception");
            }
            catch(MessageNotWriteableException)
            {
            }
        }

        [Test]
        public void TtestWriteOnlyBody()
        {
            // should always be readable
            TextMessage textMessage = new TextMessage();
            textMessage.ReadOnlyBody = false;
            try
            {
                textMessage.Text = "test";
                Assert.IsTrue(textMessage.Text.Length > 0);
            }
            catch(MessageNotReadableException)
            {
                Assert.Fail("should be readable");
            }
            textMessage.ReadOnlyBody = true;
            try
            {
                Assert.IsTrue(textMessage.Text.Length > 0);
                textMessage.Text = "test";
                Assert.Fail("should throw exception");
            }
            catch(MessageNotReadableException)
            {
                Assert.Fail("should be readable");
            }
            catch(MessageNotWriteableException)
            {
            }
        }
#endif

        [Test]
        public void TestNullText()
        {
            TextMessage nullMessage = new TextMessage();
            Assert.IsNull(nullMessage.Text);
        }
    }
}
