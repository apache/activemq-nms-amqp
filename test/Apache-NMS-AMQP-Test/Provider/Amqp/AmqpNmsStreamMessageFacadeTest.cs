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

using System.Collections.Generic;
using Amqp.Framing;
using Apache.NMS;
using Apache.NMS.AMQP.Provider.Amqp.Message;
using Apache.NMS.AMQP.Util;
using NUnit.Framework;

namespace NMS.AMQP.Test.Provider.Amqp
{
    [TestFixture]
    public class AmqpNmsStreamMessageFacadeTest : AmqpNmsMessageTypesTestCase
    {
        [Test]
        public void TestNewMessageToSendReportsNoBody()
        {
            AmqpNmsStreamMessageFacade amqpNmsStreamMessageFacade = CreateNewStreamMessageFacade();
            Assert.False(amqpNmsStreamMessageFacade.HasBody(), "Message should report no body");
        }

        [Test]
        public void TestNewMessageToSendDoesNotContainMessageTypeAnnotation()
        {
            AmqpNmsStreamMessageFacade amqpNmsStreamMessageFacade = CreateNewStreamMessageFacade();
            MessageAnnotations annotations = amqpNmsStreamMessageFacade.MessageAnnotations;

            Assert.Null(annotations, "MessageAnnotations section was present");
            Assert.AreEqual(MessageSupport.JMS_TYPE_STRM, amqpNmsStreamMessageFacade.JmsMessageType);
        }

        [Test]
        public void TestNewMessageToSendContainsAmqpSequenceBody()
        {
            AmqpNmsStreamMessageFacade amqpNmsStreamMessageFacade = CreateNewStreamMessageFacade();
            object body = amqpNmsStreamMessageFacade.Message.BodySection;

            Assert.NotNull(body, "Body section was not present");
            Assert.That(body, Is.InstanceOf<AmqpSequence>(), "Body section was not of expected type: " + body.GetType());
        }

        [Test]
        public void TestPeekWithNewMessageToSendThrowsMEOFE()
        {
            AmqpNmsStreamMessageFacade amqpNmsStreamMessageFacade = CreateNewStreamMessageFacade();

            Assert.Throws<MessageEOFException>(() => amqpNmsStreamMessageFacade.Peek());
        }

        [Test]
        public void TestPopWithNewMessageToSendThrowsMEOFE()
        {
            AmqpNmsStreamMessageFacade amqpNmsStreamMessageFacade = CreateNewStreamMessageFacade();

            Assert.Throws<MessageEOFException>(() => amqpNmsStreamMessageFacade.Pop());
        }

        [Test]
        public void TestPeekUsingReceivedMessageWithAmqpValueBodyReturnsExpectedValue()
        {
            global::Amqp.Message message = new global::Amqp.Message();
            List<object> list = new List<object>();
            list.Add(false);
            message.BodySection = new AmqpValue() {Value = list};

            AmqpNmsStreamMessageFacade amqpNmsStreamMessageFacade = CreateReceivedStreamMessageFacade(message);

            Assert.AreEqual(false, amqpNmsStreamMessageFacade.Peek(), "Unexpected value retrieved");
        }

        [Test]
        public void TestPeekUsingReceivedMessageWithAmqpSequenceBodyReturnsExpectedValue()
        {
            global::Amqp.Message message = new global::Amqp.Message();
            List<object> list = new List<object>();
            list.Add(false);
            message.BodySection = new AmqpSequence() {List = list};

            AmqpNmsStreamMessageFacade amqpNmsStreamMessageFacade = CreateReceivedStreamMessageFacade(message);

            Assert.AreEqual(false, amqpNmsStreamMessageFacade.Peek(), "Unexpected value retrieved");
        }

        [Test]
        public void TestRepeatedPeekReturnsExpectedValue()
        {
            global::Amqp.Message message = new global::Amqp.Message();
            List<object> list = new List<object>();
            list.Add(false);
            list.Add(true);
            message.BodySection = new AmqpSequence() {List = list};

            AmqpNmsStreamMessageFacade amqpNmsStreamMessageFacade = CreateReceivedStreamMessageFacade(message);

            Assert.True(amqpNmsStreamMessageFacade.HasBody(), "Message should report that it contains a body");
            Assert.AreEqual(false, amqpNmsStreamMessageFacade.Peek(), "Unexpected value retrieved");
            Assert.AreEqual(false, amqpNmsStreamMessageFacade.Peek(), "Unexpected value retrieved");
        }

        [Test]
        public void TestRepeatedPeekAfterPopReturnsExpectedValue()
        {
            global::Amqp.Message message = new global::Amqp.Message();
            List<object> list = new List<object>();
            list.Add(false);
            list.Add(true);
            message.BodySection = new AmqpSequence() {List = list};

            AmqpNmsStreamMessageFacade amqpNmsStreamMessageFacade = CreateReceivedStreamMessageFacade(message);

            Assert.AreEqual(false, amqpNmsStreamMessageFacade.Peek(), "Unexpected value retrieved");
            amqpNmsStreamMessageFacade.Pop();
            Assert.AreEqual(true, amqpNmsStreamMessageFacade.Peek(), "Unexpected value retrieved");
        }

        [Test]
        public void TestResetPositionAfterPop()
        {
            global::Amqp.Message message = new global::Amqp.Message();
            List<object> list = new List<object>();
            list.Add(false);
            list.Add(true);
            message.BodySection = new AmqpSequence() {List = list};

            AmqpNmsStreamMessageFacade amqpNmsStreamMessageFacade = CreateReceivedStreamMessageFacade(message);

            Assert.AreEqual(false, amqpNmsStreamMessageFacade.Peek(), "Unexpected value retrieved");
            amqpNmsStreamMessageFacade.Pop();

            amqpNmsStreamMessageFacade.Reset();

            Assert.AreEqual(false, amqpNmsStreamMessageFacade.Peek(), "Unexpected value retrieved");
            amqpNmsStreamMessageFacade.Pop();

            Assert.AreEqual(true, amqpNmsStreamMessageFacade.Peek(), "Unexpected value retrieved");
            amqpNmsStreamMessageFacade.Pop();
        }

        [Test]
        public void TestResetPositionAfterPeekThrowsMEOFE()
        {
            global::Amqp.Message message = new global::Amqp.Message();
            List<object> list = new List<object>();
            list.Add(false);
            list.Add(true);
            message.BodySection = new AmqpSequence() {List = list};

            AmqpNmsStreamMessageFacade amqpNmsStreamMessageFacade = CreateReceivedStreamMessageFacade(message);

            Assert.AreEqual(false, amqpNmsStreamMessageFacade.Peek(), "Unexpected value retrieved");
            amqpNmsStreamMessageFacade.Pop();
            Assert.AreEqual(true, amqpNmsStreamMessageFacade.Peek(), "Unexpected value retrieved");
            amqpNmsStreamMessageFacade.Pop();

            Assert.Throws<MessageEOFException>(() => amqpNmsStreamMessageFacade.Peek(), "expected exception to be thrown");

            amqpNmsStreamMessageFacade.Reset();

            Assert.AreEqual(false, amqpNmsStreamMessageFacade.Peek(), "Unexpected value retrieved");
            amqpNmsStreamMessageFacade.Pop();
            Assert.AreEqual(true, amqpNmsStreamMessageFacade.Peek(), "Unexpected value retrieved");
        }

        [Test]
        public void TestClearBody()
        {
            AmqpNmsStreamMessageFacade amqpNmsStreamMessageFacade = CreateNewStreamMessageFacade();

            // add some stuff
            amqpNmsStreamMessageFacade.Put(true);
            amqpNmsStreamMessageFacade.Put(false);

            // retrieve only some of it, leaving some unread
            Assert.AreEqual(true, amqpNmsStreamMessageFacade.Peek(), "Unexpected value retrieved");
            amqpNmsStreamMessageFacade.Pop();

            // clear
            amqpNmsStreamMessageFacade.ClearBody();

            // add something else
            amqpNmsStreamMessageFacade.Put('c');

            // check we can get it alone before another IOOBE (i.e position was reset, other contents cleared)

            Assert.AreEqual('c', amqpNmsStreamMessageFacade.Peek(), "Unexpected value retrieved");
            amqpNmsStreamMessageFacade.Pop();

            Assert.Throws<MessageEOFException>(() => amqpNmsStreamMessageFacade.Peek());
        }

        [Test]
        public void TestPopFullyReadListThrowsMEOFE()
        {
            global::Amqp.Message message = new global::Amqp.Message();
            List<object> list = new List<object>();
            list.Add(false);
            message.BodySection = new AmqpSequence() {List = list};

            AmqpNmsStreamMessageFacade amqpNmsStreamMessageFacade = CreateReceivedStreamMessageFacade(message);

            Assert.AreEqual(false, amqpNmsStreamMessageFacade.Peek(), "Unexpected value retrieved");
            amqpNmsStreamMessageFacade.Pop();

            Assert.Throws<MessageEOFException>(() => amqpNmsStreamMessageFacade.Pop());
        }

        [Test]
        public void TestCreateWithUnexpectedBodySectionTypeThrowsISE()
        {
            global::Amqp.Message message = new global::Amqp.Message();
            message.BodySection = new Data {Binary = new byte[0]};

            Assert.Throws<IllegalStateException>(() => CreateReceivedStreamMessageFacade(message));
        }

        [Test]
        public void TestCreateWithAmqpValueBodySectionContainingUnexpectedValueThrowsISE()
        {
            global::Amqp.Message message = new global::Amqp.Message();
            message.BodySection = new AmqpValue() {Value = "not-a-list"};
            
            Assert.Throws<IllegalStateException>(() => CreateReceivedStreamMessageFacade(message));
        }

        [Test]
        public void TestCreateWithEmptyAmqpValueBodySection()
        {
            global::Amqp.Message message = new global::Amqp.Message();
            message.BodySection = new AmqpValue() {Value = null};

            AmqpNmsStreamMessageFacade amqpNmsStreamMessageFacade = CreateReceivedStreamMessageFacade(message);
            
            //Should be able to use the message, e.g clearing it and adding to it.
            amqpNmsStreamMessageFacade.ClearBody();
            amqpNmsStreamMessageFacade.Put("myString");
        }

        [Test]
        public void TestCreateWithEmptyAmqpSequenceBodySection()
        {
            global::Amqp.Message message = new global::Amqp.Message();
            message.BodySection = new AmqpSequence() {List = null};

            AmqpNmsStreamMessageFacade amqpNmsStreamMessageFacade = CreateReceivedStreamMessageFacade(message);
            
            //Should be able to use the message, e.g clearing it and adding to it.
            amqpNmsStreamMessageFacade.ClearBody();
            amqpNmsStreamMessageFacade.Put("myString");
        }

        [Test]
        public void TestCreateWithNoBodySection()
        {
            global::Amqp.Message message = new global::Amqp.Message();
            message.BodySection = null;
            
            AmqpNmsStreamMessageFacade amqpNmsStreamMessageFacade = CreateReceivedStreamMessageFacade(message);
            
            //Should be able to use the message, e.g clearing it and adding to it.
            amqpNmsStreamMessageFacade.ClearBody();
            amqpNmsStreamMessageFacade.Put("myString");
        }

        [Test]
        public void TestMessageCopy()
        {
            AmqpNmsStreamMessageFacade amqpNmsStreamMessageFacade = CreateNewStreamMessageFacade();
            amqpNmsStreamMessageFacade.Put(true);
            amqpNmsStreamMessageFacade.Put("test");
            amqpNmsStreamMessageFacade.Put(1);

            AmqpNmsStreamMessageFacade copy = amqpNmsStreamMessageFacade.Copy() as AmqpNmsStreamMessageFacade;
            Assert.IsNotNull(copy);
            Assert.AreEqual(true, copy.Peek());
            copy.Pop();
            Assert.AreEqual("test", copy.Peek());
            copy.Pop();
            Assert.AreEqual(1, copy.Peek());
            copy.Pop();
        }
    }
}