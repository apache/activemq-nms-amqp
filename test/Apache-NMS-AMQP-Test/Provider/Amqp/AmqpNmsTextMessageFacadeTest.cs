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
using System.Text;
using Amqp.Framing;
using Apache.NMS;
using Apache.NMS.AMQP.Provider.Amqp.Message;
using Apache.NMS.AMQP.Util;
using NUnit.Framework;

namespace NMS.AMQP.Test.Provider.Amqp
{
    [TestFixture]
    public class AmqpNmsTextMessageFacadeTest : AmqpNmsMessageTypesTestCase
    {
        //---------- Test initial state of newly created message -----------------//
        [Test]
        public void TestNewMessageToSendDoesNotContainMessageTypeAnnotation()
        {
            AmqpNmsTextMessageFacade textMessageFacade = CreateNewTextMessageFacade();


            MessageAnnotations annotations = textMessageFacade.MessageAnnotations;

            Assert.Null(annotations, "MessageAnnotations section was present");
            Assert.AreEqual(MessageSupport.JMS_TYPE_TXT, textMessageFacade.JmsMsgType);
        }

        [Test]
        public void TestNewMessageToSendClearBodyDoesNotFail()
        {
            AmqpNmsTextMessageFacade textMessageFacade = CreateNewTextMessageFacade();
            textMessageFacade.ClearBody();
        }

        [Test]
        public void TestNewMessageToSendReturnsNullText()
        {
            AmqpNmsTextMessageFacade textMessageFacade = CreateNewTextMessageFacade();
            textMessageFacade.ClearBody();
            Assert.Null(textMessageFacade.Text);
        }

        // ---------- test for normal message operations -------------------------//

        [Test]
        public void TestMessageClearBodyWorks()
        {
            AmqpNmsTextMessageFacade textMessageFacade = CreateNewTextMessageFacade();
            Assert.Null(textMessageFacade.Text);
            textMessageFacade.Text = "SomeTextForMe";
            Assert.NotNull(textMessageFacade.Text);
            textMessageFacade.ClearBody();
            Assert.Null(textMessageFacade.Text);
        }

        [Test]
        public void TestSetGetTextWithNewMessageToSend()
        {
            string text = "myTestText";
            AmqpNmsTextMessageFacade textMessageFacade = CreateNewTextMessageFacade();

            textMessageFacade.Text = text;
            Assert.NotNull(textMessageFacade.Message.BodySection);
            Assert.IsInstanceOf<AmqpValue>(textMessageFacade.Message.BodySection);
            Assert.AreEqual(text, ((AmqpValue) textMessageFacade.Message.BodySection).Value);
            Assert.AreEqual(text, textMessageFacade.Text);
        }

        // ---------- test handling of received messages -------------------------//

        [Test]
        public void TestCreateWithEmptyAmqpValue()
        {
            global::Amqp.Message message = new global::Amqp.Message
            {
                BodySection = new AmqpValue {Value = null}
            };

            AmqpNmsTextMessageFacade textMessageFacade = CreateReceivedTextMessageFacade(message);

            // Should be able to use the message, e.g clearing it and adding to it.
            textMessageFacade.ClearBody();
            textMessageFacade.Text = "TEST";
            Assert.AreEqual("TEST", textMessageFacade.Text);
        }

        [Test]
        public void TestCreateWithNonEmptyAmqpValue()
        {
            global::Amqp.Message message = new global::Amqp.Message
            {
                BodySection = new AmqpValue {Value = "TEST"}
            };

            AmqpNmsTextMessageFacade textMessageFacade = CreateReceivedTextMessageFacade(message);

            Assert.AreEqual("TEST", textMessageFacade.Text);

            // Should be able to use the message, e.g clearing it and adding to it.
            textMessageFacade.ClearBody();
            textMessageFacade.Text = "TEST-CLEARED";
            Assert.AreEqual("TEST-CLEARED", textMessageFacade.Text);
        }

        [Test]
        public void TestGetTextUsingReceivedMessageWithNoBodySectionReturnsNull()
        {
            global::Amqp.Message message = new global::Amqp.Message();
            AmqpNmsTextMessageFacade textMessageFacade = CreateReceivedTextMessageFacade(message);

            Assert.Null(textMessageFacade.Text, "expected null string");
        }

        [Test]
        public void TestGetTextUsingReceivedMessageWithAmqpValueSectionContainingNull()
        {
            global::Amqp.Message message = new global::Amqp.Message
            {
                BodySection = new AmqpValue {Value = null}
            };

            AmqpNmsTextMessageFacade textMessageFacade = CreateReceivedTextMessageFacade(message);
            Assert.Null(textMessageFacade.Text, "expected null string");
        }

        [Test]
        public void TestGetTextUsingReceivedMessageWithDataSectionContainingNothingReturnsEmptyString()
        {
            global::Amqp.Message message = new global::Amqp.Message
            {
                BodySection = new Data() {Binary = null}
            };

            // This shouldn't happen with actual received messages, since Data sections can't really
            // have a null value in them, they would have an empty byte array, but just in case...
            AmqpNmsTextMessageFacade textMessageFacade = CreateReceivedTextMessageFacade(message);

            Assert.AreEqual(string.Empty, textMessageFacade.Text, "expected zero-length string");
        }

        [Test]
        public void TestGetTextUsingReceivedMessageWithZeroLengthDataSectionReturnsEmptyString()
        {
            byte[] bytes = Encoding.UTF8.GetBytes("");

            global::Amqp.Message message = new global::Amqp.Message
            {
                BodySection = new Data() {Binary = bytes}
            };

            AmqpNmsTextMessageFacade textMessageFacade = CreateReceivedTextMessageFacade(message);
            Assert.AreEqual(string.Empty, textMessageFacade.Text, "expected zero-length string");
        }

        [Test]
        public void TestGetTextUsingReceivedMessageWithDataSectionContainingStringBytes()
        {
            string encodedString = "myEncodedString";
            byte[] bytes = Encoding.UTF8.GetBytes(encodedString);

            global::Amqp.Message message = new global::Amqp.Message
            {
                BodySection = new Data() {Binary = bytes}
            };

            AmqpNmsTextMessageFacade textMessageFacade = CreateReceivedTextMessageFacade(message);
            Assert.AreEqual(encodedString, textMessageFacade.Text);
        }

        [Test]
        public void TestGetTextWithNonAmqpValueOrDataSectionReportsNoBody()
        {
            global::Amqp.Message message = new global::Amqp.Message
            {
                BodySection = new AmqpSequence() {List = new List<object>()}
            };

            AmqpNmsTextMessageFacade textMessageFacade = CreateReceivedTextMessageFacade(message);

            Assert.False(textMessageFacade.HasBody());
        }

        [Test]
        public void TestGetTextWithNonAmqpValueOrDataSectionThrowsIse()
        {
            global::Amqp.Message message = new global::Amqp.Message
            {
                BodySection = new AmqpSequence() {List = new List<object>()}
            };

            AmqpNmsTextMessageFacade textMessageFacade = CreateReceivedTextMessageFacade(message);

            Assert.Catch<IllegalStateException>(() =>
            {
                string text = textMessageFacade.Text;
            });
        }

        [Test]
        public void TestGetTextWithAmqpValueContainingNonNullNonStringValueThrowsIse()
        {
            global::Amqp.Message message = new global::Amqp.Message
            {
                BodySection = new AmqpValue() {Value = true}
            };
            
            AmqpNmsTextMessageFacade textMessageFacade = CreateReceivedTextMessageFacade(message);

            Assert.Catch<IllegalStateException>(() =>
            {
                string text = textMessageFacade.Text;
            });
        }

        [Test]
        public void TestMessageCopy()
        {
            AmqpNmsTextMessageFacade textMessageFacade = CreateNewTextMessageFacade();
            textMessageFacade.Text = "SomeTextForMe";

            AmqpNmsTextMessageFacade copy = textMessageFacade.Copy() as AmqpNmsTextMessageFacade;
            Assert.IsNotNull(copy);
            Assert.AreEqual(textMessageFacade.Text, copy.Text);
        }
    }
}