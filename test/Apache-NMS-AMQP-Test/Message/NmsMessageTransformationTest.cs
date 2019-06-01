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
using System.Collections.Generic;
using Apache.NMS;
using Apache.NMS.AMQP;
using Apache.NMS.AMQP.Message;
using NMS.AMQP.Test.Message.Facade;
using NMS.AMQP.Test.Message.Foreign;
using NUnit.Framework;

namespace NMS.AMQP.Test.Message
{
    [TestFixture]
    public class NmsMessageTransformationTest
    {
        private readonly TestMessageFactory factory = new TestMessageFactory();

        [Test]
        public void TestTransformNmsMessage()
        {
            NmsMessage orig = new NmsMessage(new NmsTestMessageFacade());

            orig.NMSMessageId = "ID:CONNECTION:1:1";

            NmsMessage transformed = NmsMessageTransformation.TransformMessage(factory, orig);

            Assert.NotNull(transformed);
            Assert.AreEqual(orig, transformed);
            Assert.AreNotSame(orig, transformed);
        }

        [Test]
        public void TestForeignMessageTransformCreateNewMessage()
        {
            ForeignNmsMessage foreignMessage = new ForeignNmsMessage();

            NmsMessage transformed = NmsMessageTransformation.TransformMessage(factory, foreignMessage);

            Assert.AreNotSame(foreignMessage, transformed);
            Assert.AreNotEqual(foreignMessage, transformed);
        }

        [Test]
        public void TestEmptyForeignBytesMessageTransformCreateNewMessage()
        {
            ForeignNmsBytesMessage foreignMessage = new ForeignNmsBytesMessage();

            NmsMessage transformed = NmsMessageTransformation.TransformMessage(factory, foreignMessage);

            Assert.AreNotSame(foreignMessage, transformed);
            Assert.IsInstanceOf<NmsBytesMessage>(transformed);
            NmsBytesMessage message = (NmsBytesMessage) transformed;
            message.Reset();
            Assert.AreEqual(0, message.BodyLength);
        }

        [Test]
        public void TestForeignBytesMessageTransformCreateNewMessage()
        {
            ForeignNmsBytesMessage foreignMessage = new ForeignNmsBytesMessage();
            foreignMessage.WriteBoolean(true);
            foreignMessage.Properties.SetBool("boolProperty", true);

            NmsMessage transformed = NmsMessageTransformation.TransformMessage(factory, foreignMessage);

            Assert.AreNotSame(foreignMessage, transformed);
            Assert.IsInstanceOf<NmsBytesMessage>(transformed);
            NmsBytesMessage message = (NmsBytesMessage) transformed;
            message.Reset();
            Assert.IsTrue(message.BodyLength > 0);
            Assert.IsTrue(message.ReadBoolean());
            Assert.IsTrue(message.Properties.Contains("boolProperty"));
        }

        [Test]
        public void TestEmptyForeignTextMessageTransformCreateNewMessage()
        {
            ForeignNmsTextMessage foreignMessage = new ForeignNmsTextMessage();

            NmsMessage transformed = NmsMessageTransformation.TransformMessage(factory, foreignMessage);

            Assert.IsNotNull(transformed);
            Assert.AreNotSame(foreignMessage, transformed);

            Assert.IsInstanceOf<NmsTextMessage>(transformed);
            NmsTextMessage message = (NmsTextMessage) transformed;
            Assert.IsNull(message.Text);
        }

        [Test]
        public void TestForeignTextMessageTransformCreateNewMessage()
        {
            string messageBody = "TEST-MESSAGE-BODY";
            ForeignNmsTextMessage foreignMessage = new ForeignNmsTextMessage { Text = messageBody };

            NmsMessage transformed = NmsMessageTransformation.TransformMessage(factory, foreignMessage);

            Assert.IsNotNull(transformed);
            Assert.AreNotSame(foreignMessage, transformed);

            Assert.IsInstanceOf<NmsTextMessage>(transformed);
            NmsTextMessage message = (NmsTextMessage) transformed;
            Assert.AreEqual(messageBody, message.Text);
        }

        [Test]
        public void TestEmptyForeignMapMessageTransformCreateNewMessage()
        {
            ForeignNmsMapMessage foreignMessage = new ForeignNmsMapMessage();

            NmsMessage transformed = NmsMessageTransformation.TransformMessage(factory, foreignMessage);

            Assert.IsNotNull(transformed);
            Assert.AreNotSame(foreignMessage, transformed);

            Assert.IsInstanceOf<NmsMapMessage>(transformed);

            NmsMapMessage message = (NmsMapMessage) transformed;
            CollectionAssert.IsEmpty(message.Body.Keys);
        }

        [Test]
        public void TestForeignMapMessageTransformCreateNewMessage()
        {
            ForeignNmsMapMessage foreignMessage = new ForeignNmsMapMessage();

            foreignMessage.Body.SetBool("bool", true);
            foreignMessage.Body.SetChar("char", 'a');
            foreignMessage.Body.SetString("string", "string");
            foreignMessage.Body.SetByte("byte", 1);
            foreignMessage.Body.SetShort("short", 1);
            foreignMessage.Body.SetInt("int", 1);
            foreignMessage.Body.SetLong("long", 1);
            foreignMessage.Body.SetFloat("float", 1.5F);
            foreignMessage.Body.SetDouble("double", 1.5D);
            foreignMessage.Body.SetList("list", new List<string>() { "a" });
            foreignMessage.Body.SetDictionary("dictionary", new Dictionary<string, string>() { { "a", "a" } });
            foreignMessage.Body.SetBytes("bytes", new byte[] { 6 });

            NmsMessage transformed = NmsMessageTransformation.TransformMessage(factory, foreignMessage);

            Assert.IsNotNull(transformed);
            Assert.AreNotSame(foreignMessage, transformed);

            Assert.IsInstanceOf<NmsMapMessage>(transformed);

            NmsMapMessage message = (NmsMapMessage) transformed;

            Assert.IsTrue(message.Body.GetBool("bool"));
            Assert.AreEqual('a', message.Body.GetChar("char"));
            Assert.AreEqual("string", message.Body.GetString("string"));
            Assert.AreEqual(1, message.Body.GetByte("byte"));
            Assert.AreEqual(1, message.Body.GetShort("short"));
            Assert.AreEqual(1, message.Body.GetInt("int"));
            Assert.AreEqual(1, message.Body.GetLong("long"));
            Assert.AreEqual(1.5F, message.Body.GetFloat("float"));
            Assert.AreEqual(1.5F, message.Body.GetDouble("double"));
            CollectionAssert.AreEqual(new List<string>() { "a" }, message.Body.GetList("list"));
            CollectionAssert.AreEqual(new Dictionary<string, string>() { { "a", "a" } }, message.Body.GetDictionary("dictionary"));
            CollectionAssert.AreEqual(new byte[] { 6 }, message.Body.GetBytes("bytes"));
        }

        [Test]
        public void TestEmptyForeignStreamMessageTransformCreateNewMessage()
        {
            ForeignNmsStreamMessage foreignMessage = new ForeignNmsStreamMessage();

            NmsMessage transformed = NmsMessageTransformation.TransformMessage(factory, foreignMessage);

            Assert.IsNotNull(transformed);
            Assert.AreNotSame(foreignMessage, transformed);

            Assert.IsInstanceOf<NmsStreamMessage>(transformed);

            NmsStreamMessage message = (NmsStreamMessage) transformed;
            message.Reset();
            Assert.Catch<MessageEOFException>(() => message.ReadBoolean());
        }

        [Test]
        public void TestForeignStreamMessageTransformCreateNewMessage()
        {
            ForeignNmsStreamMessage foreignMessage = new ForeignNmsStreamMessage();
            foreignMessage.WriteBoolean(true);
            foreignMessage.WriteString("test");
            foreignMessage.WriteBoolean(true);

            NmsMessage transformed = NmsMessageTransformation.TransformMessage(factory, foreignMessage);

            Assert.IsNotNull(transformed);
            Assert.AreNotSame(foreignMessage, transformed);

            Assert.IsInstanceOf<NmsStreamMessage>(transformed);

            NmsStreamMessage message = (NmsStreamMessage) transformed;
            message.Reset();
            Assert.IsTrue(message.ReadBoolean());
            Assert.AreEqual("test", message.ReadString());
            Assert.IsTrue(message.ReadBoolean());
        }

        [Test]
        public void TestEmptyForeignObjectMessageTransformCreateNewMessage()
        {
            ForeignNmsObjectMessage foreignMessage = new ForeignNmsObjectMessage();

            NmsMessage transformed = NmsMessageTransformation.TransformMessage(factory, foreignMessage);

            Assert.AreNotSame(foreignMessage, transformed);

            Assert.IsInstanceOf<NmsObjectMessage>(transformed);
            NmsObjectMessage message = (NmsObjectMessage) transformed;
            Assert.IsNull(message.Body);
        }

        [Test]
        public void TestForeignObjectMessageTransformCreateNewMessage()
        {
            string messageBody = "TEST-MESSAGE-BODY";
            ForeignNmsObjectMessage foreignMessage = new ForeignNmsObjectMessage();
            foreignMessage.Body = messageBody;

            NmsMessage transformed = NmsMessageTransformation.TransformMessage(factory, foreignMessage);

            Assert.AreNotSame(foreignMessage, transformed);

            Assert.IsInstanceOf<NmsObjectMessage>(transformed);
            NmsObjectMessage message = (NmsObjectMessage) transformed;
            Assert.AreEqual(messageBody, message.Body);
        }

        [Test]
        public void TestNMSMessageHeadersAreCopied()
        {
            string destinationName = "Test-Destination-Name";

            NmsMessage source = new NmsMessage(new NmsTestMessageFacade());

            NmsTopic destination = new NmsTopic(destinationName);
            NmsTopic replyTo = new NmsTopic("ReplyTo: " + destinationName);

            source.NMSMessageId = "ID:TEST";
            source.NMSCorrelationID = "ID:CORRELATION";
            source.NMSDestination = destination;
            source.NMSReplyTo = replyTo;
            source.NMSDeliveryMode = MsgDeliveryMode.NonPersistent;
            source.NMSRedelivered = true;
            source.NMSType = "test-type";
            source.NMSPriority = MsgPriority.Highest;
            source.NMSTimestamp = DateTime.UtcNow;

            NmsMessage transformed = NmsMessageTransformation.TransformMessage(factory, source);

            Assert.AreEqual(source.NMSMessageId, transformed.NMSMessageId);
            Assert.AreEqual(source.NMSCorrelationID, transformed.NMSCorrelationID);
            Assert.AreEqual(source.NMSDestination, transformed.NMSDestination);
            Assert.AreEqual(source.NMSReplyTo, transformed.NMSReplyTo);
            Assert.AreEqual(source.NMSDeliveryMode, transformed.NMSDeliveryMode);
            Assert.AreEqual(source.NMSRedelivered, transformed.NMSRedelivered);
            Assert.AreEqual(source.NMSType, transformed.NMSType);
            Assert.AreEqual(source.NMSPriority, transformed.NMSPriority);
            Assert.AreEqual(source.NMSTimestamp, transformed.NMSTimestamp);
        }

        [Test]
        public void TestNMSMessagePropertiesAreCopied()
        {
            ForeignNmsMapMessage foreignMessage = new ForeignNmsMapMessage();

            foreignMessage.Properties.SetBool("bool", true);
            foreignMessage.Properties.SetChar("char", 'a');
            foreignMessage.Properties.SetString("string", "string");
            foreignMessage.Properties.SetByte("byte", 1);
            foreignMessage.Properties.SetShort("short", 1);
            foreignMessage.Properties.SetInt("int", 1);
            foreignMessage.Properties.SetLong("long", 1);
            foreignMessage.Properties.SetFloat("float", 1.5F);
            foreignMessage.Properties.SetDouble("double", 1.5D);
            foreignMessage.Properties.SetList("list", new List<string>() { "a" });
            foreignMessage.Properties.SetDictionary("dictionary", new Dictionary<string, string>() { { "a", "a" } });
            foreignMessage.Properties.SetBytes("bytes", new byte[] { 6 });
            
            NmsMessage transformed = NmsMessageTransformation.TransformMessage(factory, foreignMessage);
            
            Assert.IsTrue(transformed.Properties.GetBool("bool"));
            Assert.AreEqual('a', transformed.Properties.GetChar("char"));
            Assert.AreEqual("string", transformed.Properties.GetString("string"));
            Assert.AreEqual(1, transformed.Properties.GetByte("byte"));
            Assert.AreEqual(1, transformed.Properties.GetShort("short"));
            Assert.AreEqual(1, transformed.Properties.GetInt("int"));
            Assert.AreEqual(1, transformed.Properties.GetLong("long"));
            Assert.AreEqual(1.5F, transformed.Properties.GetFloat("float"));
            Assert.AreEqual(1.5F, transformed.Properties.GetDouble("double"));
            CollectionAssert.AreEqual(new List<string>() { "a" }, transformed.Properties.GetList("list"));
            CollectionAssert.AreEqual(new Dictionary<string, string>() { { "a", "a" } }, transformed.Properties.GetDictionary("dictionary"));
            CollectionAssert.AreEqual(new byte[] { 6 }, transformed.Properties.GetBytes("bytes"));
        }
    }
}