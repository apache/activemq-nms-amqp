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
using Apache.NMS;
using Apache.NMS.AMQP;
using Apache.NMS.AMQP.Message;
using NMS.AMQP.Test.Message.Facade;
using NUnit.Framework;

namespace NMS.AMQP.Test.Message
{
    [TestFixture]
    public class NmsMessageTest
    {
        private string nmsMessageId;
        private string nmsCorrelationId;
        private DateTime nmsTimestamp;
        private IDestination nmsDestination;
        private IDestination nmsReplyTo;
        private MsgDeliveryMode nmsDeliveryMode;
        private bool nmsRedelivered;
        private string nmsType;

        private readonly INmsMessageFactory factory = new TestMessageFactory();

        [SetUp]
        public void SetUp()
        {
            nmsMessageId = "ID:TEST-ID:0:0:0:1";
            nmsTimestamp = DateTime.UtcNow;
            nmsCorrelationId = "testCorrelationId";
            nmsDestination = new NmsTopic("test.topic");
            nmsReplyTo = new NmsTopic("test.replyto.topic:001");
            nmsDeliveryMode = MsgDeliveryMode.Persistent;
            nmsRedelivered = true;
            nmsType = "test type";
        }

        [Test]
        public void TestMessageSetToReadOnlyOnSend()
        {
            NmsMessage message = factory.CreateMessage();

            Assert.False(message.IsReadOnly);
            Assert.False(message.IsReadOnlyProperties);
            message.OnSend(TimeSpan.Zero);
            Assert.True(message.IsReadOnly);
        }

        [Test]
        public void TestMessageSetToReadOnlyOnDispatch()
        {
            NmsMessage message = factory.CreateMessage();

            Assert.False(message.IsReadOnlyBody);
            Assert.False(message.IsReadOnlyProperties);
            message.OnDispatch();
            Assert.True(message.IsReadOnlyBody);
            Assert.True(message.IsReadOnlyProperties);
        }

        [Test]
        public void TestToString()
        {
            NmsMessage message = factory.CreateMessage();
            Assert.True(message.ToString().StartsWith("NmsMessage"));
        }

        [Test]
        public void TestHashCode()
        {
            NmsMessage message = factory.CreateMessage();
            message.NMSMessageId = nmsMessageId;
            Assert.AreEqual(message.NMSMessageId.GetHashCode(), nmsMessageId.GetHashCode());
            Assert.AreEqual(message.GetHashCode(), nmsMessageId.GetHashCode());
        }

        [Test]
        public void TestHashCodeWhenNoMessageIdAssigned()
        {
            NmsMessage message1 = factory.CreateMessage();
            NmsMessage message2 = factory.CreateMessage();

            Assert.AreNotEqual(message1.GetHashCode(), message2.GetHashCode());
            Assert.AreEqual(message1.GetHashCode(), message1.GetHashCode());
        }

        [Test]
        public void TestSetReadOnly()
        {
            NmsMessage message = factory.CreateMessage();
            message.IsReadOnlyProperties = true;
            Assert.Throws<MessageNotWriteableException>(() => message.Properties.SetInt("test", 1));
        }

        [Test]
        public void TestSetToForeignNMSID()
        {
            NmsMessage message = factory.CreateMessage();
            message.NMSMessageId = "ID:EMS-SERVER.8B443C380083:429";
        }

        [Test]
        public void TestEqualsObject()
        {
            NmsMessage msg1 = factory.CreateMessage();
            NmsMessage msg2 = factory.CreateMessage();

            msg1.NMSMessageId = nmsMessageId;
            Assert.False(msg1.Equals(msg2));
            Assert.False(msg2.Equals(msg1));

            msg2.NMSMessageId = nmsMessageId;
            Assert.True(msg1.Equals(msg2));
            Assert.True(msg2.Equals(msg1));

            msg2.NMSMessageId = nmsMessageId + "More";
            Assert.False(msg1.Equals(msg2));
            Assert.False(msg2.Equals(msg1));

            Assert.True(msg1.Equals(msg1));
            Assert.False(msg1.Equals(null));
            Assert.False(msg1.Equals(""));
        }

        [Test]
        public void TestEqualsObjectNullMessageIds()
        {
            NmsMessage msg1 = factory.CreateMessage();
            NmsMessage msg2 = factory.CreateMessage();
            Assert.False(msg1.Equals(msg2));
            Assert.False(msg2.Equals(msg1));
        }

        [Test]
        public void TestGetAndSetNMSMessageId()
        {
            NmsMessage msg1 = factory.CreateMessage();
            Assert.IsNull(msg1.NMSMessageId);
            msg1.NMSMessageId = nmsMessageId;
            Assert.AreEqual(msg1.NMSMessageId, nmsMessageId);
        }

        [Test]
        public void TestGetAndSetNMSTimestamp()
        {
            NmsMessage msg = factory.CreateMessage();
            msg.NMSTimestamp = nmsTimestamp;
            Assert.AreEqual(msg.NMSTimestamp, nmsTimestamp);
        }

        [Test]
        public void TestGetAndSetNMSCorrelationId()
        {
            NmsMessage msg = factory.CreateMessage();
            msg.NMSCorrelationID = nmsCorrelationId;
            Assert.AreEqual(msg.NMSCorrelationID, nmsCorrelationId);
        }

        [Test]
        public void TestGetAndSetNMSReplyTo()
        {
            NmsMessage msg = factory.CreateMessage();
            msg.NMSReplyTo = nmsReplyTo;
            Assert.AreEqual(nmsReplyTo, msg.NMSReplyTo);
        }

        [Test]
        public void TestGetAndSetNmsDestination()
        {
            NmsMessage msg = factory.CreateMessage();
            msg.NMSDestination = nmsDestination;
            Assert.AreEqual(nmsDestination, msg.NMSDestination);
        }

        [Test]
        public void TestGetAndSetNMSDeliveryMode()
        {
            NmsMessage msg = factory.CreateMessage();
            msg.NMSDeliveryMode = MsgDeliveryMode.Persistent;
            Assert.AreEqual(nmsDeliveryMode, msg.NMSDeliveryMode);
            msg.NMSDeliveryMode = MsgDeliveryMode.NonPersistent;
            Assert.AreEqual(MsgDeliveryMode.NonPersistent, msg.NMSDeliveryMode);
        }

        [Test]
        public void TestGetAndSetNMSRedelivered()
        {
            NmsMessage msg = factory.CreateMessage();
            msg.NMSRedelivered = nmsRedelivered;
            Assert.AreEqual(nmsRedelivered, msg.NMSRedelivered);
        }

        [Test]
        public void TestGetAndSetNMSType()
        {
            NmsMessage msg = factory.CreateMessage();
            msg.NMSType = nmsType;
            Assert.AreEqual(nmsType, msg.NMSType);
        }

        [Test]
        public void TestGetAndSetNMSPriority()
        {
            NmsMessage msg = factory.CreateMessage();
            msg.NMSPriority = MsgPriority.Normal;
            Assert.AreEqual(MsgPriority.Normal, msg.NMSPriority);
        }

        [Test]
        public void TestClearProperties()
        {
            NmsMessage msg = factory.CreateMessage();
            msg.Properties.SetString("test", "test");
            msg.NMSMessageId = nmsMessageId;
            msg.ClearProperties();
            Assert.Null(msg.Properties.GetString("test"));
            Assert.NotNull(msg.NMSMessageId);
        }

        [Test]
        public void TestClearPropertiesClearsReadOnly()
        {
            NmsMessage msg = factory.CreateMessage();
            msg.OnDispatch();

            Assert.Throws<MessageNotWriteableException>(() => msg.Properties.SetString("test", "test"));

            Assert.True(msg.IsReadOnlyProperties);

            msg.ClearProperties();

            msg.Properties.SetString("test", "test");

            Assert.False(msg.IsReadOnlyProperties);
        }

        [Test]
        public void TestPropertyExists()
        {
            NmsMessage msg = factory.CreateMessage();
            msg.Properties.SetString("test", "test");
            Assert.True(msg.Properties.Contains("test"));

            msg.Properties.SetInt("NMSXDeliveryCount", 1);
            Assert.True(msg.Properties.Contains("NMSXDeliveryCount"));
        }

        [Test]
        public void TestGetBooleanProperty()
        {
            NmsMessage msg = factory.CreateMessage();
            string name = "booleanProperty";
            msg.Properties.SetBool(name, true);
            Assert.True(msg.Properties.GetBool(name));
        }

        [Test]
        public void TestGetByteProperty()
        {
            NmsMessage msg = factory.CreateMessage();
            string name = "byteProperty";
            msg.Properties.SetByte(name, 1);
            Assert.AreEqual(1, msg.Properties.GetByte(name));
        }

        [Test]
        public void TestGetIntProperty()
        {
            NmsMessage msg = factory.CreateMessage();
            string name = "intProperty";
            msg.Properties.SetInt(name, 1);
            Assert.AreEqual(1, msg.Properties.GetInt(name));
        }

        [Test]
        public void TestGetLongProperty()
        {
            NmsMessage msg = factory.CreateMessage();
            String name = "longProperty";
            msg.Properties.SetLong(name, 1);
            Assert.AreEqual(1, msg.Properties.GetLong(name));
        }

        [Test]
        public void TestGetFloatProperty()
        {
            NmsMessage msg = factory.CreateMessage();
            String name = "floatProperty";
            msg.Properties.SetFloat(name, 1.3f);
            Assert.AreEqual(1.3f, msg.Properties.GetFloat(name));
        }

        [Test]
        public void TestGetDoubleProperty()
        {
            NmsMessage msg = factory.CreateMessage();
            String name = "doubleProperty";
            msg.Properties.SetDouble(name, 1.3f);
            Assert.AreEqual(1.3f, msg.Properties.GetDouble(name));
        }

        [Test]
        public void TestGetStringProperty()
        {
            NmsMessage msg = factory.CreateMessage();
            String name = "stringProperty";
            msg.Properties.SetString(name, "test");
            Assert.AreEqual("test", msg.Properties.GetString(name));
        }

        [Test]
        public void TestSetNullProperty()
        {
            NmsMessage msg = factory.CreateMessage();
            String name = "cheese";
            msg.Properties.SetString(name, "Cheddar");
            Assert.AreEqual("Cheddar", msg.Properties.GetString(name));

            msg.Properties.SetString(name, null);
            Assert.IsNull(msg.Properties.GetString(name));
        }

        [Test]
        public void TestSetNullPropertyName()
        {
            NmsMessage msg = factory.CreateMessage();

            Assert.Throws<ArgumentNullException>(() => msg.Properties.SetString(null, "asd"));
        }

        [Test]
        public void TestSetEmptyPropertyName()
        {
            NmsMessage msg = factory.CreateMessage();

            Assert.Throws<ArgumentNullException>(() => msg.Properties.SetString(null, "asd"));
        }

        [Test]
        public void TestClearBody()
        {
            NmsMessage msg = factory.CreateMessage();
            msg.ClearBody();
            Assert.False(msg.IsReadOnlyBody);
        }

        [Test]
        public void TestBooleanPropertyConversion()
        {
            NmsMessage msg = factory.CreateMessage();
            String name = "property";
            msg.Properties.SetBool(name, true);

            Assert.IsTrue(msg.Properties.GetBool(name));
            Assert.AreEqual(bool.TrueString, msg.Properties.GetString(name));

            Assert.Throws<MessageFormatException>(() => msg.Properties.GetByte(name));
            Assert.Throws<MessageFormatException>(() => msg.Properties.GetShort(name));
            Assert.Throws<MessageFormatException>(() => msg.Properties.GetInt(name));
            Assert.Throws<MessageFormatException>(() => msg.Properties.GetLong(name));
            Assert.Throws<MessageFormatException>(() => msg.Properties.GetFloat(name));
            Assert.Throws<MessageFormatException>(() => msg.Properties.GetDouble(name));
        }

        [Test]
        public void TestBytePropertyConversion()
        {
            NmsMessage msg = factory.CreateMessage();
            String name = "property";
            msg.Properties.SetByte(name, 1);
            
            Assert.AreEqual(1, msg.Properties.GetByte(name));
            Assert.AreEqual(1, msg.Properties.GetShort(name));
            Assert.AreEqual(1, msg.Properties.GetInt(name));
            Assert.AreEqual(1, msg.Properties.GetLong(name));
            Assert.AreEqual("1", msg.Properties.GetString(name));
            
            Assert.Throws<MessageFormatException>(() => msg.Properties.GetBool(name));
            Assert.Throws<MessageFormatException>(() => msg.Properties.GetFloat(name));
            Assert.Throws<MessageFormatException>(() => msg.Properties.GetDouble(name));
        }

        [Test]
        public void TestSetAndGetGroupId()
        {
            NmsMessage msg = factory.CreateMessage();
            
            msg.Properties.SetString("NMSXGroupId", "testGroupId");
            
            Assert.AreEqual(msg.Facade.GroupId, "testGroupId");
        }
        
        [Test]
        public void TestSetAndGetGroupSequence()
        {
            NmsMessage msg = factory.CreateMessage();
            
            msg.Properties.SetInt("NMSXGroupSeq", 10);            
            
            Assert.AreEqual(msg.Facade.GroupSequence, 10);
        }
        
        // TODO: Test conversion for other properties

        [Test]
        public void TestReadOnlyProperties()
        {
            NmsMessage msg = factory.CreateMessage();
            msg.IsReadOnlyProperties = true;
            String name = "property";

            Assert.Throws<MessageNotWriteableException>(() => msg.Properties.SetString(name, "test"));
            Assert.Throws<MessageNotWriteableException>(() => msg.Properties.SetBool(name, true));
            Assert.Throws<MessageNotWriteableException>(() => msg.Properties.SetByte(name, 1));
            Assert.Throws<MessageNotWriteableException>(() => msg.Properties.SetShort(name, 1));
            Assert.Throws<MessageNotWriteableException>(() => msg.Properties.SetInt(name, 1));
            Assert.Throws<MessageNotWriteableException>(() => msg.Properties.SetLong(name, 1));
            Assert.Throws<MessageNotWriteableException>(() => msg.Properties.SetFloat(name, 1));
            Assert.Throws<MessageNotWriteableException>(() => msg.Properties.SetDouble(name, 1));
        }

        [Test]
        public void TestAcknowledgeWithNoCallbackDoesNotThrow()
        {
            NmsMessage msg = factory.CreateMessage();
            msg.Acknowledge();
        }

        [Test]
        public void TestCopyMessage()
        {
            NmsMessage msg = factory.CreateMessage();
            msg.IsReadOnly = true;
            msg.IsReadOnlyProperties = true;
            msg.IsReadOnlyBody = true;
            msg.NmsAcknowledgeCallback = new NmsAcknowledgeCallback(null);
            
            NmsMessage copy = msg.Copy();
            
            Assert.AreEqual(copy.IsReadOnly, copy.IsReadOnly);
            Assert.AreEqual(copy.IsReadOnlyProperties, copy.IsReadOnlyProperties);
            Assert.AreEqual(copy.IsReadOnlyBody, copy.IsReadOnlyBody);
            Assert.AreEqual(copy.NmsAcknowledgeCallback, copy.NmsAcknowledgeCallback);
        }
    }
}