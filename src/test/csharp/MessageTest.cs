/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor licensete agreements.  See the NOTICE file distributed with
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

using NUnit.Framework;
using System;
using System.Text;
using System.Collections;
using Apache.NMS.Util;
using Apache.NMS.Amqp;

namespace Apache.NMS.Amqp.Test.Commands
{
    [TestFixture]
    public class MessageTest
    {
        private string nmsMessageID;
        private string nmsCorrelationID;
        private Topic nmsDestination;
        private Topic nmsReplyTo;
        private MsgDeliveryMode nmsDeliveryMode;
        private bool nmsRedelivered;
        private string nmsType;
        private MsgPriority nmsPriority;
        private DateTime nmsTimestamp;
        private long[] consumerIDs;

        [SetUp]
        public virtual void SetUp()
        {
            this.nmsMessageID = "testid";
            this.nmsCorrelationID = "testcorrelationid";
            this.nmsDestination = new Topic("TEST.Message");
			this.nmsReplyTo = new Topic("TEST.Message.replyto.topic:001");
            this.nmsDeliveryMode = MsgDeliveryMode.NonPersistent;
            this.nmsRedelivered = true;
            this.nmsType = "test type";
            this.nmsPriority = MsgPriority.High;
            this.nmsTimestamp = DateTime.Now;
            this.consumerIDs = new long[3];

            for(int i = 0; i < this.consumerIDs.Length; i++)
            {
                this.consumerIDs[i] = i;
            }
        }

        [Test]
        public void TestSetToForeignNMSID()
        {
            TextMessage msg = new TextMessage();
            msg.NMSMessageId = "SomeString";
        }

        [Test]
        public void TestShallowCopy()
        {
            TextMessage msg1 = new TextMessage();
            msg1.NMSMessageId = nmsMessageID;
            TextMessage msg2 = (TextMessage)msg1.Clone();
            Assert.IsTrue(msg1 != msg2);
            Assert.IsTrue(msg1.Equals(msg2));
            msg2.NMSPriority = MsgPriority.Highest;
            Assert.IsFalse(msg1.Equals(msg2));
        }

        [Test]
        public void TestCopy()
        {
            this.nmsMessageID = "ID:1141:45278:429";
            this.nmsCorrelationID = "testcorrelationid";
            this.nmsDestination = new Topic("test.topic");
            this.nmsReplyTo = new Topic("test.replyto.topic:001");
            this.nmsDeliveryMode = MsgDeliveryMode.NonPersistent;
            this.nmsType = "test type";
            this.nmsPriority = MsgPriority.High;
            this.nmsTimestamp = DateTime.Now;

            TextMessage msg1 = new TextMessage();
            msg1.NMSMessageId = this.nmsMessageID;
            msg1.NMSCorrelationID = this.nmsCorrelationID;
            msg1.NMSDestination = this.nmsDestination;
            msg1.NMSReplyTo = this.nmsReplyTo;
            msg1.NMSDeliveryMode = this.nmsDeliveryMode;
            msg1.NMSType = this.nmsType;
            msg1.NMSPriority = this.nmsPriority;
            msg1.NMSTimestamp = this.nmsTimestamp;

            TextMessage msg2 = msg1;

            Assert.IsTrue(msg1.NMSMessageId.Equals(msg2.NMSMessageId));
            Assert.IsTrue(msg1.NMSCorrelationID.Equals(msg2.NMSCorrelationID));
            Assert.IsTrue(msg1.NMSDestination.Equals(msg2.NMSDestination));
            Assert.IsTrue(msg1.NMSReplyTo.Equals(msg2.NMSReplyTo));
            Assert.IsTrue(msg1.NMSDeliveryMode == msg2.NMSDeliveryMode);
            Assert.IsTrue(msg1.NMSRedelivered == msg2.NMSRedelivered);
            Assert.IsTrue(msg1.NMSType.Equals(msg2.NMSType));
            Assert.IsTrue(msg1.NMSPriority == msg2.NMSPriority);
            Assert.IsTrue(msg1.NMSTimestamp == msg2.NMSTimestamp);
        }

        [Test]
        public void TestGetAndSetNMSCorrelationID()
        {
            TextMessage msg = new TextMessage();
            msg.NMSCorrelationID = this.nmsCorrelationID;
            Assert.IsTrue(msg.NMSCorrelationID.Equals(this.nmsCorrelationID));
        }

        [Test]
        public void TestGetAndSetNMSReplyTo()
        {
            TextMessage msg = new TextMessage();
            msg.NMSReplyTo = this.nmsReplyTo;
            Assert.AreEqual(msg.NMSReplyTo, this.nmsReplyTo);
        }

        [Test]
        public void TestGetAndSetNMSDeliveryMode()
        {
            TextMessage msg = new TextMessage();
            msg.NMSDeliveryMode = this.nmsDeliveryMode;
            Assert.IsTrue(msg.NMSDeliveryMode == this.nmsDeliveryMode);
        }

        [Test]
        public void TestGetAndSetNMSType()
        {
            TextMessage msg = new TextMessage();
            msg.NMSType = this.nmsType;
            Assert.AreEqual(msg.NMSType, this.nmsType);
        }

        [Test]
        public void TestGetAndSetNMSPriority()
        {
            TextMessage msg = new TextMessage();
            msg.NMSPriority = this.nmsPriority;
            Assert.IsTrue(msg.NMSPriority == this.nmsPriority);
        }

        public void TestClearProperties()
        {
            TextMessage msg = new TextMessage();
            msg.Properties.SetString("test", "test");
            msg.Content = new byte[1];
            msg.NMSMessageId = this.nmsMessageID;
            msg.ClearProperties();
            Assert.IsNull(msg.Properties.GetString("test"));
            Assert.IsNotNull(msg.NMSMessageId);
            Assert.IsNotNull(msg.Content);
        }

        [Test]
        public void TestPropertyExists()
        {
            TextMessage msg = new TextMessage();
            msg.Properties.SetString("test", "test");
            Assert.IsTrue(msg.Properties.Contains("test"));
        }

        [Test]
        public void TestGetBooleanProperty()
        {
            TextMessage msg = new TextMessage();
            string name = "booleanProperty";
            msg.Properties.SetBool(name, true);
            Assert.IsTrue(msg.Properties.GetBool(name));
        }

        [Test]
        public void TestGetByteProperty()
        {
            TextMessage msg = new TextMessage();
            string name = "byteProperty";
            msg.Properties.SetByte(name, (byte)1);
            Assert.IsTrue(msg.Properties.GetByte(name) == 1);
        }

        [Test]
        public void TestGetShortProperty()
        {
            TextMessage msg = new TextMessage();
            string name = "shortProperty";
            msg.Properties.SetShort(name, (short)1);
            Assert.IsTrue(msg.Properties.GetShort(name) == 1);
        }

        [Test]
        public void TestGetIntProperty()
        {
            TextMessage msg = new TextMessage();
            string name = "intProperty";
            msg.Properties.SetInt(name, 1);
            Assert.IsTrue(msg.Properties.GetInt(name) == 1);
        }

        [Test]
        public void TestGetLongProperty()
        {
            TextMessage msg = new TextMessage();
            string name = "longProperty";
            msg.Properties.SetLong(name, 1);
            Assert.IsTrue(msg.Properties.GetLong(name) == 1);
        }

        [Test]
        public void TestGetFloatProperty()
        {
            TextMessage msg = new TextMessage();
            string name = "floatProperty";
            msg.Properties.SetFloat(name, 1.3f);
            Assert.IsTrue(msg.Properties.GetFloat(name) == 1.3f);
        }

        [Test]
        public void TestGetDoubleProperty()
        {
            TextMessage msg = new TextMessage();
            string name = "doubleProperty";
            msg.Properties.SetDouble(name, 1.3d);
            Assert.IsTrue(msg.Properties.GetDouble(name) == 1.3);
        }

        [Test]
        public void TestGetStringProperty()
        {
            TextMessage msg = new TextMessage();
            string name = "stringProperty";
            msg.Properties.SetString(name, name);
            Assert.IsTrue(msg.Properties.GetString(name).Equals(name));
        }

        [Test]
        public void TestGetObjectProperty()
        {
            TextMessage msg = new TextMessage();
            string name = "floatProperty";
            msg.Properties.SetFloat(name, 1.3f);
            Assert.IsTrue(msg.Properties[name] is float);
            Assert.IsTrue((float)msg.Properties[name] == 1.3f);
        }

        [Test]
        public void TestGetPropertyNames()
        {
            TextMessage msg = new TextMessage();
            string name = "floatProperty";
            msg.Properties.SetFloat(name, 1.3f);

            foreach(string key in msg.Properties.Keys)
            {
                Assert.IsTrue(key.Equals(name));
            }
        }

        [Test]
        public void TestSetObjectProperty()
        {
            TextMessage msg = new TextMessage();
            string name = "property";

            try
            {
                msg.Properties[name] = "string";
                msg.Properties[name] = (Char) 1;
                msg.Properties[name] = (Int16) 1;
                msg.Properties[name] = (Int32) 1;
                msg.Properties[name] = (Int64) 1;
                msg.Properties[name] = (Byte)1;
                msg.Properties[name] = (UInt16)1;
                msg.Properties[name] = (UInt32)1;
                msg.Properties[name] = (UInt64)1;
                msg.Properties[name] = (Single)1.1f;
                msg.Properties[name] = (Double) 1.1;
                msg.Properties[name] = (Boolean) true;
                msg.Properties[name] = null;
            }
            catch(NMSException)
            {
                Assert.Fail("should accept object primitives and String");
            }

            try
            {
                msg.Properties[name] = new Object();
                Assert.Fail("should accept only object primitives and String");
            }
            catch(NMSException)
            {
            }

            try
            {
                msg.Properties[name] = new StringBuilder();
                Assert.Fail("should accept only object primitives and String");
            }
            catch(NMSException)
            {
            }
        }

        [Test]
        public void TestConvertProperties()
        {
            TextMessage msg = new TextMessage();

            // Set/verify a property using each supported AMQP data type

            msg.Properties["stringProperty"] = "string";
            msg.Properties["booleanProperty"] = (Boolean)true;

            msg.Properties["charProperty"] = (Char)'h';
            msg.Properties["shortProperty"] = (Int16) 2;
            msg.Properties["intProperty"] = (Int32) 3;
            msg.Properties["longProperty"] = (Int64) 4;

            msg.Properties["byteProperty"] = (Byte)5;
            msg.Properties["ushortProperty"] = (UInt16)6;
            msg.Properties["uintProperty"] = (UInt32)7;
            msg.Properties["ulongProperty"] = (UInt64)8;

            msg.Properties["floatProperty"] = (Single)9.9f;
            msg.Properties["doubleProperty"] = (Double) 10.1;
            msg.Properties["nullProperty"] = null;
            msg.Properties["guidProperty"] = new Guid("000102030405060708090a0b0c0d0e0f");

            IPrimitiveMap properties = msg.Properties;
            Assert.AreEqual(properties["stringProperty"], "string");
            Assert.AreEqual(properties["booleanProperty"], true);

            Assert.AreEqual((Char)properties["charProperty"], (Char)'h');
            Assert.AreEqual(properties["shortProperty"], (short) 2);
            Assert.AreEqual(properties["intProperty"], (int) 3);
            Assert.AreEqual(properties["longProperty"], (long) 4);

            Assert.AreEqual(properties["byteProperty"], (byte)5);
            Assert.AreEqual(properties["ushortProperty"], (UInt16)6);
            Assert.AreEqual(properties["uintProperty"], (int)7);
            Assert.AreEqual(properties["ulongProperty"], (long)8);
            
            Assert.AreEqual(properties["floatProperty"], 9.9f);
            Assert.AreEqual(properties["doubleProperty"], 10.1);
            Assert.IsNull(properties["nullProperty"]);
            Guid rxGuid = (Guid)properties["guidProperty"];
            Assert.AreEqual(rxGuid.ToString(), "00010203-0405-0607-0809-0a0b0c0d0e0f");
        }

        [Test]
        public void TestSetNullProperty()
        {
            TextMessage msg = new TextMessage();
            string name = "cheese";
            msg.Properties.SetString(name, "Cheddar");
            Assert.AreEqual("Cheddar", msg.Properties.GetString(name));

            msg.Properties.SetString(name, null);
            Assert.AreEqual(null, msg.Properties.GetString(name));
        }

        [Test]
        public void TestSetNullPropertyName()
        {
            TextMessage msg = new TextMessage();

            try
            {
                msg.Properties.SetString(null, "Cheese");
                Assert.Fail("Should have thrown exception");
            }
            catch(Exception)
            {
            }
        }

        [Test]
        public void TestSetEmptyPropertyName()
        {
            TextMessage msg = new TextMessage();

            try
            {
                msg.Properties.SetString("", "Cheese");
                Assert.Fail("Should have thrown exception");
            }
            catch(Exception)
            {
            }
        }

        [Test]
        public void TestGetAndSetNMSXDeliveryCount()
        {
            TextMessage msg = new TextMessage();
            msg.Properties.SetInt("NMSXDeliveryCount", 1);
            int count = msg.Properties.GetInt("NMSXDeliveryCount");
            Assert.IsTrue(count == 1, "expected delivery count = 1 - got: " + count);
        }

        [Test]
        public void TestClearBody()
        {
            BytesMessage message = new BytesMessage();
            message.ClearBody();
            Assert.IsFalse(message.ReadOnlyBody);
        }

        //
        // Helper functions for TestPropertyConversionXxx tests.
        // Demonstrate properties are inaccessible using various methods.
        //   Get the named property from the map.
        //   Assert that the Get function throws; display message on failure.
        //
        public void TestGetBoolThrows(IPrimitiveMap map, string name, string message)
        {
            try
            {
                map.GetBool(name);
                Assert.Fail(message);
            }
            catch (NMSException)
            {
            }
        }

        public void TestGetByteThrows(IPrimitiveMap map, string name, string message)
        {
            try
            {
                map.GetByte(name);
                Assert.Fail(message);
            }
            catch (NMSException)
            {
            }
        }

        public void TestGetBytesThrows(IPrimitiveMap map, string name, string message)
        {
            try
            {
                map.GetBytes(name);
                Assert.Fail(message);
            }
            catch (NMSException)
            {
            }
        }

        public void TestGetCharThrows(IPrimitiveMap map, string name, string message)
        {
            try
            {
                map.GetChar(name);
                Assert.Fail(message);
            }
            catch (NMSException)
            {
            }
        }

        public void TestGetDictionaryThrows(IPrimitiveMap map, string name, string message)
        {
            try
            {
                map.GetDictionary(name);
                Assert.Fail(message);
            }
            catch (NMSException)
            {
            }
        }

        public void TestGetDoubleThrows(IPrimitiveMap map, string name, string message)
        {
            try
            {
                map.GetDouble(name);
                Assert.Fail(message);
            }
            catch (NMSException)
            {
            }
        }

        public void TestGetFloatThrows(IPrimitiveMap map, string name, string message)
        {
            try
            {
                map.GetFloat(name);
                Assert.Fail(message);
            }
            catch (NMSException)
            {
            }
        }

        public void TestGetIntThrows(IPrimitiveMap map, string name, string message)
        {
            try
            {
                map.GetInt(name);
                Assert.Fail(message);
            }
            catch (NMSException)
            {
            }
        }

        public void TestGetListThrows(IPrimitiveMap map, string name, string message)
        {
            try
            {
                map.GetList(name);
                Assert.Fail(message);
            }
            catch (NMSException)
            {
            }
        }

        public void TestGetLongThrows(IPrimitiveMap map, string name, string message)
        {
            try
            {
                map.GetLong(name);
                Assert.Fail(message);
            }
            catch (NMSException)
            {
            }
        }

        public void TestGetShortThrows(IPrimitiveMap map, string name, string message)
        {
            try
            {
                map.GetShort(name);
                Assert.Fail(message);
            }
            catch (NMSException)
            {
            }
        }

        public void TestGetStringThrows(IPrimitiveMap map, string name, string message)
        {
            try
            {
                map.GetString(name);
                Assert.Fail(message);
            }
            catch (NMSException)
            {
            }
        }

        [Test]
        public void TestPropertyConversionBoolean()
        {
            TextMessage msg = new TextMessage();
            String propertyName = "property";
            msg.Properties.SetBool(propertyName, true);

            Assert.AreEqual(msg.Properties[propertyName], true);
            Assert.IsTrue(msg.Properties.GetBool(propertyName));
            Assert.AreEqual(msg.Properties[propertyName].ToString(), "True");

            //TestGetBoolThrows(msg.Properties, propertyName, "GetBool should have thrown");
            TestGetByteThrows(msg.Properties, propertyName, "GetByte should have thrown");
            TestGetBytesThrows(msg.Properties, propertyName, "GetBytes should have thrown");
            TestGetCharThrows(msg.Properties, propertyName, "GetChar should have thrown");
            TestGetDictionaryThrows(msg.Properties, propertyName, "GetDictionary should have thrown");
            TestGetDoubleThrows(msg.Properties, propertyName, "GetDouble should have thrown");
            TestGetFloatThrows(msg.Properties, propertyName, "GetFloat should have thrown");
            TestGetIntThrows(msg.Properties, propertyName, "GetInt should have thrown");
            TestGetListThrows(msg.Properties, propertyName, "GetList should have thrown");
            TestGetLongThrows(msg.Properties, propertyName, "GetLong should have thrown");
            TestGetShortThrows(msg.Properties, propertyName, "GetShort should have thrown");
            TestGetStringThrows(msg.Properties, propertyName, "GetString should have thrown");
        }


        [Test]
        public void TestPropertyConversionByte()
        {
            TextMessage msg = new TextMessage();
            String propertyName = "property";
            msg.Properties.SetByte(propertyName, (byte)1);

            Assert.AreEqual(msg.Properties[propertyName], 1);
            Assert.AreEqual(msg.Properties.GetByte(propertyName), 1);
            Assert.AreEqual(msg.Properties[propertyName].ToString(), "1");

            TestGetBoolThrows(msg.Properties, propertyName, "GetBool should have thrown");
            //TestGetByteThrows(msg.Properties, propertyName, "GetByte should have thrown");
            TestGetBytesThrows(msg.Properties, propertyName, "GetBytes should have thrown");
            TestGetCharThrows(msg.Properties, propertyName, "GetChar should have thrown");
            TestGetDictionaryThrows(msg.Properties, propertyName, "GetDictionary should have thrown");
            TestGetDoubleThrows(msg.Properties, propertyName, "GetDouble should have thrown");
            TestGetFloatThrows(msg.Properties, propertyName, "GetFloat should have thrown");
            TestGetIntThrows(msg.Properties, propertyName, "GetInt should have thrown");
            TestGetListThrows(msg.Properties, propertyName, "GetList should have thrown");
            TestGetLongThrows(msg.Properties, propertyName, "GetLong should have thrown");
            TestGetShortThrows(msg.Properties, propertyName, "GetShort should have thrown");
            TestGetStringThrows(msg.Properties, propertyName, "GetString should have thrown");
        }

        [Test]
        public void TestPropertyConversionShort()
        {
            TextMessage msg = new TextMessage();
            String propertyName = "property";
            msg.Properties.SetShort(propertyName, (short)1);

            Assert.AreEqual((short)msg.Properties[propertyName], 1);
            Assert.AreEqual(msg.Properties.GetShort(propertyName), 1);
            Assert.AreEqual(msg.Properties[propertyName].ToString(), "1");

            TestGetBoolThrows(msg.Properties, propertyName, "GetBool should have thrown");
            TestGetByteThrows(msg.Properties, propertyName, "GetByte should have thrown");
            TestGetBytesThrows(msg.Properties, propertyName, "GetBytes should have thrown");
            TestGetCharThrows(msg.Properties, propertyName, "GetChar should have thrown");
            TestGetDictionaryThrows(msg.Properties, propertyName, "GetDictionary should have thrown");
            TestGetDoubleThrows(msg.Properties, propertyName, "GetDouble should have thrown");
            TestGetFloatThrows(msg.Properties, propertyName, "GetFloat should have thrown");
            TestGetIntThrows(msg.Properties, propertyName, "GetInt should have thrown");
            TestGetListThrows(msg.Properties, propertyName, "GetList should have thrown");
            TestGetLongThrows(msg.Properties, propertyName, "GetLong should have thrown");
            //TestGetShortThrows(msg.Properties, propertyName, "GetShort should have thrown");
            TestGetStringThrows(msg.Properties, propertyName, "GetString should have thrown");
        }

        [Test]
        public void TestPropertyConversionInt()
        {
            TextMessage msg = new TextMessage();
            String propertyName = "property";
            msg.Properties.SetInt(propertyName, (int)1);

            Assert.AreEqual((int)msg.Properties[propertyName], 1);
            Assert.AreEqual(msg.Properties.GetInt(propertyName), 1);
            Assert.AreEqual(msg.Properties[propertyName].ToString(), "1");

            TestGetBoolThrows(msg.Properties, propertyName, "GetBool should have thrown");
            TestGetByteThrows(msg.Properties, propertyName, "GetByte should have thrown");
            TestGetBytesThrows(msg.Properties, propertyName, "GetBytes should have thrown");
            TestGetCharThrows(msg.Properties, propertyName, "GetChar should have thrown");
            TestGetDictionaryThrows(msg.Properties, propertyName, "GetDictionary should have thrown");
            TestGetDoubleThrows(msg.Properties, propertyName, "GetDouble should have thrown");
            TestGetFloatThrows(msg.Properties, propertyName, "GetFloat should have thrown");
            //TestGetIntThrows(msg.Properties, propertyName, "GetInt should have thrown");
            TestGetListThrows(msg.Properties, propertyName, "GetList should have thrown");
            TestGetLongThrows(msg.Properties, propertyName, "GetLong should have thrown");
            TestGetShortThrows(msg.Properties, propertyName, "GetShort should have thrown");
            TestGetStringThrows(msg.Properties, propertyName, "GetString should have thrown");
        }

        [Test]
        public void TestPropertyConversionLong()
        {
            TextMessage msg = new TextMessage();
            String propertyName = "property";
            msg.Properties.SetLong(propertyName, 1);

            Assert.AreEqual((long)msg.Properties[propertyName], 1);
            Assert.AreEqual(msg.Properties.GetLong(propertyName), 1);
            Assert.AreEqual(msg.Properties[propertyName].ToString(), "1");

            TestGetBoolThrows(msg.Properties, propertyName, "GetBool should have thrown");
            TestGetByteThrows(msg.Properties, propertyName, "GetByte should have thrown");
            TestGetBytesThrows(msg.Properties, propertyName, "GetBytes should have thrown");
            TestGetCharThrows(msg.Properties, propertyName, "GetChar should have thrown");
            TestGetDictionaryThrows(msg.Properties, propertyName, "GetDictionary should have thrown");
            TestGetDoubleThrows(msg.Properties, propertyName, "GetDouble should have thrown");
            TestGetFloatThrows(msg.Properties, propertyName, "GetFloat should have thrown");
            TestGetIntThrows(msg.Properties, propertyName, "GetInt should have thrown");
            TestGetListThrows(msg.Properties, propertyName, "GetList should have thrown");
            //TestGetLongThrows(msg.Properties, propertyName, "GetLong should have thrown");
            TestGetShortThrows(msg.Properties, propertyName, "GetShort should have thrown");
            TestGetStringThrows(msg.Properties, propertyName, "GetString should have thrown");
        }

        [Test]
        public void TestPropertyConversionFloat()
        {
            TextMessage msg = new TextMessage();
            String propertyName = "property";
            float floatValue = (float)1.5;
            msg.Properties.SetFloat(propertyName, floatValue);
            Assert.AreEqual((float)msg.Properties[propertyName], floatValue, 0);
            Assert.AreEqual(msg.Properties.GetFloat(propertyName), floatValue, 0);
            Assert.AreEqual(msg.Properties[propertyName].ToString(), floatValue.ToString());

            TestGetBoolThrows(msg.Properties, propertyName, "GetBool should have thrown");
            TestGetByteThrows(msg.Properties, propertyName, "GetByte should have thrown");
            TestGetBytesThrows(msg.Properties, propertyName, "GetBytes should have thrown");
            TestGetCharThrows(msg.Properties, propertyName, "GetChar should have thrown");
            TestGetDictionaryThrows(msg.Properties, propertyName, "GetDictionary should have thrown");
            TestGetDoubleThrows(msg.Properties, propertyName, "GetDouble should have thrown");
            //TestGetFloatThrows(msg.Properties, propertyName, "GetFloat should have thrown");
            TestGetIntThrows(msg.Properties, propertyName, "GetInt should have thrown");
            TestGetListThrows(msg.Properties, propertyName, "GetList should have thrown");
            TestGetLongThrows(msg.Properties, propertyName, "GetLong should have thrown");
            TestGetShortThrows(msg.Properties, propertyName, "GetShort should have thrown");
            TestGetStringThrows(msg.Properties, propertyName, "GetString should have thrown");
        }

        [Test]
        public void TestPropertyConversionDouble()
        {
            TextMessage msg = new TextMessage();
            String propertyName = "property";
            Double doubleValue = 1.5;
            msg.Properties.SetDouble(propertyName, doubleValue);
            Assert.AreEqual((double)msg.Properties[propertyName], doubleValue, 0);
            Assert.AreEqual(msg.Properties.GetDouble(propertyName), doubleValue, 0);
            Assert.AreEqual(msg.Properties[propertyName].ToString(), doubleValue.ToString());

            TestGetBoolThrows(msg.Properties, propertyName, "GetBool should have thrown");
            TestGetByteThrows(msg.Properties, propertyName, "GetByte should have thrown");
            TestGetBytesThrows(msg.Properties, propertyName, "GetBytes should have thrown");
            TestGetCharThrows(msg.Properties, propertyName, "GetChar should have thrown");
            TestGetDictionaryThrows(msg.Properties, propertyName, "GetDictionary should have thrown");
            //TestGetDoubleThrows(msg.Properties, propertyName, "GetDouble should have thrown");
            TestGetFloatThrows(msg.Properties, propertyName, "GetFloat should have thrown");
            TestGetIntThrows(msg.Properties, propertyName, "GetInt should have thrown");
            TestGetListThrows(msg.Properties, propertyName, "GetList should have thrown");
            TestGetLongThrows(msg.Properties, propertyName, "GetLong should have thrown");
            TestGetShortThrows(msg.Properties, propertyName, "GetShort should have thrown");
            TestGetStringThrows(msg.Properties, propertyName, "GetString should have thrown");
        }

        [Test]
        public void TestPropertyConversionString()
        {
            TextMessage msg = new TextMessage();
            String propertyName = "property";
            String stringValue = "True";
            msg.Properties.SetString(propertyName, stringValue);
            Assert.AreEqual(msg.Properties.GetString(propertyName), stringValue);
            Assert.AreEqual((string)msg.Properties[propertyName], stringValue);

            stringValue = "1";
            msg.Properties.SetString(propertyName, stringValue);
            // TODO:
            //Assert.AreEqual(msg.Properties.GetByte(propertyName), 1);
            //Assert.AreEqual(msg.Properties.GetShort(propertyName), 1);
            //Assert.AreEqual(msg.Properties.GetInt(propertyName), 1);
            //Assert.AreEqual(msg.Properties.GetLong(propertyName), 1);

            Double doubleValue = 1.5;
            stringValue = doubleValue.ToString();
            msg.Properties.SetString(propertyName, stringValue);
            // TODO:
            //Assert.AreEqual(msg.Properties.GetFloat(propertyName), 1.5, 0);
            //Assert.AreEqual(msg.Properties.GetDouble(propertyName), 1.5, 0);

            stringValue = "bad";
            msg.Properties.SetString(propertyName, stringValue);

            TestGetBoolThrows(msg.Properties, propertyName, "GetBool should have thrown");
            TestGetByteThrows(msg.Properties, propertyName, "GetByte should have thrown");
            TestGetBytesThrows(msg.Properties, propertyName, "GetBytes should have thrown");
            TestGetCharThrows(msg.Properties, propertyName, "GetChar should have thrown");
            TestGetDictionaryThrows(msg.Properties, propertyName, "GetDictionary should have thrown");
            TestGetDoubleThrows(msg.Properties, propertyName, "GetDouble should have thrown");
            TestGetFloatThrows(msg.Properties, propertyName, "GetFloat should have thrown");
            TestGetIntThrows(msg.Properties, propertyName, "GetInt should have thrown");
            TestGetListThrows(msg.Properties, propertyName, "GetList should have thrown");
            TestGetLongThrows(msg.Properties, propertyName, "GetLong should have thrown");
            TestGetShortThrows(msg.Properties, propertyName, "GetShort should have thrown");
            //TestGetStringThrows(msg.Properties, propertyName, "GetString should have thrown");
        }
    }
}
