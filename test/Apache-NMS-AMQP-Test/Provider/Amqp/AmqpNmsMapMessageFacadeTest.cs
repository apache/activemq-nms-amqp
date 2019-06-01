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

using System.Text;
using Amqp.Framing;
using Amqp.Types;
using Apache.NMS;
using Apache.NMS.AMQP.Provider.Amqp;
using Apache.NMS.AMQP.Provider.Amqp.Message;
using Apache.NMS.AMQP.Util;
using NUnit.Framework;

namespace NMS.AMQP.Test.Provider.Amqp
{
    [TestFixture]
    public class AmqpNmsMapMessageFacadeTest : AmqpNmsMessageTypesTestCase
    {
        //---------- Test initial state of newly created message -----------------//

        [Test]
        public void TestNewMessageToSendDoesNotContainMessageTypeAnnotation()
        {
            AmqpNmsMapMessageFacade facade = CreateNewMapMessageFacade();

            Assert.Null(facade.MessageAnnotations);

            Assert.AreEqual(MessageSupport.JMS_TYPE_MAP, facade.JmsMsgType);
        }

        [Test]
        public void TestNewMessageToSendClearBodyDoesNotFail()
        {
            AmqpNmsMapMessageFacade facade = CreateNewMapMessageFacade();
            facade.ClearBody();
        }

        [Test]
        public void TestNewMessageToSendReportsNoBody()
        {
            AmqpNmsMapMessageFacade facade = CreateNewMapMessageFacade();
            facade.HasBody();
        }

        [Test]
        public void TestNewMessageToSendReportsIsEmpty()
        {
            AmqpNmsMapMessageFacade facade = CreateNewMapMessageFacade();
            CollectionAssert.IsEmpty(facade.Map.Keys);
        }

        [Test]
        public void TestNewMessageToSendItemExists()
        {
            AmqpNmsMapMessageFacade facade = CreateNewMapMessageFacade();
            Assert.False(facade.Map.Contains("entry"));
        }

        [Test]
        public void TestNewMessageToSendReturnsEmptyMapNamesEnumeration()
        {
            AmqpNmsMapMessageFacade facade = CreateNewMapMessageFacade();
            Assert.NotNull(facade.Map.Keys);
        }

        [Test]
        public void TestMessageClearBodyWorks()
        {
            AmqpNmsMapMessageFacade facade = CreateNewMapMessageFacade();
            CollectionAssert.IsEmpty(facade.Map.Keys);
            facade.Map.SetString("entry1", "value1");
            CollectionAssert.IsNotEmpty(facade.Map.Keys);
            facade.ClearBody();
            CollectionAssert.IsEmpty(facade.Map.Keys);
        }

        // ---------- test handling of received messages -------------------------//
        [Test]
        public void TestCreateWithEmptyMap()
        {
            global::Amqp.Message message = new global::Amqp.Message
            {
                BodySection = new AmqpValue { Value = new Map() }
            };

            AmqpNmsMapMessageFacade facade = CreateReceivedMapMessageFacade(message);

            // Should be able to use the message, e.g clearing it and adding to it.
            facade.ClearBody();
            facade.Map.SetString("entry1", "value1");
        }

        [Test]
        public void TestCreateWithPopulatedMap()
        {
            global::Amqp.Message message = new global::Amqp.Message
            {
                BodySection = new AmqpValue
                {
                    Value = new Map
                    {
                        { "entry1", true },
                        { "entry2", false }
                    }
                }
            };

            AmqpNmsMapMessageFacade facade = CreateReceivedMapMessageFacade(message);

            // Data should be preserved
            Assert.True(facade.Map.Keys.Count > 0);
            bool result = facade.Map.GetBool("entry1");
            Assert.True(result);
            Assert.True(facade.HasBody());

            // Should be able to use the message, e.g clearing it and adding to it.
            facade.ClearBody();
            Assert.False(facade.HasBody());
            facade.Map.SetString("entry", "value");
        }

        [Test]
        public void TestCreateWithAmqpSequenceBodySectionThrowsISE()
        {
            global::Amqp.Message message = new global::Amqp.Message
            {
                BodySection = new AmqpSequence { List = null }
            };

            Assert.Throws<IllegalStateException>(() => CreateReceivedMapMessageFacade(message));
        }

        [Test]
        public void TestCreateWithAmqpValueBodySectionContainingUnexpectedValueThrowsISE()
        {
            global::Amqp.Message message = new global::Amqp.Message
            {
                BodySection = new AmqpValue { Value = "not-a-map" }
            };

            Assert.Throws<IllegalStateException>(() => CreateReceivedMapMessageFacade(message));
        }

        [Test]
        public void TestCreateWithNullBodySection()
        {
            global::Amqp.Message message = new global::Amqp.Message
            {
                BodySection = null
            };

            AmqpNmsMapMessageFacade facade = CreateReceivedMapMessageFacade(message);

            // Should be able to use the message, e.g clearing it and adding to it.
            facade.ClearBody();
            facade.Map.SetString("entry", "value");
            CollectionAssert.IsNotEmpty(facade.Map.Keys);
        }

        [Test]
        public void TestCreateWithEmptyAmqpValueBodySection()
        {
            global::Amqp.Message message = new global::Amqp.Message
            {
                BodySection = new AmqpValue()
                {
                    Value = null
                }
            };

            AmqpNmsMapMessageFacade facade = CreateReceivedMapMessageFacade(message);

            // Should be able to use the message, e.g clearing it and adding to it.
            facade.ClearBody();
            facade.Map.SetString("entry", "value");
            CollectionAssert.IsNotEmpty(facade.Map.Keys);
        }

        //----- Test Read / Write of special contents in Map ---------------------//

        /*
         * Verify that for a message received with an AmqpValue containing a Map with a Binary entry
         * value, we are able to read it back as a byte[]. 
         */
        [Test]
        public void TestReceivedMapWithBinaryEntryReturnsByteArray()
        {
            string myKey1 = "key1";
            string bytesSource = "myBytesAmqpValue";
            byte[] bytes = Encoding.UTF8.GetBytes(bytesSource);

            global::Amqp.Message message = new global::Amqp.Message
            {
                BodySection = new AmqpValue { Value = new Map { { myKey1, bytes } } }
            };

            AmqpNmsMapMessageFacade facade = CreateReceivedMapMessageFacade(message);

            // retrieve the bytes using getBytes, check they match expectation
            byte[] bytesValue = facade.Map.GetBytes(myKey1);
            CollectionAssert.AreEqual(bytes, bytesValue);
        }

        [Test]
        public void TestMessageCopy()
        {
            AmqpNmsMapMessageFacade facade = CreateNewMapMessageFacade();
            facade.Map.SetString("entry1", "value");
            facade.Map.SetByte("entry2", 1);
            facade.Map.SetInt("entry3", 1);

            AmqpNmsMapMessageFacade copy = facade.Copy() as AmqpNmsMapMessageFacade;
            Assert.IsNotNull(copy);
            Assert.True(copy.Map.Contains("entry1"));
            Assert.True(copy.Map.Contains("entry2"));
            Assert.True(copy.Map.Contains("entry3"));
        }
    }
}