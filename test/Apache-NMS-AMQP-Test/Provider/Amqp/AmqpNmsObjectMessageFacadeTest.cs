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
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using Amqp.Framing;
using Amqp.Types;
using Apache.NMS;
using Apache.NMS.AMQP.Provider.Amqp.Message;
using Apache.NMS.AMQP.Util;
using NUnit.Framework;

namespace NMS.AMQP.Test.Provider.Amqp
{
    [TestFixture]
    public class AmqpNmsObjectMessageFacadeTest : AmqpNmsMessageTypesTestCase
    {
        // ---------- Test initial state of newly created message -----------------//

        [Test]
        public void TestNewMessageToSendDoesNotContainMessageTypeAnnotation()
        {
            AmqpNmsObjectMessageFacade amqpObjectMessageFacade = CreateNewObjectMessageFacade(false);
            Assert.Null(amqpObjectMessageFacade.MessageAnnotations);

            Assert.IsNull(amqpObjectMessageFacade.JmsMsgType);
        }

        [Test]
        public void TestNewMessageToSendReturnsNullObject()
        {
            DoNewMessageToSendReturnsNullObjectTestImpl(false);
        }

        [Test]
        public void TestNewAmqpTypedMessageToSendReturnsNullObject()
        {
            DoNewMessageToSendReturnsNullObjectTestImpl(true);
        }

        private void DoNewMessageToSendReturnsNullObjectTestImpl(bool amqpTyped)
        {
            AmqpNmsObjectMessageFacade amqpObjectMessageFacade = CreateNewObjectMessageFacade(amqpTyped);
            Assert.Null(amqpObjectMessageFacade.Object);
        }

        [Test]
        public void TestNewMessageToSendHasBodySectionRepresentingNull()
        {
            DoNewMessageToSendHasBodySectionRepresentingNull(false);
        }

        [Test]
        public void TestNewAmqpTypedMessageToSendHasBodySectionRepresentingNull()
        {
            DoNewMessageToSendHasBodySectionRepresentingNull(true);
        }

        private void DoNewMessageToSendHasBodySectionRepresentingNull(bool amqpTyped)
        {
            AmqpNmsObjectMessageFacade amqpObjectMessageFacade = CreateNewObjectMessageFacade(amqpTyped);
            amqpObjectMessageFacade.OnSend(TimeSpan.Zero);

            Assert.NotNull(amqpObjectMessageFacade.Message.BodySection, "Message body should be presents");
            if (amqpTyped)
                Assert.AreSame(AmqpTypedObjectDelegate.NULL_OBJECT_BODY, amqpObjectMessageFacade.Message.BodySection, "Expected existing body section to be replaced");
            else
                Assert.AreSame(AmqpSerializedObjectDelegate.NULL_OBJECT_BODY, amqpObjectMessageFacade.Message.BodySection, "Expected existing body section to be replaced");
        }

        // ---------- test for normal message operations -------------------------//

        /*
         * Test that Bytes setting an object on a new message results in the expected
         * content in the body section of the underlying message.
         */
        [Test]
        public void TestSetObjectOnNewMessage()
        {
            String content = "myStringContent";

            AmqpNmsObjectMessageFacade amqpObjectMessageFacade = CreateNewObjectMessageFacade();
            amqpObjectMessageFacade.Object = content;

            var bytes = GetSerializedBytes(content);

            // retrieve the bytes from the underlying message, check they match expectation
            RestrictedDescribed messageBodySection = amqpObjectMessageFacade.Message.BodySection;
            Assert.NotNull(messageBodySection);

            Assert.IsInstanceOf<Data>(messageBodySection);
            CollectionAssert.AreEqual(bytes, ((Data) messageBodySection).Binary, "Underlying message data section did not contain the expected bytes");
        }

        /*
         * Test that setting an object on a new message results in the expected
         * content in the body section of the underlying message.
         */
        [Test]
        public void TestSetObjectOnNewAmqpTypedMessage()
        {
            String content = "myStringContent";

            AmqpNmsObjectMessageFacade amqpObjectMessageFacade = CreateNewObjectMessageFacade(true);
            amqpObjectMessageFacade.Object = content;

            // retrieve the body from the underlying message, check it matches expectation
            RestrictedDescribed section = amqpObjectMessageFacade.Message.BodySection;
            Assert.NotNull(section);
            Assert.IsInstanceOf<AmqpValue>(section);
            Assert.AreEqual(content, ((AmqpValue) section).Value);
        }

        /*
         * Test that setting a null object on a message results in the underlying body
         * section being set with the null object body, ensuring getObject returns null.
         */
        [Test]
        public void TestSetObjectWithNullClearsExistingBodySection()
        {
            global::Amqp.Message message = new global::Amqp.Message
            {
                Properties = new Properties
                {
                    ContentType = MessageSupport.SERIALIZED_DOTNET_OBJECT_CONTENT_TYPE
                },
                BodySection = new Data
                {
                    Binary = new byte[0]
                }
            };

            AmqpNmsObjectMessageFacade facade = CreateReceivedObjectMessageFacade(message);

            Assert.NotNull(facade.Message.BodySection, "Expected existing body section to be found");
            facade.Object = null;
            Assert.AreSame(AmqpSerializedObjectDelegate.NULL_OBJECT_BODY, facade.Message.BodySection, "Expected existing body section to be replaced");
            Assert.Null(facade.Object);
        }

        /*
         * Test that clearing the body on a message results in the underlying body
         * section being set with the null object body, ensuring getObject returns null.
         */
        [Test]
        public void TestClearBodyWithExistingSerializedBodySection()
        {
            global::Amqp.Message message = new global::Amqp.Message
            {
                Properties = new Properties
                {
                    ContentType = MessageSupport.SERIALIZED_DOTNET_OBJECT_CONTENT_TYPE
                },
                BodySection = new Data
                {
                    Binary = new byte[0]
                }
            };

            AmqpNmsObjectMessageFacade facade = CreateReceivedObjectMessageFacade(message);

            Assert.NotNull(facade.Message.BodySection, "Expected existing body section to be found");
            facade.ClearBody();

            Assert.AreSame(AmqpSerializedObjectDelegate.NULL_OBJECT_BODY, facade.Message.BodySection, "Expected existing body section to be replaced");
            Assert.Null(facade.Object);
        }

        /*
         * Test that setting an object on a new message and later getting the value, returns an
         * equal but different object that does not pick up intermediate changes to the set object.
         */
        [Test]
        public void TestSetThenGetObjectOnSerializedMessageReturnsSnapshot()
        {
            Dictionary<string, string> origMap = new Dictionary<string, string>
            {
                {"key1", "value1"}
            };

            AmqpNmsObjectMessageFacade facade = CreateNewObjectMessageFacade(false);
            facade.Object = origMap;

            Dictionary<string, string> d = new Dictionary<string, string>();

            // verify we get a different-but-equal object back
            object body = facade.Object;
            Assert.IsInstanceOf<Dictionary<string, string>>(body);
            Dictionary<string, string> returnedObject1 = (Dictionary<string, string>) body;
            Assert.AreNotSame(origMap, returnedObject1, "Expected different objects, due to snapshot being taken");
            Assert.AreEqual(origMap, returnedObject1, "Expected equal objects, due to snapshot being taken");

            // mutate the original object
            origMap.Add("key2", "value2");

            // verify we get a different-but-equal object back when compared to the previously retrieved object
            object body2 = facade.Object;
            Assert.IsInstanceOf<Dictionary<string, string>>(body2);
            Dictionary<string, string> returnedObject2 = (Dictionary<string, string>) body2;
            Assert.AreNotSame(origMap, returnedObject2, "Expected different objects, due to snapshot being taken");
            Assert.AreEqual(returnedObject1, returnedObject2);

            // verify the mutated map is a different and not equal object
            Assert.AreNotSame(returnedObject1, returnedObject2, "Expected different objects, due to snapshot being taken");
            Assert.AreNotEqual(origMap, returnedObject2, "Expected objects to differ, due to snapshot being taken");
        }

        // ---------- test handling of received messages -------------------------//

        [Test]
        public void TestGetObjectUsingReceivedMessageWithNoBodySectionNoContentTypeReturnsNull()
        {
            DoGetObjectUsingReceivedMessageWithNoBodySectionReturnsNullTestImpl(true);
        }

        [Test]
        public void TestGetObjectUsingReceivedMessageWithNoBodySectionReturnsNull()
        {
            DoGetObjectUsingReceivedMessageWithNoBodySectionReturnsNullTestImpl(false);
        }

        private void DoGetObjectUsingReceivedMessageWithNoBodySectionReturnsNullTestImpl(bool amqpTyped)
        {
            global::Amqp.Message message = new global::Amqp.Message();

            if (!amqpTyped)
            {
                message.Properties = new Properties {ContentType = MessageSupport.SERIALIZED_DOTNET_OBJECT_CONTENT_TYPE};
            }

            AmqpNmsObjectMessageFacade facade = CreateReceivedObjectMessageFacade(message);

            Assert.Null(facade.Object, "Expected null object");
        }

        [Test]
        public void TestGetObjectUsingReceivedMessageWithDataSectionContainingNothingReturnsNull()
        {
            global::Amqp.Message message = new global::Amqp.Message
            {
                Properties = new Properties
                {
                    ContentType = MessageSupport.SERIALIZED_DOTNET_OBJECT_CONTENT_TYPE
                },
                BodySection = new AmqpValue {Value = "nonBinarySectionContent"}
            };

            AmqpNmsObjectMessageFacade facade = CreateReceivedObjectMessageFacade(message);

            Assert.Catch<IllegalStateException>(() =>
            {
                object body = facade.Object;
            });
        }

        /*
         * Test that setting an object on a received message and later getting the value, returns an
         * equal but different object that does not pick up intermediate changes to the set object.
         */
        [Test]
        public void TestSetThenGetObjectOnSerializedReceivedMessageNoContentTypeReturnsSnapshot()
        {
            Dictionary<string, string> origMap = new Dictionary<string, string>
            {
                {"key1", "value1"}
            };
            global::Amqp.Message message = new global::Amqp.Message()
            {
                Properties = new Properties {ContentType = MessageSupport.SERIALIZED_DOTNET_OBJECT_CONTENT_TYPE},
                BodySection = new Data {Binary = GetSerializedBytes(origMap)}
            };

            AmqpNmsObjectMessageFacade facade = CreateReceivedObjectMessageFacade(message);

            // verify we get a different-but-equal object back
            object body = facade.Object;
            Assert.IsInstanceOf<Dictionary<string, string>>(body);
            Dictionary<string, string> returnedObject1 = (Dictionary<string, string>) body;
            Assert.AreNotSame(origMap, returnedObject1, "Expected different objects, due to snapshot being taken");
            Assert.AreEqual(origMap, returnedObject1, "Expected equal objects, due to snapshot being taken");


            // verify we get a different-but-equal object back when compared to the previously retrieved object
            object body2 = facade.Object;
            Assert.IsInstanceOf<Dictionary<string, string>>(body2);
            Dictionary<string, string> returnedObject2 = (Dictionary<string, string>) body2;
            Assert.AreNotSame(origMap, returnedObject2, "Expected different objects, due to snapshot being taken");
            Assert.AreEqual(returnedObject1, returnedObject2);

            // mutate the original object
            origMap.Add("key2", "value2");

            // verify the mutated map is a different and not equal object
            Assert.AreNotSame(returnedObject1, returnedObject2, "Expected different objects, due to snapshot being taken");
            Assert.AreNotEqual(origMap, returnedObject2, "Expected objects to differ, due to snapshot being taken");
        }

        [Test]
        public void TestSetThenGetObjectOnSerializedReceivedMessageReturnsSnapshot()
        {
            Map origMap = new Map
            {
                {"key1", "value1"}
            };
            global::Amqp.Message message = new global::Amqp.Message {BodySection = new AmqpValue {Value = origMap}};

            AmqpNmsObjectMessageFacade facade = CreateReceivedObjectMessageFacade(message);

            // verify we get a different-but-equal object back
            object body = facade.Object;
            Assert.IsInstanceOf<Map>(body);
            Map returnedObject1 = (Map) body;
            Assert.AreNotSame(origMap, returnedObject1, "Expected different objects, due to snapshot being taken");
            Assert.AreEqual(origMap, returnedObject1, "Expected equal objects, due to snapshot being taken");


            // verify we get a different-but-equal object back when compared to the previously retrieved object
            object body2 = facade.Object;
            Assert.IsInstanceOf<Map>(body2);
            Map returnedObject2 = (Map) body2;
            Assert.AreNotSame(origMap, returnedObject2, "Expected different objects, due to snapshot being taken");
            Assert.AreEqual(returnedObject1, returnedObject2);

            // mutate the original object
            origMap.Add("key2", "value2");

            // verify the mutated map is a different and not equal object
            Assert.AreNotSame(returnedObject1, returnedObject2, "Expected different objects, due to snapshot being taken");
            Assert.AreNotEqual(origMap, returnedObject2, "Expected objects to differ, due to snapshot being taken");
        }

        [Test]
        public void TestMessageCopy()
        {
            String content = "myStringContent";

            AmqpNmsObjectMessageFacade amqpObjectMessageFacade = CreateNewObjectMessageFacade();
            amqpObjectMessageFacade.Object = content;

            AmqpNmsObjectMessageFacade copy = amqpObjectMessageFacade.Copy() as AmqpNmsObjectMessageFacade;
            Assert.IsNotNull(copy);
            Assert.AreEqual(amqpObjectMessageFacade.Object, copy.Object);
        }

        private static byte[] GetSerializedBytes(object content)
        {
            using (MemoryStream stream = new MemoryStream())
            {
                BinaryFormatter formatter = new BinaryFormatter();
                formatter.Serialize(stream, content);
                byte[] bytes = stream.ToArray();
                return bytes;
            }
        }
    }
}