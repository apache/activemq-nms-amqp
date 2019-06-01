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
using System.IO;
using System.Text;
using Amqp.Framing;
using Apache.NMS;
using Apache.NMS.AMQP.Message.Facade;
using Apache.NMS.AMQP.Provider.Amqp.Message;
using Apache.NMS.AMQP.Util;
using NUnit.Framework;

namespace NMS.AMQP.Test.Provider.Amqp
{
    [TestFixture]
    public class AmqpNmsBytesMessageFacadeTest : AmqpNmsMessageTypesTestCase
    {
        [Test]
        public void TestNewMessageDoesNotContainMessageTypeAnnotation()
        {
            AmqpNmsBytesMessageFacade amqpNmsBytesMessageFacade = CreateNewBytesMessageFacade();

            Assert.IsNull(amqpNmsBytesMessageFacade.Message.MessageAnnotations);

            Assert.AreEqual(MessageSupport.JMS_TYPE_BYTE, amqpNmsBytesMessageFacade.JmsMsgType);
        }

        [Test]
        public void TestGetInputStreamWithNewMessageReturnsEmptyInputStream()
        {
            AmqpNmsBytesMessageFacade amqpNmsBytesMessageFacade = CreateNewBytesMessageFacade();

            BinaryReader byteArrayInputStream = amqpNmsBytesMessageFacade.GetDataReader();
            Assert.NotNull(byteArrayInputStream);

            // try to read a byte, it should return 0 bytes read, i.e EOS.
            Assert.AreEqual(0, byteArrayInputStream.Read(new byte[1], 0, 1));
        }

        [Test]
        public void TestGetBodyLengthUsingNewMessage()
        {
            AmqpNmsBytesMessageFacade amqpNmsBytesMessageFacade = CreateNewBytesMessageFacade();

            Assert.AreEqual(0, amqpNmsBytesMessageFacade.BodyLength);
        }

        [Test]
        public void TestNewMessageHasContentType()
        {
            AmqpNmsBytesMessageFacade amqpNmsBytesMessageFacade = CreateNewBytesMessageFacade();

            Assert.NotNull(amqpNmsBytesMessageFacade.Message.Properties);
            Assert.NotNull(amqpNmsBytesMessageFacade.Message.Properties.ContentType);

            string contentType = amqpNmsBytesMessageFacade.Message.Properties.ContentType.ToString();
            Assert.AreEqual("application/octet-stream", contentType);
        }

        // ---------- test for normal message operations -------------------------//

        [Test]
        public void TestGetBodyLengthUsingPopulatedMessageToSend()
        {
            byte[] bytes = Encoding.UTF8.GetBytes("myBytes");

            AmqpNmsBytesMessageFacade amqpNmsBytesMessageFacade = CreateNewBytesMessageFacade();
            BinaryWriter binaryWriter = amqpNmsBytesMessageFacade.GetDataWriter();
            binaryWriter.Write(bytes);

            amqpNmsBytesMessageFacade.Reset();

            Assert.AreEqual(bytes.Length, amqpNmsBytesMessageFacade.BodyLength);
        }

        [Test]
        public void TestGetOutputStreamReturnsSameStream()
        {
            AmqpNmsBytesMessageFacade amqpNmsBytesMessageFacade = CreateNewBytesMessageFacade();

            BinaryReader binaryReader1 = amqpNmsBytesMessageFacade.GetDataReader();
            BinaryReader binaryReader2 = amqpNmsBytesMessageFacade.GetDataReader();

            Assert.AreSame(binaryReader1, binaryReader2);
        }

        [Test]
        public void TestGetInputStreamReturnsSameStream()
        {
            AmqpNmsBytesMessageFacade amqpNmsBytesMessageFacade = CreateNewBytesMessageFacade();

            BinaryWriter binaryWriter1 = amqpNmsBytesMessageFacade.GetDataWriter();
            BinaryWriter binaryWriter2 = amqpNmsBytesMessageFacade.GetDataWriter();

            Assert.AreSame(binaryWriter1, binaryWriter2);
        }

        [Test]
        public void TestClearBodySetsBodyLength0AndCausesEmptyInputStream()
        {
            byte[] bytes = Encoding.UTF8.GetBytes("myBytes");
            global::Amqp.Message message = new global::Amqp.Message
            {
                BodySection = new Data
                {
                    Binary = bytes
                }
            };

            AmqpNmsBytesMessageFacade amqpNmsBytesMessageFacade = CreateReceivedBytesMessageFacade(message);
            amqpNmsBytesMessageFacade.ClearBody();

            Assert.True(amqpNmsBytesMessageFacade.BodyLength == 0, "Expected no message content from facade");
            Assert.AreEqual(0, amqpNmsBytesMessageFacade.GetDataReader().Read(new byte[1], 0, 1));

            AssertDataBodyAsExpected(amqpNmsBytesMessageFacade, 0);
        }

        [Test]
        public void TestClearBodyWithExistingInputStream()
        {
            byte[] bytes = Encoding.UTF8.GetBytes("myBytes");
            global::Amqp.Message message = new global::Amqp.Message
            {
                BodySection = new Data
                {
                    Binary = bytes
                }
            };

            AmqpNmsBytesMessageFacade amqpNmsBytesMessageFacade = CreateReceivedBytesMessageFacade(message);
            amqpNmsBytesMessageFacade.GetDataReader();

            amqpNmsBytesMessageFacade.ClearBody();

            Assert.AreEqual(0, amqpNmsBytesMessageFacade.GetDataReader().Read(new byte[1], 0, 1));
            AssertDataBodyAsExpected(amqpNmsBytesMessageFacade, 0);
        }

        [Test]
        public void TestClearBodyWithExistingOutputStream()
        {
            byte[] bytes = Encoding.UTF8.GetBytes("myBytes");
            global::Amqp.Message message = new global::Amqp.Message
            {
                BodySection = new Data
                {
                    Binary = bytes
                }
            };

            AmqpNmsBytesMessageFacade amqpNmsBytesMessageFacade = CreateReceivedBytesMessageFacade(message);
            amqpNmsBytesMessageFacade.GetDataWriter();

            amqpNmsBytesMessageFacade.ClearBody();

            AssertDataBodyAsExpected(amqpNmsBytesMessageFacade, 0);
        }

        [Test]
        public void TestGetInputStreamThrowsJMSISEWhenFacadeBeingWrittenTo()
        {
            AmqpNmsBytesMessageFacade amqpNmsBytesMessageFacade = CreateNewBytesMessageFacade();

            amqpNmsBytesMessageFacade.GetDataWriter();

            Assert.Throws<IllegalStateException>(() => amqpNmsBytesMessageFacade.GetDataReader());
        }

        [Test]
        public void TestGetOutputStreamThrowsJMSISEWhenFacadeBeingReadFrom()
        {
            AmqpNmsBytesMessageFacade amqpNmsBytesMessageFacade = CreateNewBytesMessageFacade();

            amqpNmsBytesMessageFacade.GetDataReader();

            Assert.Throws<IllegalStateException>(() => amqpNmsBytesMessageFacade.GetDataWriter());
        }

        [Test]
        public void TestGetInputStreamUsingReceivedMessageWithNoBodySectionReturnsEmptyInputStream()
        {
            global::Amqp.Message message = new global::Amqp.Message();

            AmqpNmsBytesMessageFacade amqpNmsBytesMessageFacade = CreateReceivedBytesMessageFacade(message);
            BinaryReader binaryReader = amqpNmsBytesMessageFacade.GetDataReader();
            Assert.NotNull(binaryReader);

            Assert.AreEqual(0, amqpNmsBytesMessageFacade.GetDataReader().Read(new byte[1], 0, 1));
        }

        [Test]
        public void TestGetBodyLengthUsingReceivedMessageWithDataSectionContainingNonZeroLengthBinary()
        {
            int length = 5;
            global::Amqp.Message message = new global::Amqp.Message
            {
                BodySection = new Data
                {
                    Binary = new byte[length]
                }
            };

            AmqpNmsBytesMessageFacade amqpNmsBytesMessageFacade = CreateReceivedBytesMessageFacade(message);

            Assert.AreEqual(length, amqpNmsBytesMessageFacade.BodyLength, "Message reports unexpected length");
        }

        [Test]
        public void TestGetBodyLengthUsingReceivedMessageWithAmqpValueSectionContainingNonZeroLengthBinary()
        {
            int length = 10;
            global::Amqp.Message message = new global::Amqp.Message
            {
                BodySection = new AmqpValue
                {
                    Value = new byte[length]
                }
            };

            AmqpNmsBytesMessageFacade amqpNmsBytesMessageFacade = CreateReceivedBytesMessageFacade(message);

            Assert.AreEqual(length, amqpNmsBytesMessageFacade.BodyLength, "Message reports unexpected length");
        }

        [Test]
        public void TestGetBodyLengthUsingReceivedMessageWithAmqpValueSectionContainingZeroLengthBinary()
        {
            global::Amqp.Message message = new global::Amqp.Message
            {
                BodySection = new AmqpValue
                {
                    Value = new byte[0]
                }
            };

            AmqpNmsBytesMessageFacade amqpNmsBytesMessageFacade = CreateReceivedBytesMessageFacade(message);

            Assert.AreEqual(0, amqpNmsBytesMessageFacade.BodyLength, "Message reports unexpected length");
        }

        [Test]
        public void TestGetBodyLengthUsingReceivedMessageWithAmqpValueSectionContainingNull()
        {
            global::Amqp.Message message = new global::Amqp.Message
            {
                BodySection = new AmqpValue
                {
                    Value = null
                }
            };

            AmqpNmsBytesMessageFacade amqpNmsBytesMessageFacade = CreateReceivedBytesMessageFacade(message);

            Assert.AreEqual(0, amqpNmsBytesMessageFacade.BodyLength, "Message reports unexpected length");
        }

        [Test]
        public void TestInputStreamUsingReceivedMessageWithAmqpValueSectionContainingBinary()
        {
            byte[] bytes = Encoding.UTF8.GetBytes("myBytes");
            global::Amqp.Message message = new global::Amqp.Message
            {
                BodySection = new AmqpValue
                {
                    Value = bytes
                }
            };

            AmqpNmsBytesMessageFacade amqpNmsBytesMessageFacade = CreateReceivedBytesMessageFacade(message);
            BinaryReader binaryReader = amqpNmsBytesMessageFacade.GetDataReader();

            // retrieve the expected bytes, check they match
            byte[] receivedBytes = new byte[bytes.Length];

            binaryReader.Read(receivedBytes, 0, bytes.Length);

            CollectionAssert.AreEqual(bytes, receivedBytes);

            // verify no more bytes remain, i.e EOS
            Assert.AreEqual(0, binaryReader.Read(new byte[1], 0, 1));
        }

        [Test]
        public void TestInputStreamUsingReceivedMessageWithDataSection()
        {
            byte[] bytes = Encoding.UTF8.GetBytes("myBytes");
            global::Amqp.Message message = new global::Amqp.Message
            {
                BodySection = new Data()
                {
                    Binary = bytes
                }
            };

            AmqpNmsBytesMessageFacade amqpNmsBytesMessageFacade = CreateReceivedBytesMessageFacade(message);
            BinaryReader binaryReader = amqpNmsBytesMessageFacade.GetDataReader();

            // retrieve the expected bytes, check they match
            byte[] receivedBytes = new byte[bytes.Length];

            binaryReader.Read(receivedBytes, 0, bytes.Length);

            CollectionAssert.AreEqual(bytes, receivedBytes);

            // verify no more bytes remain, i.e EOS
            Assert.AreEqual(0, binaryReader.Read(new byte[1], 0, 1));
        }

        [Test]
        public void TestGetInputStreamUsingReceivedMessageWithDataSectionContainingNothingReturnsEmptyStream()
        {
            global::Amqp.Message message = new global::Amqp.Message
            {
                BodySection = new Data()
                {
                    Binary = null
                }
            };

            AmqpNmsBytesMessageFacade amqpNmsBytesMessageFacade = CreateReceivedBytesMessageFacade(message);
            BinaryReader binaryReader = amqpNmsBytesMessageFacade.GetDataReader();

            Assert.NotNull(binaryReader);

            Assert.AreEqual(0, amqpNmsBytesMessageFacade.BodyLength, "Message reports unexpected length");
            Assert.AreEqual(0, binaryReader.Read(new byte[1], 0, 1));
        }

        [Test]
        public void TestGetMethodsWithNonAmqpValueNonDataSectionThrowsISE()
        {
            global::Amqp.Message message = new global::Amqp.Message
            {
                BodySection = new AmqpSequence()
                {
                    List = new List<object>()
                }
            };

            AmqpNmsBytesMessageFacade amqpNmsBytesMessageFacade = CreateReceivedBytesMessageFacade(message);

            Assert.Throws<IllegalStateException>(() => amqpNmsBytesMessageFacade.GetDataReader());
            Assert.Throws<IllegalStateException>(() =>
            {
                long bodyLength = amqpNmsBytesMessageFacade.BodyLength;
            });
        }

        /*
         * Test that setting bytes on a received message results in the expected content in the body section
         * of the underlying message and returned by a new InputStream requested from the message.
         */
        [Test]
        public void TestGetMethodsWithAmqpValueContainingNonNullNonBinaryValueThrowsISE()
        {
            byte[] orig = Encoding.UTF8.GetBytes("myOrigBytes");
            byte[] replacement = Encoding.UTF8.GetBytes("myReplacementBytes");

            global::Amqp.Message message = new global::Amqp.Message
            {
                BodySection = new Data()
                {
                    Binary = orig
                }
            };

            AmqpNmsBytesMessageFacade amqpNmsBytesMessageFacade = CreateReceivedBytesMessageFacade(message);

            BinaryWriter binaryWriter = amqpNmsBytesMessageFacade.GetDataWriter();
            binaryWriter.Write(replacement);

            amqpNmsBytesMessageFacade.Reset();

            // Retrieve the new Binary from the underlying message, check they match
            // (the backing arrays may be different length so not checking arrayEquals)
            Data body = (Data) amqpNmsBytesMessageFacade.Message.BodySection;
            Assert.AreEqual(replacement, body.Binary, "Underlying message data section did not contain the expected bytes");

            Assert.AreEqual(replacement.Length, amqpNmsBytesMessageFacade.BodyLength, "expected body length to match replacement bytes");

            // retrieve the new bytes via an InputStream, check they match expected
            byte[] receivedBytes = new byte[replacement.Length];
            BinaryReader binaryReader = amqpNmsBytesMessageFacade.GetDataReader();
            binaryReader.Read(receivedBytes, 0, replacement.Length);

            CollectionAssert.AreEqual(replacement, receivedBytes, "Retrieved bytes from input steam did not match expected bytes");

            // verify no more bytes remain, i.e EOS
            Assert.AreEqual(0, binaryReader.Read(new byte[1], 0, 1));
        }

        [Test]
        public void TestHasBodyOnNewMessage()
        {
            AmqpNmsBytesMessageFacade amqpNmsBytesMessageFacade = CreateNewBytesMessageFacade();
            
            Assert.False(amqpNmsBytesMessageFacade.HasBody());
        }

        [Test]
        public void TestHasBodyWithContent()
        {
            byte[] bodyBytes = Encoding.UTF8.GetBytes("myOrigBytes");
            global::Amqp.Message message = new global::Amqp.Message
            {
                BodySection = new Data()
                {
                    Binary = bodyBytes
                }
            };
            
            AmqpNmsBytesMessageFacade amqpNmsBytesMessageFacade = CreateReceivedBytesMessageFacade(message);
            
            Assert.True(amqpNmsBytesMessageFacade.HasBody());
            
            amqpNmsBytesMessageFacade.ClearBody();
            
            Assert.False(amqpNmsBytesMessageFacade.HasBody());
        }

        [Test]
        public void TestHasBodyWithActiveInputStream()
        {
            byte[] bodyBytes = Encoding.UTF8.GetBytes("myOrigBytes");
            
            global::Amqp.Message message = new global::Amqp.Message
            {
                BodySection = new Data()
                {
                    Binary = bodyBytes
                }
            };
            
            AmqpNmsBytesMessageFacade amqpNmsBytesMessageFacade = CreateReceivedBytesMessageFacade(message);
            
            Assert.True(amqpNmsBytesMessageFacade.HasBody());

            amqpNmsBytesMessageFacade.GetDataReader();
            
            Assert.True(amqpNmsBytesMessageFacade.HasBody());
        }

        [Test]
        public void TestHasBodyWithActiveOutputStream()
        {
            AmqpNmsBytesMessageFacade amqpNmsBytesMessageFacade = CreateNewBytesMessageFacade();
            
            Assert.False(amqpNmsBytesMessageFacade.HasBody());

            BinaryWriter binaryWriter = amqpNmsBytesMessageFacade.GetDataWriter();

            Assert.False(amqpNmsBytesMessageFacade.HasBody());
            
            binaryWriter.Write(1);
            
            // Body exists after some data written.
            
            Assert.True(amqpNmsBytesMessageFacade.HasBody());
            amqpNmsBytesMessageFacade.Reset();
            Assert.True(amqpNmsBytesMessageFacade.HasBody());
        }

        [Test]
        public void TestCopyOnPopulatedNewMessageCreatesDataSection()
        {
            AmqpNmsBytesMessageFacade amqpNmsBytesMessageFacade = CreateNewBytesMessageFacade();
            
            byte[] bytes = Encoding.UTF8.GetBytes("myBytes");
            BinaryWriter binaryWriter = amqpNmsBytesMessageFacade.GetDataWriter();
            binaryWriter.Write(bytes);

            AmqpNmsBytesMessageFacade copy = amqpNmsBytesMessageFacade.Copy() as AmqpNmsBytesMessageFacade;
            Assert.IsNotNull(copy);
            AssertDataBodyAsExpected(amqpNmsBytesMessageFacade, bytes.Length);
            AssertDataBodyAsExpected(copy, bytes.Length);
            Assert.AreEqual(amqpNmsBytesMessageFacade.ContentType, copy.ContentType);
        }

        [Test]
        public void TestCopyOfNewMessageDoesNotCreateDataSection()
        {
            AmqpNmsBytesMessageFacade amqpNmsBytesMessageFacade = CreateNewBytesMessageFacade();

            AmqpNmsBytesMessageFacade copy = amqpNmsBytesMessageFacade.Copy() as AmqpNmsBytesMessageFacade;
            Assert.IsNotNull(copy);
            AssertDataBodyAsExpected(amqpNmsBytesMessageFacade, 0);
            AssertDataBodyAsExpected(copy, 0);
            Assert.AreEqual(amqpNmsBytesMessageFacade.ContentType, copy.ContentType);
        }

        [Test]
        public void TestGetOutputStreamOnCopiedMessageLeavesOriginalUntouched()
        {
            AmqpNmsBytesMessageFacade amqpNmsBytesMessageFacade = CreateNewBytesMessageFacade();
            
            byte[] bytes = Encoding.UTF8.GetBytes("myBytes");
            BinaryWriter binaryWriter = amqpNmsBytesMessageFacade.GetDataWriter();
            binaryWriter.Write(bytes);
            
            AmqpNmsBytesMessageFacade copy = amqpNmsBytesMessageFacade.Copy() as AmqpNmsBytesMessageFacade;
            
            Assert.IsNotNull(copy);
            AssertDataBodyAsExpected(amqpNmsBytesMessageFacade, bytes.Length);
            AssertDataBodyAsExpected(copy, bytes.Length);

            copy.GetDataWriter();
            AssertDataBodyAsExpected(amqpNmsBytesMessageFacade, bytes.Length);
            AssertDataBodyAsExpected(copy, 0);
        }

        private void AssertDataBodyAsExpected(AmqpNmsBytesMessageFacade facade, int length)
        {
            Assert.NotNull(facade.Message.BodySection, "Expected body section to be present");
            Assert.IsInstanceOf<Data>(facade.Message.BodySection);
            byte[] value = ((Data) facade.Message.BodySection).Binary;
            Assert.NotNull(value);
            Assert.AreEqual(length, value.Length, "Unexpected body length");
        }
    }
}