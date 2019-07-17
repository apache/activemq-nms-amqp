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
using System.Linq;
using System.Text;
using Apache.NMS;
using Apache.NMS.AMQP.Message;
using NMS.AMQP.Test.Message.Facade;
using NUnit.Framework;

namespace NMS.AMQP.Test.Message
{
    [TestFixture]
    public class NmsBytesMessageTest
    {
        private readonly INmsMessageFactory factory = new TestMessageFactory();

        [Test]
        public void TestToString()
        {
            NmsBytesMessage bytesMessage = factory.CreateBytesMessage();
            Assert.True(bytesMessage.ToString().StartsWith("NmsBytesMessage"));
        }

        [Test]
        public void TestResetOnNewlyPopulatedBytesMessageUpdatesBodyLength()
        {
            byte[] bytes = Encoding.UTF8.GetBytes("myBytes");
            NmsBytesMessage bytesMessage = factory.CreateBytesMessage();
            bytesMessage.WriteBytes(bytes);
            bytesMessage.Reset();
            Assert.AreEqual(bytes.Length, bytesMessage.BodyLength, "Message reports unexpected length");
        }

        [Test]
        public void TestGetBodyLengthOnNewMessageThrowsMessageNotReadableException()
        {
            NmsBytesMessage bytesMessage = factory.CreateBytesMessage();

            try
            {
                long _ = bytesMessage.BodyLength;
                Assert.Fail("Exception expected");
            }
            catch (MessageNotReadableException)
            {
            }
        }

        [Test]
        public void TestReadBytesUsingReceivedMessageWithNoBodyReturnsEOS()
        {
            NmsBytesMessage bytesMessage = factory.CreateBytesMessage();
            bytesMessage.OnDispatch();
            Assert.AreEqual(0, bytesMessage.ReadBytes(new byte[1]));
        }

        [Test]
        public void TestReadBytesUsingReceivedMessageWithBodyReturnsBytes()
        {
            byte[] content = Encoding.UTF8.GetBytes("myBytes");

            NmsTestBytesMessageFacade facade = new NmsTestBytesMessageFacade(content);
            NmsBytesMessage bytesMessage = new NmsBytesMessage(facade);
            bytesMessage.OnDispatch();

            // retrieve the expected bytes, check they match
            byte[] receivedBytes = new byte[content.Length];
            bytesMessage.ReadBytes(receivedBytes);
            CollectionAssert.AreEqual(content, receivedBytes);

            // verify no more bytes remain, i.e EOS
            Assert.AreEqual(0, bytesMessage.ReadBytes(new byte[1]), "Expected input stream to be at end but data was returned");
            Assert.AreEqual(content.Length, bytesMessage.BodyLength);
        }

        /*
         * Test that attempting to write bytes to a received message (without calling BytesMessage#clearBody first)
         * causes a MessageNotWriteableException to be thrown due to being read-only.
         */
        [Test]
        public void TestReceivedBytesMessageThrowsMessageNotWriteableExceptionOnWriteBytes()
        {
            byte[] content = Encoding.UTF8.GetBytes("myBytes");

            NmsTestBytesMessageFacade facade = new NmsTestBytesMessageFacade(content);
            NmsBytesMessage bytesMessage = new NmsBytesMessage(facade);
            bytesMessage.OnDispatch();
            Assert.Throws<MessageNotWriteableException>(() => bytesMessage.WriteBytes(content));
        }

        /*
         * Test that attempting to read bytes from a new message (without calling BytesMessage#reset first) causes a
         * MessageNotReadableException to be thrown due to being write-only.
         */
        [Test]
        public void TestNewBytesMessageThrowsMessageNotReadableOnReadBytes()
        {
            NmsBytesMessage bytesMessage = factory.CreateBytesMessage();
            byte[] receivedBytes = new byte[1];
            Assert.Throws<MessageNotReadableException>(() => bytesMessage.ReadBytes(receivedBytes));
        }

        /*
         * Test that calling BytesMessage#clearBody causes a received
         * message to become writable
         */
        [Test]
        public void TestClearBodyOnReceivedBytesMessageMakesMessageWritable()
        {
            byte[] content = Encoding.UTF8.GetBytes("myBytes");
            NmsTestBytesMessageFacade facade = new NmsTestBytesMessageFacade(content);

            NmsBytesMessage bytesMessage = new NmsBytesMessage(facade);
            bytesMessage.OnDispatch();

            Assert.True(bytesMessage.IsReadOnlyBody);
            bytesMessage.ClearBody();
            Assert.False(bytesMessage.IsReadOnlyBody);
        }

        /*
         * Test that calling BytesMessage#ClearBody of a received message
         * causes the facade input stream to be empty and body length to return 0.         
         */
        [Test]
        public void TestClearBodyOnReceivedBytesMessageClearsFacadeInputStream()
        {
            byte[] content = Encoding.UTF8.GetBytes("myBytes");
            NmsTestBytesMessageFacade facade = new NmsTestBytesMessageFacade(content);

            NmsBytesMessage bytesMessage = new NmsBytesMessage(facade);
            bytesMessage.OnDispatch();

            Assert.True(facade.BodyLength > 0, "Expected message content but none was present");
            Assert.AreEqual(1, facade.GetDataReader().Read(new byte[1], 0, 1), "Expected data from facade");
            bytesMessage.ClearBody();
            Assert.True(facade.BodyLength == 0, "Expected no message content from facade");
            Assert.AreEqual(0, facade.GetDataReader().Read(new byte[1], 0, 1), "Expected no data from facade, but got some");
        }

        /*
         * Test that attempting to call BytesMessage#BodyLength on a received message after calling
         * BytesMessage#ClearBody causes MessageNotReadableException to be thrown due to being write-only.
         */
        [Test]
        public void TestGetBodyLengthOnClearedReceivedMessageThrowsMessageNotReadableException()
        {
            byte[] content = Encoding.UTF8.GetBytes("myBytes");
            NmsTestBytesMessageFacade facade = new NmsTestBytesMessageFacade(content);

            NmsBytesMessage bytesMessage = new NmsBytesMessage(facade);
            bytesMessage.OnDispatch();

            Assert.AreEqual(content.Length, bytesMessage.BodyLength, "Unexpected message length");
            bytesMessage.ClearBody();

            try
            {
                long bodyLength = bytesMessage.BodyLength;
                Assert.Fail("expected exception to be thrown");
            }
            catch (MessageNotReadableException)
            {
                // expected
            }
        }

        /*
         * Test that calling BytesMessage#Reset causes a write-only
         * message to become read-only
         */
        [Test]
        public void TestResetOnReceivedBytesMessageResetsMarker()
        {
            byte[] content = Encoding.UTF8.GetBytes("myBytes");
            NmsTestBytesMessageFacade facade = new NmsTestBytesMessageFacade(content);

            NmsBytesMessage bytesMessage = new NmsBytesMessage(facade);
            bytesMessage.OnDispatch();

            // retrieve a few bytes, check they match the first few expected bytes
            byte[] partialBytes = new byte[3];
            bytesMessage.ReadBytes(partialBytes);
            byte[] partialOriginalBytes = content.Take(3).ToArray();
            CollectionAssert.AreEqual(partialOriginalBytes, partialBytes);

            bytesMessage.Reset();

            // retrieve all the expected bytes, check they match
            byte[] resetBytes = new byte[content.Length];
            bytesMessage.ReadBytes(resetBytes);
            CollectionAssert.AreEqual(content, resetBytes);
        }

        /*
         * Test that calling BytesMessage#Reset on a new message which has been populated
         * causes the marker to be reset and makes the message read-only
         */
        [Test]
        public void TestResetOnNewlyPopulatedBytesMessageResetsMarkerAndMakesReadable()
        {
            byte[] content = Encoding.UTF8.GetBytes("myBytes");
            NmsTestBytesMessageFacade facade = new NmsTestBytesMessageFacade(content);

            NmsBytesMessage bytesMessage = new NmsBytesMessage(facade);

            Assert.False(bytesMessage.IsReadOnlyBody, "Message should be writable");
            bytesMessage.WriteBytes(content);
            bytesMessage.Reset();
            Assert.True(bytesMessage.IsReadOnlyBody, "Message should not be writable");

            // retrieve the bytes, check they match
            byte[] resetBytes = new byte[content.Length];
            bytesMessage.ReadBytes(resetBytes);
            CollectionAssert.AreEqual(content, resetBytes);
        }

        /*
         * Verify that nothing is read when BytesMessage#ReadBytes(byte[])} is
         * called with a zero length destination array.
         */
        [Test]
        public void TestReadBytesWithZeroLengthDestination()
        {
            NmsBytesMessage bytesMessage = factory.CreateBytesMessage();
            bytesMessage.Reset();
            Assert.AreEqual(0, bytesMessage.ReadBytes(new byte[0]), "Did not expect any bytes to be read");
        }

        /*
         * Verify that when BytesMessage#ReadBytes(byte[], int)} is called
         * with a negative length that an IndexOutOfRangeException is thrown.
         */
        [Test]
        public void TestReadBytesWithNegativeLengthThrowsIOORE()
        {
            NmsBytesMessage bytesMessage = factory.CreateBytesMessage();
            bytesMessage.Reset();
            Assert.Throws<IndexOutOfRangeException>(() => bytesMessage.ReadBytes(new byte[0], -1));
        }

        /*
         * Verify that when BytesMessage#ReadBytes(byte[], int)} is called
         * with a length that is greater than the size of the provided array,
         * an IndexOutOfRangeException is thrown.
         */
        [Test]
        public void TestReadBytesWithLengthGreatThanArraySizeThrowsIOORE()
        {
            NmsBytesMessage bytesMessage = factory.CreateBytesMessage();
            bytesMessage.Reset();
            Assert.Throws<IndexOutOfRangeException>(() => bytesMessage.ReadBytes(new byte[0], 2));
        }

        [Test]
        public void TestWriteObjectWithNullThrowsANE()
        {
            NmsBytesMessage bytesMessage = factory.CreateBytesMessage();
            Assert.Throws<ArgumentNullException>(() => bytesMessage.WriteObject(null));
        }

        [Test]
        public void TestWriteObjectWithIllegalTypeThrowsMFE()
        {
            NmsBytesMessage bytesMessage = factory.CreateBytesMessage();
            Assert.Throws<MessageFormatException>(() => bytesMessage.WriteObject(new object()));
        }

        [Test]
        public void TestGetBodyLength()
        {
            NmsBytesMessage bytesMessage = factory.CreateBytesMessage();
            int len = 10;
            for (int i = 0; i < len; i++)
            {
                bytesMessage.WriteInt64(5L);
            }

            bytesMessage.Reset();
            Assert.True(bytesMessage.BodyLength == len * 8);
        }

        [Test]
        public void TestSetGetContent()
        {
            byte[] content = Encoding.UTF8.GetBytes("myBytes");
            NmsBytesMessage msg = factory.CreateBytesMessage();
            msg.Content = content;
            msg.Reset();
            CollectionAssert.AreEqual(content, msg.Content);
        }

        [Test]
        public void TestReadBoolean()
        {
            NmsBytesMessage msg = factory.CreateBytesMessage();
            msg.WriteBoolean(true);
            msg.Reset();
            Assert.True(msg.ReadBoolean());
        }

        [Test]
        public void TestReadByte()
        {
            NmsBytesMessage msg = factory.CreateBytesMessage();
            msg.WriteByte(2);
            msg.Reset();
            Assert.AreEqual(2, msg.ReadByte());
        }

        [Test]
        public void TestReadShort()
        {
            NmsBytesMessage msg = factory.CreateBytesMessage();
            msg.WriteInt16(3000);
            msg.Reset();
            Assert.AreEqual(3000, msg.ReadInt16());
        }

        [Test]
        public void TestReadChar()
        {
            NmsBytesMessage msg = factory.CreateBytesMessage();
            msg.WriteChar('a');
            msg.Reset();
            Assert.AreEqual('a', msg.ReadChar());
        }

        [Test]
        public void TestReadInt()
        {
            NmsBytesMessage msg = factory.CreateBytesMessage();
            msg.WriteInt32(3000);
            msg.Reset();
            Assert.AreEqual(3000, msg.ReadInt32());
        }

        [Test]
        public void TestReadLong()
        {
            NmsBytesMessage msg = factory.CreateBytesMessage();
            msg.WriteInt64(3000);
            msg.Reset();
            Assert.AreEqual(3000, msg.ReadInt64());
        }

        [Test]
        public void TestReadFloat()
        {
            NmsBytesMessage msg = factory.CreateBytesMessage();
            msg.WriteSingle(3.3f);
            msg.Reset();
            Assert.AreEqual(3.3f, msg.ReadSingle());
        }

        [Test]
        public void TestReadDouble()
        {
            NmsBytesMessage msg = factory.CreateBytesMessage();
            msg.WriteDouble(3.3d);
            msg.Reset();
            Assert.AreEqual(3.3d, msg.ReadDouble());
        }

        [Test]
        public void TestReadUTF()
        {
            NmsBytesMessage msg = factory.CreateBytesMessage();
            string str = "this is a test";
            msg.WriteString(str);
            msg.Reset();
            Assert.AreEqual(str, msg.ReadString());
        }

        [Test]
        public void TestReadBytesArray()
        {
            NmsBytesMessage msg = factory.CreateBytesMessage();
            byte[] data = new byte[50];
            for (int i = 0; i < data.Length; i++)
            {
                data[i] = (byte) i;
            }

            msg.WriteBytes(data);
            msg.Reset();
            byte[] test = new byte[data.Length];
            msg.ReadBytes(test);
            CollectionAssert.AreEqual(data, test);
        }

        [Test]
        public void TestWriteObject()
        {
            NmsBytesMessage msg = factory.CreateBytesMessage();
            msg.WriteObject("fred");
            msg.WriteObject(true);
            msg.WriteObject('a');
            msg.WriteObject((byte) 1);
            msg.WriteObject((short) 3);
            msg.WriteObject((int) 3);
            msg.WriteObject(300L);
            msg.WriteObject(3.3F);
            msg.WriteObject(3.3D);
            msg.WriteObject(new byte[3]);

            Assert.Throws<MessageFormatException>(() => msg.WriteObject(new object()));
        }

        [Test]
        public void TestClearBodyOnNewMessage()
        {
            NmsBytesMessage msg = factory.CreateBytesMessage();
            msg.WriteInt32(1);
            msg.ClearBody();
            Assert.False(msg.IsReadOnlyBody);
            msg.Reset();
            Assert.AreEqual(0, msg.BodyLength);
        }

        [Test]
        public void TestReset()
        {
            NmsBytesMessage msg = factory.CreateBytesMessage();
            msg.WriteDouble(3.3D);
            msg.WriteInt64(311);
            
            msg.Reset();
            
            Assert.True(msg.IsReadOnlyBody);
            Assert.AreEqual(3.3D, msg.ReadDouble());
            Assert.AreEqual(311, msg.ReadInt64());

            Assert.Throws<MessageNotWriteableException>(() => msg.WriteInt32(33));
        }

        [Test]
        public void TestReadOnlyBody()
        {
            NmsBytesMessage message = factory.CreateBytesMessage();
            message.WriteBoolean(true);
            message.WriteByte((byte) 1);
            message.WriteBytes(new byte[1]);
            message.WriteBytes(new byte[3], 0, 2);
            message.WriteChar('a');
            message.WriteDouble(1.5);
            message.WriteSingle((float) 1.5);
            message.WriteInt32(1);
            message.WriteInt64(1);
            message.WriteObject("stringobj");
            message.WriteSingle((short) 1);
            message.WriteString("utfstring");
            
            message.Reset();
            
            message.ReadBoolean();
            message.ReadByte();
            message.ReadBytes(new byte[1]);
            message.ReadBytes(new byte[2], 2);
            message.ReadChar();
            message.ReadDouble();
            message.ReadSingle();
            message.ReadInt32();
            message.ReadInt64();
            message.ReadString();
            message.ReadSingle();
            message.ReadString();

            Assert.Throws<MessageNotWriteableException>(() => message.WriteBoolean(true));
            Assert.Throws<MessageNotWriteableException>(() => message.WriteByte(1));
            Assert.Throws<MessageNotWriteableException>(() => message.WriteBytes(new byte[1]));
            Assert.Throws<MessageNotWriteableException>(() => message.WriteBytes(new byte[3], 0, 2));
            Assert.Throws<MessageNotWriteableException>(() => message.WriteChar('a'));
            Assert.Throws<MessageNotWriteableException>(() => message.WriteDouble(1.5));
            Assert.Throws<MessageNotWriteableException>(() => message.WriteSingle(1.5f));
            Assert.Throws<MessageNotWriteableException>(() => message.WriteInt32(1));
            Assert.Throws<MessageNotWriteableException>(() => message.WriteInt64(1));
            Assert.Throws<MessageNotWriteableException>(() => message.WriteObject("utfstring"));
        }

        [Test]
        public void TestWriteOnlyBody()
        {
            NmsBytesMessage message = factory.CreateBytesMessage();
            message.ClearBody();
            
            message.WriteBoolean(true);
            message.WriteByte((byte) 1);
            message.WriteBytes(new byte[1]);
            message.WriteBytes(new byte[3], 0, 2);
            message.WriteChar('a');
            message.WriteDouble(1.5);
            message.WriteSingle((float) 1.5);
            message.WriteInt32(1);
            message.WriteInt64(1);
            message.WriteObject("stringobj");
            message.WriteSingle((short) 1);
            message.WriteString("utfstring");
            
            Assert.Throws<MessageNotReadableException>(() => message.ReadBoolean());
            Assert.Throws<MessageNotReadableException>(() => message.ReadByte());
            Assert.Throws<MessageNotReadableException>(() => message.ReadBytes(new byte[1]));
            Assert.Throws<MessageNotReadableException>(() => message.ReadBytes(new byte[2], 2));
            Assert.Throws<MessageNotReadableException>(() => message.ReadChar());
            Assert.Throws<MessageNotReadableException>(() => message.ReadDouble());
            Assert.Throws<MessageNotReadableException>(() => message.ReadSingle());
            Assert.Throws<MessageNotReadableException>(() => message.ReadInt32());
            Assert.Throws<MessageNotReadableException>(() => message.ReadInt64());
            Assert.Throws<MessageNotReadableException>(() => message.ReadString());
            Assert.Throws<MessageNotReadableException>(() => message.ReadInt16());
        }

        [Test]
        public void TestMessageCopy()
        {
            NmsBytesMessage message = factory.CreateBytesMessage();

            NmsBytesMessage copy = message.Copy() as NmsBytesMessage;
            Assert.IsNotNull(copy);
        }
    }
}