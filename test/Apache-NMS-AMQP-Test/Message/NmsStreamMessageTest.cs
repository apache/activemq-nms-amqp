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
using System.Globalization;
using System.Linq;
using System.Text;
using Apache.NMS;
using Apache.NMS.AMQP.Message;
using NMS.AMQP.Test.Message.Facade;
using NUnit.Framework;

namespace NMS.AMQP.Test.Message
{
    [TestFixture]
    public class NmsStreamMessageTest
    {
        private readonly TestMessageFactory factory = new TestMessageFactory();

        [Test]
        public void TestToString()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();
            Assert.True(streamMessage.ToString().StartsWith("NmsStreamMessage"));
        }

        [Test]
        public void TestReadWithEmptyStreamThrowsMEOFE()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();
            streamMessage.Reset();

            Assert.Throws<MessageEOFException>(() => streamMessage.ReadBoolean(), "Expected exception to be thrown as message has no content");
        }

        [Test]
        public void TestClearBodyOnNewMessageRemovesExistingValues()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();
            streamMessage.WriteBoolean(true);

            streamMessage.ClearBody();

            streamMessage.WriteBoolean(false);
            streamMessage.Reset();

            // check we get only the value added after the clear
            Assert.False(streamMessage.ReadBoolean(), "expected value added after the clear");

            Assert.Throws<MessageEOFException>(() => streamMessage.ReadBoolean(), "Expected exception to be thrown");
        }

        [Test]
        public void TestNewMessageIsWriteOnlyThrowsMNRE()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            Assert.Throws<MessageNotReadableException>(() => streamMessage.ReadBoolean(), "Expected exception to be thrown");
        }

        /// <summary>
        /// Verify the stream position is not incremented during illegal type conversion failure.
        /// This covers every read method except readObject (which doesn't do type conversion) and
        /// ReadBytes(), which is tested by TestIllegalTypeConvesionFailureDoesNotIncrementPosition2
        ///
        /// Write bytes, then deliberately try to retrieve them as illegal types, then check they can
        /// be successfully read. 
        /// </summary>
        [Test]
        public void TestIllegalTypeConversionFailureDoesNotIncrementPosition1()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            byte[] bytes = { 0, 255, 78 };

            streamMessage.WriteBytes(bytes);
            streamMessage.Reset();

            AssertGetStreamEntryThrowsMessageFormatException<bool>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<byte>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<short>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<char>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<int>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<long>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<float>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<double>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<string>(streamMessage);

            byte[] retrievedByteArray = new byte[bytes.Length];
            int readBytesLength = streamMessage.ReadBytes(retrievedByteArray);

            Assert.AreEqual(bytes.Length, readBytesLength, "Number of bytes read did not match original array length");
            CollectionAssert.AreEqual(bytes, retrievedByteArray, "Expected array to equal retrieved bytes");
            Assert.AreEqual(-1, streamMessage.ReadBytes(retrievedByteArray), "Expected completion return value");
        }

        [Test]
        public void TestIllegalTypeConversionFailureDoesNotIncrementPosition2()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            String stringVal = "myString";
            streamMessage.WriteString(stringVal);
            streamMessage.Reset();

            AssertGetStreamEntryThrowsMessageFormatException<byte[]>(streamMessage);

            Assert.AreEqual(stringVal, streamMessage.ReadString(), "Expected written string");
        }

        /// <summary>
        /// When a null stream entry is encountered, the accessor methods is type dependent and
        /// should either return null, throw NRE, or behave in the same fashion as
        /// <primitive>.Parse(string)
        ///
        /// Test that this is the case, and in doing show demonstrate that primitive type conversion
        /// failure does not increment the stream position, as shown by not hitting the end of the
        /// stream unexpectedly.
        /// </summary>
        [Test]
        public void TestNullStreamEntryResultsInExpectedBehaviour()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            streamMessage.WriteObject(null);
            streamMessage.Reset();

            // expect an NFE from the primitive integral, float, double, and char <type>.valueOf(null) conversions
            AssertGetStreamEntryThrowsNullReferenceException<byte>(streamMessage);
            AssertGetStreamEntryThrowsNullReferenceException<short>(streamMessage);
            AssertGetStreamEntryThrowsNullReferenceException<int>(streamMessage);
            AssertGetStreamEntryThrowsNullReferenceException<long>(streamMessage);
            AssertGetStreamEntryThrowsNullReferenceException<float>(streamMessage);
            AssertGetStreamEntryThrowsNullReferenceException<double>(streamMessage);
            AssertGetStreamEntryThrowsNullReferenceException<char>(streamMessage);

            // expect null
            Assert.Null(streamMessage.ReadObject());
            streamMessage.Reset(); // need to reset as read was a success
            Assert.Null(streamMessage.ReadString());
            streamMessage.Reset(); // need to reset as read was a success

            // expect completion value.
            Assert.AreEqual(-1, streamMessage.ReadBytes(new byte[1]));
            streamMessage.Reset(); // need to reset as read was a success

            // expect false
            Assert.False(streamMessage.ReadBoolean());
        }

        [Test]
        public void TestClearBodyAppliesCorrectState()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            streamMessage.WriteObject(2);
            streamMessage.ClearBody();
            Assert.False(streamMessage.IsReadOnlyBody);
            streamMessage.WriteObject(2);

            Assert.Throws<MessageNotReadableException>(() => streamMessage.ReadObject());
        }

        [Test]
        public void TestResetAppliesCorrectState()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            streamMessage.WriteDouble(24.5);
            streamMessage.WriteInt64(311);

            streamMessage.Reset();

            Assert.True(streamMessage.IsReadOnlyBody);
            Assert.AreEqual(streamMessage.ReadDouble(), 24.5);
            Assert.AreEqual(streamMessage.ReadInt64(), 311);

            Assert.Throws<MessageNotWriteableException>(() => streamMessage.WriteInt32(33));
        }

        // ======= object =========

        [Test]
        public void TestWriteObjectWithIllegalTypeThrowsMFE()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            Assert.Throws<MessageFormatException>(() => streamMessage.WriteObject(1m));
        }

        [Test]
        public void TestWriteReadObject()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            Object nullEntryValue = null;
            bool boolEntryValue = false;
            byte byteEntryValue = 1;
            short shortEntryValue = 2;
            int intEntryValue = 3;
            long longEntryValue = 4;
            float floatEntryValue = 5.01f;
            double doubleEntryValue = 6.01d;
            string stringEntryValue = "string";
            char charEntryValue = 'c';
            byte[] bytes = { 1, 170, 65 };

            streamMessage.WriteObject(nullEntryValue);
            streamMessage.WriteObject(boolEntryValue);
            streamMessage.WriteObject(byteEntryValue);
            streamMessage.WriteObject(shortEntryValue);
            streamMessage.WriteObject(intEntryValue);
            streamMessage.WriteObject(longEntryValue);
            streamMessage.WriteObject(floatEntryValue);
            streamMessage.WriteObject(doubleEntryValue);
            streamMessage.WriteObject(stringEntryValue);
            streamMessage.WriteObject(charEntryValue);
            streamMessage.WriteObject(bytes);

            streamMessage.Reset();

            Assert.AreEqual(nullEntryValue, streamMessage.ReadObject(), "Got unexpected value from stream");
            Assert.AreEqual(boolEntryValue, streamMessage.ReadObject(), "Got unexpected value from stream");
            Assert.AreEqual(byteEntryValue, streamMessage.ReadObject(), "Got unexpected value from stream");
            Assert.AreEqual(shortEntryValue, streamMessage.ReadObject(), "Got unexpected value from stream");
            Assert.AreEqual(intEntryValue, streamMessage.ReadObject(), "Got unexpected value from stream");
            Assert.AreEqual(longEntryValue, streamMessage.ReadObject(), "Got unexpected value from stream");
            Assert.AreEqual(floatEntryValue, streamMessage.ReadObject(), "Got unexpected value from stream");
            Assert.AreEqual(doubleEntryValue, streamMessage.ReadObject(), "Got unexpected value from stream");
            Assert.AreEqual(stringEntryValue, streamMessage.ReadObject(), "Got unexpected value from stream");
            Assert.AreEqual(charEntryValue, streamMessage.ReadObject(), "Got unexpected value from stream");
            CollectionAssert.AreEqual(bytes, (byte[])streamMessage.ReadObject(), "Got unexpected value from stream");
        }

        // ======= bytes =========
        [Test, Description("Write bytes, then retrieve them as all of the legal type combinations")]
        public void TestWriteBytesReadLegal()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();
            byte[] value = { 0, 255, 78 };

            streamMessage.WriteBytes(value);
            streamMessage.Reset();

            byte[] dest = new byte[value.Length];

            int readBytesLength = streamMessage.ReadBytes(dest);
            Assert.AreEqual(value.Length, readBytesLength, "Number of bytes read did not match expectation");
            CollectionAssert.AreEqual(value, dest, "value not as expected");
        }

        [Test, Description("Write bytes, then retrieve them as all of the illegal type combinations to verify it" +
                           "fails as expected")]
        public void TestWriteBytesReadIllegal()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();
            byte[] value = { 0, 255, 78 };

            streamMessage.WriteBytes(value);
            streamMessage.Reset();

            AssertGetStreamEntryThrowsMessageFormatException<bool>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<byte>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<short>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<char>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<int>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<long>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<float>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<double>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<string>(streamMessage);
        }

        [Test]
        public void TestReadBytesWithNullSignalsCompletion()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();
            streamMessage.WriteObject(null);

            streamMessage.Reset();

            Assert.AreEqual(-1, streamMessage.ReadBytes(new byte[1]), "Expected immediate completion signal");
        }

        [Test]
        public void TestReadBytesWithZeroLengthSource()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();
            streamMessage.WriteBytes(new byte[0]);

            streamMessage.Reset();

            byte[] fullRetrievedBytes = new byte[1];

            Assert.AreEqual(0, streamMessage.ReadBytes(fullRetrievedBytes), "Expected no bytes to be read, as none were written");
        }

        [Test]
        public void TestReadBytesWithZeroLengthDestination()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            byte[] bytes = { 11, 44, 99 };
            streamMessage.WriteBytes(bytes);

            streamMessage.Reset();

            byte[] zeroDestination = new byte[0];
            byte[] fullRetrievedBytes = new byte[bytes.Length];

            Assert.AreEqual(0, streamMessage.ReadBytes(zeroDestination), "Expected no bytes to be read");
            Assert.AreEqual(bytes.Length, streamMessage.ReadBytes(fullRetrievedBytes), "Expected all bytes to be read");
            CollectionAssert.AreEqual(bytes, fullRetrievedBytes, "Expected arrays to be equal");
            Assert.AreEqual(-1, streamMessage.ReadBytes(zeroDestination), "Expected completion signal");
        }

        [Test]
        public void TestReadObjectForBytesReturnsNewArray()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            byte[] bytes = { 11, 44, 99 };
            streamMessage.WriteBytes(bytes);

            streamMessage.Reset();

            byte[] retrievedBytes = (byte[])streamMessage.ReadObject();

            Assert.AreNotSame(bytes, retrievedBytes, "Expected different array objects");
            CollectionAssert.AreEqual(bytes, retrievedBytes, "Expected arrays to be equal");
        }

        [Test]
        public void TestReadBytesFullWithUndersizedDestinationArrayUsingMultipleReads()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            byte[] bytes = { 3, 78, 253, 26, 8 };
            Assert.AreEqual(1, bytes.Length % 2, "bytes should be odd length");

            int undersizedLength = 2;
            int remaining = 1;

            streamMessage.WriteBytes(bytes);
            streamMessage.Reset();

            byte[] undersizedDestination = new byte[undersizedLength];
            byte[] fullRetrievedBytes = new byte[bytes.Length];

            Assert.AreEqual(undersizedLength, streamMessage.ReadBytes(undersizedDestination), "Number of bytes read did not match destination array length");
            int read = undersizedLength;
            Array.Copy(undersizedDestination, 0, fullRetrievedBytes, 0, undersizedLength);
            Assert.AreEqual(undersizedLength, streamMessage.ReadBytes(undersizedDestination), "Number of bytes read did not match destination array length");
            Array.Copy(undersizedDestination, 0, fullRetrievedBytes, read, undersizedLength);
            read += undersizedLength;
            Assert.AreEqual(remaining, streamMessage.ReadBytes(undersizedDestination), "Number of bytes read did not match expectation");
            Array.Copy(undersizedDestination, 0, fullRetrievedBytes, read, remaining);
            read += remaining;
            CollectionAssert.AreEqual(bytes, fullRetrievedBytes, "Expected array to equal retrieved bytes");
        }

        [Test]
        public void TestReadBytesFullWithPreciselySizedDestinationArray()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            byte[] bytes = { 11, 44, 99 };
            streamMessage.WriteBytes(bytes);

            streamMessage.Reset();

            byte[] retrievedByteArray = new byte[bytes.Length];
            int readBytesLength = streamMessage.ReadBytes(retrievedByteArray);

            Assert.AreEqual(bytes.Length, readBytesLength, "Number of bytes read did not match original array length");
            CollectionAssert.AreEqual(bytes, retrievedByteArray, "Expected array to equal retrieved bytes");
            Assert.AreEqual(-1, streamMessage.ReadBytes(retrievedByteArray), "Expected completion return value");
        }

        [Test]
        public void TestReadBytesFullWithOversizedDestinationArray()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            byte[] bytes = { 4, 115, 255 };
            streamMessage.WriteBytes(bytes);

            streamMessage.Reset();

            byte[] oversizedDestination = new byte[bytes.Length + 1];
            int readBytesLength = streamMessage.ReadBytes(oversizedDestination);

            Assert.AreEqual(bytes.Length, readBytesLength, "Number of bytes read did not match original array length");
            CollectionAssert.AreEqual(bytes, oversizedDestination.Take(readBytesLength), "Expected array subset to equal retrieved bytes");
        }

        [Test]
        public void TestReadObjectAfterPartialReadBytesThrowsMFE()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            byte[] bytes = { 4, 44, 99 };
            streamMessage.WriteBytes(bytes);

            streamMessage.Reset();

            // start reading via readBytes
            int partialLength = 2;
            byte[] retrievedByteArray = new byte[partialLength];
            int readBytesLength = streamMessage.ReadBytes(retrievedByteArray);

            Assert.AreEqual(partialLength, readBytesLength);
            CollectionAssert.AreEqual(bytes.Take(partialLength), retrievedByteArray, "Expected array subset to equal retrieved bytes");

            // check that using readObject does not return the full/remaining bytes as a new array

            Assert.Throws<MessageFormatException>(() => streamMessage.ReadObject());

            // finish reading via reaBytes to ensure it can be completed
            readBytesLength = streamMessage.ReadBytes(retrievedByteArray);
            Assert.AreEqual(bytes.Length - partialLength, readBytesLength);
            CollectionAssert.AreEqual(bytes.Skip(partialLength).Take(bytes.Length), retrievedByteArray.Take(readBytesLength), "Expected array subset to equal retrieved bytes");
        }

        [Test]
        public void TestWriteBytesWithOffsetAndLength()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();
            byte[] orig = Encoding.ASCII.GetBytes("myBytesAll");

            // extract the segment containing 'Bytes'
            int offset = 2;
            int length = 5;
            byte[] segment = orig.Skip(offset).Take(length).ToArray();

            // set the same section from the original bytes
            streamMessage.WriteBytes(orig, offset, length);
            streamMessage.Reset();

            byte[] retrieved = (byte[])streamMessage.ReadObject();

            // verify the retrieved bytes from the stream equal the segment but are not the same
            Assert.AreNotSame(orig, retrieved);
            Assert.AreNotSame(segment, retrieved);
            CollectionAssert.AreEqual(segment, retrieved);
        }

        // ========= boolean ========
        [Test]
        public void TestWriteReadBoolean()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();
            bool value = true;

            streamMessage.WriteBoolean(value);
            streamMessage.Reset();

            Assert.AreEqual(value, streamMessage.ReadBoolean(), "Value not as expected");
        }

        [Test, Description("Set a boolean, then retrieve it as all of the legal type combinations to verify it is" +
                           "parsed correctly")]
        public void TestWriteBooleanReadLegal()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            bool value = true;

            streamMessage.WriteBoolean(value);
            streamMessage.Reset();

            AssertGetStreamEntryEquals<bool>(streamMessage, true, value);
            AssertGetStreamEntryEquals<string>(streamMessage, true, value.ToString());
        }

        [Test, Description("Set a boolean, then retrieve it as all of the illegal type combinations to verify it" +
                           "fails as expected")]
        public void TestSetBooleanGetIllegal()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            bool value = true;

            streamMessage.WriteBoolean(value);
            streamMessage.Reset();

            AssertGetStreamEntryThrowsMessageFormatException<byte>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<short>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<char>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<int>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<long>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<float>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<double>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<byte[]>(streamMessage);
        }

        // ========= string ========

        [Test]
        public void TestWriteReadString()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            string value = "myString";

            streamMessage.WriteString(value);
            streamMessage.Reset();

            Assert.AreEqual(value, streamMessage.ReadString(), "Value not as expected");
        }

        [Test, Description("Set a string, then retrieve it as all of the legal type combinations to verify it is parsed correctly")]
        public void TestWriteStringReadLegal()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            string integralValue = Convert.ToString(byte.MaxValue);
            streamMessage.WriteString(integralValue);
            streamMessage.Reset();

            AssertGetStreamEntryEquals<string>(streamMessage, true, integralValue);
            AssertGetStreamEntryEquals<bool>(streamMessage, true, false);
            AssertGetStreamEntryEquals<byte>(streamMessage, true, byte.MaxValue);

            streamMessage.ClearBody();
            integralValue = Convert.ToString(short.MaxValue);
            streamMessage.WriteString(integralValue);
            streamMessage.Reset();

            AssertGetStreamEntryEquals<short>(streamMessage, true, short.MaxValue);

            streamMessage.ClearBody();
            integralValue = Convert.ToString(int.MaxValue);
            streamMessage.WriteString(integralValue);
            streamMessage.Reset();

            AssertGetStreamEntryEquals<int>(streamMessage, true, int.MaxValue);

            streamMessage.ClearBody();
            integralValue = Convert.ToString(long.MaxValue);
            streamMessage.WriteString(integralValue);
            streamMessage.Reset();

            AssertGetStreamEntryEquals<long>(streamMessage, true, long.MaxValue);

            streamMessage.ClearBody();
            string fpValue = Convert.ToString(float.MaxValue, CultureInfo.InvariantCulture);
            streamMessage.WriteString(fpValue);
            streamMessage.Reset();

            AssertGetStreamEntryEquals<float>(streamMessage, true, float.Parse(fpValue));

            // TODO: make following pass
            //AssertGetStreamEntryEquals<double>(streamMessage, true, double.Parse(fpValue));
        }

        [Test]
        public void TestWriteStringReadIllegal()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            string stringValue = "myString";

            streamMessage.WriteString(stringValue);
            streamMessage.Reset();

            AssertGetStreamEntryThrowsMessageFormatException<char>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<byte[]>(streamMessage);
        }

        [Test]
        public void TestReadBigString()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            StringBuilder stringBuilder = new StringBuilder(1024 * 1024);
            for (int i = 0; i < 1024 * 1024; i++)
                stringBuilder.Append('a' + i % 26);

            string bigString = stringBuilder.ToString();
            streamMessage.WriteString(bigString);
            streamMessage.Reset();
            Assert.AreEqual(bigString, streamMessage.ReadString());
        }

        // ========= byte ========

        [Test]
        public void TestWriteReadByte()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            byte value = 6;

            streamMessage.WriteByte(value);
            streamMessage.Reset();

            Assert.AreEqual(value, streamMessage.ReadByte(), "Value not as expected");
        }

        [Test, Description("Set a byte, then retrieve it as all of the legal type combinations to verify it is parsed correctly")]
        public void TestWriteByteReadLegal()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();
            byte value = 6;

            streamMessage.WriteByte(value);
            streamMessage.Reset();

            AssertGetStreamEntryEquals<byte>(streamMessage, true, value);
            AssertGetStreamEntryEquals<short>(streamMessage, true, value);
            AssertGetStreamEntryEquals<int>(streamMessage, true, value);
            AssertGetStreamEntryEquals<long>(streamMessage, true, value);
            AssertGetStreamEntryEquals<string>(streamMessage, true, value.ToString());
        }

        [Test, Description("Set a byte, then retrieve it as all of the illegal type combinations to verify it fails as expected")]
        public void TestWriteByteReadIllegal()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();
            byte value = 6;

            streamMessage.WriteByte(value);
            streamMessage.Reset();

            AssertGetStreamEntryThrowsMessageFormatException<bool>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<char>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<float>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<double>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<byte[]>(streamMessage);
        }

        // ========= short ========

        [Test]
        public void TestWriteReadShort()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            short value = 6;

            streamMessage.WriteInt16(value);
            streamMessage.Reset();

            Assert.AreEqual(value, streamMessage.ReadInt16(), "Value not as expected");
        }

        [Test, Description("Set a short, then retrieve it as all of the legal type combinations to verify it is parsed correctly")]
        public void TestWriteShortReadLegal()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            short value = 302;

            streamMessage.WriteInt16(value);
            streamMessage.Reset();

            AssertGetStreamEntryEquals<short>(streamMessage, true, value);
            AssertGetStreamEntryEquals<int>(streamMessage, true, value);
            AssertGetStreamEntryEquals<long>(streamMessage, true, value);
            AssertGetStreamEntryEquals<string>(streamMessage, true, value.ToString());
        }

        [Test, Description("Set a short, then retrieve it as all of the illegal type combinations to verify it fails as expected")]
        public void TestWriteShortReadIllegal()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            short value = 302;

            streamMessage.WriteInt16(value);
            streamMessage.Reset();

            AssertGetStreamEntryThrowsMessageFormatException<bool>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<byte>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<char>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<float>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<double>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<byte[]>(streamMessage);
        }

        // ========= char ========

        [Test]
        public void TestWriteReadChar()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            char value = 'c';

            streamMessage.WriteChar(value);
            streamMessage.Reset();

            Assert.AreEqual(value, streamMessage.ReadChar(), "Value not as expected correctly");
        }

        [Test, Description("Set a char, then retrieve it as all of the legal type combinations to verify it is parsed correctly")]
        public void TestWriteCharReadLegal()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            char value = 'c';

            streamMessage.WriteChar(value);
            streamMessage.Reset();

            AssertGetStreamEntryEquals<char>(streamMessage, true, value);
            AssertGetStreamEntryEquals<string>(streamMessage, true, value.ToString());
        }

        [Test, Description("Set a char, then retrieve it as all of the illegal type combinations to verify it fails as expected")]
        public void TestWriteCharReadIllegal()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            char value = 'c';

            streamMessage.WriteChar(value);
            streamMessage.Reset();

            AssertGetStreamEntryThrowsMessageFormatException<bool>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<byte>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<short>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<int>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<long>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<float>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<double>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<byte[]>(streamMessage);
        }

        // ========= int ========

        [Test]
        public void TestWriteReadInt()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            int value = int.MaxValue;

            streamMessage.WriteInt32(value);
            streamMessage.Reset();

            Assert.AreEqual(value, streamMessage.ReadInt32(), "Value not as expected");
        }

        [Test, Description("Set an int, then retrieve it as all of the legal type combinations to verify it is parsed correctly")]
        public void TestWriteIntReadLegal()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            int value = int.MaxValue;

            streamMessage.WriteInt32(value);
            streamMessage.Reset();

            AssertGetStreamEntryEquals<int>(streamMessage, true, value);
            AssertGetStreamEntryEquals<long>(streamMessage, true, value);
            AssertGetStreamEntryEquals<string>(streamMessage, true, value.ToString());
        }

        [Test, Description("Set an int, then retrieve it as all of the illegal type combinations to verify it fails as expected")]
        public void TestWriteIntReadIllegal()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            int value = int.MaxValue;

            streamMessage.WriteInt32(value);
            streamMessage.Reset();

            AssertGetStreamEntryThrowsMessageFormatException<bool>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<byte>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<short>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<char>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<float>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<double>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<byte[]>(streamMessage);
        }

        // ========= long ========

        [Test]
        public void TestWriteReadLong()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            long value = long.MaxValue;

            streamMessage.WriteInt64(value);
            streamMessage.Reset();

            Assert.AreEqual(value, streamMessage.ReadInt64(), "Value not as expected");
        }

        [Test, Description("Set a long, then retrieve it as all of the legal type combinations to verify it is parsed correctly")]
        public void TestWriteLongReadLegal()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            long value = long.MaxValue;

            streamMessage.WriteInt64(value);
            streamMessage.Reset();

            AssertGetStreamEntryEquals<long>(streamMessage, true, value);
            AssertGetStreamEntryEquals<string>(streamMessage, true, value.ToString());
        }

        [Test, Description("Set a long, then retrieve it as all of the illegal type combinations to verify it fails as expected")]
        public void TestWriteLongReadIllegal()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            long value = long.MaxValue;

            streamMessage.WriteInt64(value);
            streamMessage.Reset();

            AssertGetStreamEntryThrowsMessageFormatException<bool>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<byte>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<short>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<char>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<int>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<float>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<double>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<byte[]>(streamMessage);
        }

        // ========= float ========

        [Test]
        public void TestWriteReadFloat()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            float value = float.MaxValue;

            streamMessage.WriteSingle(value);
            streamMessage.Reset();

            Assert.AreEqual(value, streamMessage.ReadSingle(), "Value not as expected");
        }

        [Test, Description("Set a float, then retrieve it as all of the legal type combinations to verify it is parsed correctly")]
        public void TestWriteFloatReadLegal()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            float value = float.MaxValue;

            streamMessage.WriteSingle(value);
            streamMessage.Reset();

            AssertGetStreamEntryEquals<float>(streamMessage, true, value);
            AssertGetStreamEntryEquals<double>(streamMessage, true, value);
            AssertGetStreamEntryEquals<string>(streamMessage, true, value.ToString(CultureInfo.InvariantCulture));
        }

        [Test, Description("Set a float, then retrieve it as all of the illegal type combinations to verify it fails as expected")]
        public void TestWriteFloatReadIllegal()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            float value = float.MaxValue;

            streamMessage.WriteSingle(value);
            streamMessage.Reset();

            AssertGetStreamEntryThrowsMessageFormatException<bool>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<byte>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<short>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<char>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<long>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<byte[]>(streamMessage);
        }

        // ========= double ========

        [Test]
        public void TestWriteReadDouble()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            double value = double.MaxValue;

            streamMessage.WriteDouble(value);
            streamMessage.Reset();

            Assert.AreEqual(value, streamMessage.ReadDouble(), "Value not as expected");
        }

        [Test, Description("Set a double, then retrieve it as all of the legal type combinations to verify it is parsed correctly")]
        public void TestWriteDoubleReadLegal()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            double value = double.MaxValue;

            streamMessage.WriteDouble(value);
            streamMessage.Reset();

            AssertGetStreamEntryEquals<double>(streamMessage, true, value);
            AssertGetStreamEntryEquals<string>(streamMessage, true, value.ToString(CultureInfo.InvariantCulture));
        }

        [Test, Description("Set a double, then retrieve it as all of the illegal type combinations to verify it fails as expected")]
        public void TestWriteDoubleReadIllegal()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            double value = double.MaxValue;

            streamMessage.WriteDouble(value);
            streamMessage.Reset();

            AssertGetStreamEntryThrowsMessageFormatException<bool>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<byte>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<short>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<char>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<int>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<long>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<float>(streamMessage);
            AssertGetStreamEntryThrowsMessageFormatException<byte[]>(streamMessage);
        }

        // ========= read failures ========

        [Test]
        public void TestReadBytesWithNullArrayThrowsArgumentNullException()
        {
            NmsStreamMessage streamMessage = factory.CreateStreamMessage();

            streamMessage.Reset();

            Assert.Throws<ArgumentNullException>(() => streamMessage.ReadBytes(null));
        }

        [Test]
        public void TestReadObjectGetsInvalidObjectThrowsMFE()
        {
            NmsTestStreamMessageFacade facade = new NmsTestStreamMessageFacade();
            facade.Put(new Uri("test://test"));
            NmsStreamMessage streamMessage = new NmsStreamMessage(facade);
            streamMessage.Reset();

            Assert.Throws<MessageFormatException>(() => streamMessage.ReadObject());
        }
        
        [Test]
        public void TestMessageCopy()
        {
            NmsStreamMessage message = factory.CreateStreamMessage();

            NmsStreamMessage copy = message.Copy() as NmsStreamMessage;
            Assert.IsNotNull(copy);
        }

        private void AssertGetStreamEntryThrowsMessageFormatException<T>(NmsStreamMessage testMessage)
        {

            Assert.Throws<MessageFormatException>(() => GetStreamEntryUsingTypeMethod(testMessage, typeof(T), new byte[0]), "expected exception to be thrown");
        }

        private void AssertGetStreamEntryThrowsNullReferenceException<T>(NmsStreamMessage testMessage)
        {
            Assert.Throws<NullReferenceException>(() => GetStreamEntryUsingTypeMethod(testMessage, typeof(T), new byte[0]), "expected exception to be thrown");
        }

        private void AssertGetStreamEntryEquals<T>(NmsStreamMessage testMessage, bool resetStreamAfter, object expectedValue)
        {
            object actualValue = GetStreamEntryUsingTypeMethod(testMessage, typeof(T), null);
            Assert.AreEqual(expectedValue, actualValue);

            if (resetStreamAfter)
            {
                testMessage.Reset();
            }
        }

        private object GetStreamEntryUsingTypeMethod(NmsStreamMessage testMessage, Type type, byte[] destination)
        {
            if (type == typeof(bool))
                return testMessage.ReadBoolean();
            if (type == typeof(byte))
                return testMessage.ReadByte();
            if (type == typeof(char))
                return testMessage.ReadChar();
            if (type == typeof(short))
                return testMessage.ReadInt16();
            if (type == typeof(int))
                return testMessage.ReadInt32();
            if (type == typeof(long))
                return testMessage.ReadInt64();
            if (type == typeof(float))
                return testMessage.ReadSingle();
            if (type == typeof(double))
                return testMessage.ReadDouble();
            if (type == typeof(string))
                return testMessage.ReadString();
            if (type == typeof(byte[]))
                return testMessage.ReadBytes(destination);

            throw new Exception("Unexpected entry type class");
        }
    }
}