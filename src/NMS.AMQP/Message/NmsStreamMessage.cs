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
using System.Runtime.InteropServices.ComTypes;
using Apache.NMS.AMQP.Message.Facade;
using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP.Message
{
    public class NmsStreamMessage : NmsMessage, IStreamMessage
    {
        private const int NO_BYTES_IN_FLIGHT = -1;
        private readonly INmsStreamMessageFacade facade;

        private int remainingBytes = NO_BYTES_IN_FLIGHT;
        private byte[] bytes;

        public NmsStreamMessage(INmsStreamMessageFacade facade) : base(facade)
        {
            this.facade = facade;
        }

        public override string ToString()
        {
            return "NmsStreamMessage { " + facade + " }";
        }

        public bool ReadBoolean()
        {
            CheckWriteOnlyBody();
            CheckBytesInFlight();

            bool result;
            object value = facade.Peek();

            if (value is bool boolResult)
                result = boolResult;
            else if (value is string stringValue)
                result = stringValue.Equals(bool.TrueString);
            else if (value is null)
                result = false;
            else
                throw new MessageFormatException("stream value: " + value.GetType().Name + " cannot be converted to boolean.");

            facade.Pop();
            return result;
        }

        private void CheckBytesInFlight()
        {
            if (remainingBytes != NO_BYTES_IN_FLIGHT)
            {
                throw new MessageFormatException("Partially read byte[] entry still being retrieved using readBytes(byte[] dest)");
            }
        }

        public byte ReadByte()
        {
            CheckWriteOnlyBody();
            CheckBytesInFlight();

            byte result;
            object value = facade.Peek();
            if (value is byte byteValue)
                result = byteValue;
            else if (value is string stringValue && byte.TryParse(stringValue, out var parsedValue))
                result = parsedValue;
            else if (value is null)
                throw new NullReferenceException("Cannot convert null value to byte.");
            else
                throw new MessageFormatException("stream value: " + value.GetType().Name + " cannot be converted to byte.");

            facade.Pop();
            return result;
        }

        public int ReadBytes(byte[] value)
        {
            CheckWriteOnlyBody();

            if (value == null)
            {
                throw new ArgumentNullException(nameof(value), "Target byte array was null");
            }

            if (remainingBytes == NO_BYTES_IN_FLIGHT)
            {
                object data = facade.Peek();
                if (data == null)
                {
                    return -1;
                }
                else if (!(data is byte[]))
                {
                    throw new MessageFormatException("Next stream value is not a byte array.");
                }
                bytes = data as byte[];
                remainingBytes = bytes.Length;
            }
            else if (remainingBytes == 0)
            {
                // We previously read all the bytes, but must have filled the destination array.
                remainingBytes = NO_BYTES_IN_FLIGHT;
                bytes = null;
                facade.Pop();
                return -1;
            }

            int previouslyRead = bytes.Length - remainingBytes;
            int lengthToCopy = Math.Min(value.Length, remainingBytes);

            if (lengthToCopy > 0)
            {
                Array.Copy(bytes, previouslyRead, value, 0, lengthToCopy);
            }

            remainingBytes -= lengthToCopy;

            if (remainingBytes == 0 && lengthToCopy < value.Length)
            {
                // All bytes have been read and the destination array was not filled on this
                // call, so the return will enable the caller to determine completion immediately.
                remainingBytes = NO_BYTES_IN_FLIGHT;
                bytes = null;
                facade.Pop();
            }

            return lengthToCopy;
        }

        public char ReadChar()
        {
            CheckWriteOnlyBody();
            CheckBytesInFlight();

            char result;
            object value = facade.Peek();
            if (value is char charValue)
                result = charValue;
            else if (value is null)
                throw new NullReferenceException("Cannot convert null value to char.");
            else
                throw new MessageFormatException("stream value: " + value.GetType().Name + " cannot be converted to char.");

            facade.Pop();
            return result;
        }

        public short ReadInt16()
        {
            CheckWriteOnlyBody();
            CheckBytesInFlight();

            short result;
            object value = facade.Peek();
            if (value is short shortValue)
                result = shortValue;
            else if (value is byte byteValue)
                result = byteValue;
            else if (value is string stringValue && short.TryParse(stringValue, out var parsedValue))
                result = parsedValue;
            else if (value is null)
                throw new NullReferenceException("Cannot convert null value to short.");
            else
                throw new MessageFormatException("stream value: " + value.GetType().Name + " cannot be converted to short.");

            facade.Pop();
            return result;
        }

        public int ReadInt32()
        {
            CheckWriteOnlyBody();
            CheckBytesInFlight();

            int result;
            object value = facade.Peek();
            if (value is int intValue)
                result = intValue;
            else if (value is short shortValue)
                result = shortValue;
            else if (value is byte byteValue)
                result = byteValue;
            else if (value is string stringValue && int.TryParse(stringValue, out var parsedValue))
                result = parsedValue;
            else if (value is null)
                throw new NullReferenceException("Cannot convert null value to int.");
            else
                throw new MessageFormatException("stream value: " + value.GetType().Name + " cannot be converted to int.");

            facade.Pop();
            return result;
        }

        public long ReadInt64()
        {
            CheckWriteOnlyBody();
            CheckBytesInFlight();

            long result;
            object value = facade.Peek();
            if (value is long longValue)
                result = longValue;
            else if (value is int intValue)
                result = intValue;
            else if (value is short shortValue)
                result = shortValue;
            else if (value is byte byteValue)
                result = byteValue;
            else if (value is string stringValue && long.TryParse(stringValue, out var parsedValue))
                result = parsedValue;
            else if (value is null)
                throw new NullReferenceException("Cannot convert null value to long.");
            else
                throw new MessageFormatException("stream value: " + value.GetType().Name + " cannot be converted to long.");

            facade.Pop();
            return result;
        }

        public float ReadSingle()
        {
            CheckWriteOnlyBody();
            CheckBytesInFlight();

            float result;
            object value = facade.Peek();
            if (value is float floatValue)
                result = floatValue;
            else if (value is string stringValue && float.TryParse(stringValue, out var parsedValue))
                result = parsedValue;
            else if (value is null)
                throw new NullReferenceException("Cannot convert null value to float.");
            else
                throw new MessageFormatException("stream value: " + value.GetType().Name + " cannot be converted to float.");

            facade.Pop();
            return result;
        }

        public double ReadDouble()
        {
            CheckWriteOnlyBody();
            CheckBytesInFlight();

            double result;
            object value = facade.Peek();
            if (value is double doubleValue)
                result = doubleValue;
            else if (value is float floatValue)
                result = floatValue;
            else if (value is string stringValue && float.TryParse(stringValue, out var parsedValue))
                result = parsedValue;
            else if (value is null)
                throw new NullReferenceException("Cannot convert null value to long.");
            else
                throw new MessageFormatException("stream value: " + value.GetType().Name + " cannot be converted to double.");

            facade.Pop();
            return result;
        }

        public string ReadString()
        {
            CheckWriteOnlyBody();
            CheckBytesInFlight();

            string result;
            object value = facade.Peek();
            if (value == null)
                result = null;
            else if (value is string stringValue)
                result = stringValue;
            else if (value is float floatValue)
                result = floatValue.ToString(CultureInfo.InvariantCulture);
            else if (value is double doubleValue)
                result = doubleValue.ToString(CultureInfo.InvariantCulture);
            else if (value is long longValue)
                result = longValue.ToString();
            else if (value is int intValue)
                result = intValue.ToString();
            else if (value is short shortValue)
                result = shortValue.ToString();
            else if (value is byte byteValue)
                result = byteValue.ToString();
            else if (value is bool boolValue)
                result = boolValue.ToString();
            else if (value is char charValue)
                result = charValue.ToString();
            else
                throw new MessageFormatException("stream value: " + value.GetType().Name + " cannot be converted to int.");

            facade.Pop();
            return result;
        }

        public object ReadObject()
        {
            CheckWriteOnlyBody();
            CheckBytesInFlight();

            object result;
            object value = facade.Peek();
            if (value == null)
                result = null;
            else if (value is string)
                result = value;
            else if (value is float)
                result = value;
            else if (value is double)
                result = value;
            else if (value is long)
                result = value;
            else if (value is int)
                result = value;
            else if (value is short)
                result = value;
            else if (value is byte)
                result = value;
            else if (value is bool)
                result = value;
            else if (value is char)
                result = value;
            else if (value is byte[] original)
            {
                byte[] bytesResult = new byte[original.Length];
                Array.Copy(original, 0, bytesResult, 0, original.Length);
                result = bytesResult;
            }
            else
                throw new MessageFormatException("stream value: " + value.GetType().Name + " cannot be converted to int.");

            facade.Pop();
            return result;
        }

        public void WriteBoolean(bool value)
        {
            CheckReadOnlyBody();
            facade.Put(value);
        }

        public void WriteByte(byte value)
        {
            CheckReadOnlyBody();
            facade.Put(value);
        }

        public void WriteBytes(byte[] value)
        {
            WriteBytes(value, 0, value.Length);
        }

        public void WriteBytes(byte[] value, int offset, int length)
        {
            CheckReadOnlyBody();

            byte[] entry = new byte[length];
            Array.Copy(value, offset, entry, 0, length);
            facade.Put(entry);
        }

        public void WriteChar(char value)
        {
            CheckReadOnlyBody();
            facade.Put(value);
        }

        public void WriteInt16(short value)
        {
            CheckReadOnlyBody();
            facade.Put(value);
        }

        public void WriteInt32(int value)
        {
            CheckReadOnlyBody();
            facade.Put(value);
        }

        public void WriteInt64(long value)
        {
            CheckReadOnlyBody();
            facade.Put(value);
        }

        public void WriteSingle(float value)
        {
            CheckReadOnlyBody();
            facade.Put(value);
        }

        public void WriteDouble(double value)
        {
            CheckReadOnlyBody();
            facade.Put(value);
        }

        public void WriteString(string value)
        {
            CheckReadOnlyBody();
            facade.Put(value);
        }

        public void WriteObject(object value)
        {
            CheckReadOnlyBody();

            switch (value)
            {
                case null:
                case string _:
                case char _:
                case bool _:
                case byte _:
                case short _:
                case int _:
                case long _:
                case float _:
                case double _:
                    facade.Put(value);
                    break;
                case byte[] bytesValue:
                    WriteBytes(bytesValue);
                    break;
                default:
                    throw new MessageFormatException("Unsupported Object type: " + value.GetType().Name);
            }
        }

        public override void ClearBody()
        {
            base.ClearBody();
            bytes = null;
            remainingBytes = NO_BYTES_IN_FLIGHT;
        }

        public void Reset()
        {
            bytes = null;
            remainingBytes = NO_BYTES_IN_FLIGHT;
            IsReadOnlyBody = true;
            facade.Reset();
        }
        
        public override NmsMessage Copy()
        {
            NmsStreamMessage copy = new NmsStreamMessage(facade.Copy() as INmsStreamMessageFacade);
            CopyInto(copy);
            return copy;
        }
    }
}