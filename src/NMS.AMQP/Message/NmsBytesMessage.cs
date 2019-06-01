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
using System.IO;
using Apache.NMS.AMQP.Message.Facade;
using Apache.NMS.Util;

namespace Apache.NMS.AMQP.Message
{
    public class NmsBytesMessage : NmsMessage, IBytesMessage
    {
        private BinaryWriter dataOut = null;
        private BinaryReader dataIn = null;
        private readonly INmsBytesMessageFacade facade;

        public NmsBytesMessage(INmsBytesMessageFacade facade) : base(facade)
        {
            this.facade = facade;
        }

        public byte[] Content
        {
            get
            {
                byte[] buffer = new byte [BodyLength];
                ReadBytes(buffer);
                return buffer;
            }
            set => WriteBytes(value);
        }

        public byte ReadByte()
        {
            InitializeReading();
            try
            {
                return dataIn.ReadByte();
            }
            catch (EndOfStreamException e)
            {
                throw NMSExceptionSupport.CreateMessageEOFException(e);
            }
            catch (IOException e)
            {
                throw NMSExceptionSupport.CreateMessageFormatException(e);
            }
        }

        public void WriteByte(byte value)
        {
            InitializeWriting();
            try
            {
                this.dataOut.Write(value);
            }
            catch (IOException e)
            {
                NMSExceptionSupport.CreateMessageFormatException(e);
            }
        }

        public bool ReadBoolean()
        {
            InitializeReading();
            try
            {
                return dataIn.ReadBoolean();
            }
            catch (EndOfStreamException e)
            {
                throw NMSExceptionSupport.CreateMessageEOFException(e);
            }
            catch (IOException e)
            {
                throw NMSExceptionSupport.CreateMessageFormatException(e);
            }
        }

        public void WriteBoolean(bool value)
        {
            InitializeWriting();
            try
            {
                this.dataOut.Write(value);
            }
            catch (IOException e)
            {
                NMSExceptionSupport.CreateMessageFormatException(e);
            }
        }

        public char ReadChar()
        {
            InitializeReading();
            try
            {
                return dataIn.ReadChar();
            }
            catch (EndOfStreamException e)
            {
                throw NMSExceptionSupport.CreateMessageEOFException(e);
            }
            catch (IOException e)
            {
                throw NMSExceptionSupport.CreateMessageFormatException(e);
            }
        }

        public void WriteChar(char value)
        {
            InitializeWriting();
            try
            {
                this.dataOut.Write(value);
            }
            catch (IOException e)
            {
                NMSExceptionSupport.CreateMessageFormatException(e);
            }
        }

        public short ReadInt16()
        {
            InitializeReading();
            try
            {
                return dataIn.ReadInt16();
            }
            catch (EndOfStreamException e)
            {
                throw NMSExceptionSupport.CreateMessageEOFException(e);
            }
            catch (IOException e)
            {
                throw NMSExceptionSupport.CreateMessageFormatException(e);
            }
        }

        public void WriteInt16(short value)
        {
            InitializeWriting();
            try
            {
                this.dataOut.Write(value);
            }
            catch (IOException e)
            {
                NMSExceptionSupport.CreateMessageFormatException(e);
            }
        }

        public int ReadInt32()
        {
            InitializeReading();
            try
            {
                return dataIn.ReadInt32();
            }
            catch (EndOfStreamException e)
            {
                throw NMSExceptionSupport.CreateMessageEOFException(e);
            }
            catch (IOException e)
            {
                throw NMSExceptionSupport.CreateMessageFormatException(e);
            }
        }

        public void WriteInt32(int value)
        {
            InitializeWriting();
            try
            {
                this.dataOut.Write(value);
            }
            catch (IOException e)
            {
                NMSExceptionSupport.CreateMessageFormatException(e);
            }
        }

        public long ReadInt64()
        {
            InitializeReading();
            try
            {
                return dataIn.ReadInt64();
            }
            catch (EndOfStreamException e)
            {
                throw NMSExceptionSupport.CreateMessageEOFException(e);
            }
            catch (IOException e)
            {
                throw NMSExceptionSupport.CreateMessageFormatException(e);
            }
        }

        public void WriteInt64(long value)
        {
            InitializeWriting();
            try
            {
                this.dataOut.Write(value);
            }
            catch (IOException e)
            {
                NMSExceptionSupport.CreateMessageFormatException(e);
            }
        }

        public float ReadSingle()
        {
            InitializeReading();
            try
            {
                return dataIn.ReadSingle();
            }
            catch (EndOfStreamException e)
            {
                throw NMSExceptionSupport.CreateMessageEOFException(e);
            }
            catch (IOException e)
            {
                throw NMSExceptionSupport.CreateMessageFormatException(e);
            }
        }

        public void WriteSingle(float value)
        {
            InitializeWriting();
            try
            {
                this.dataOut.Write(value);
            }
            catch (IOException e)
            {
                NMSExceptionSupport.CreateMessageFormatException(e);
            }
        }

        public double ReadDouble()
        {
            InitializeReading();
            try
            {
                return dataIn.ReadDouble();
            }
            catch (EndOfStreamException e)
            {
                throw NMSExceptionSupport.CreateMessageEOFException(e);
            }
            catch (IOException e)
            {
                throw NMSExceptionSupport.CreateMessageFormatException(e);
            }
        }

        public void WriteDouble(double value)
        {
            InitializeWriting();
            try
            {
                this.dataOut.Write(value);
            }
            catch (IOException e)
            {
                NMSExceptionSupport.CreateMessageFormatException(e);
            }
        }

        public int ReadBytes(byte[] value)
        {
            return ReadBytes(value, value.Length);
        }

        public int ReadBytes(byte[] value, int length)
        {
            InitializeReading();

            if (length < 0 || value.Length < length)
            {
                throw new IndexOutOfRangeException("length must not be negative or larger than the size of the provided array");
            }

            try
            {
                return dataIn.Read(value, 0, length);
            }
            catch (EndOfStreamException e)
            {
                throw NMSExceptionSupport.CreateMessageEOFException(e);
            }
            catch (IOException e)
            {
                throw NMSExceptionSupport.CreateMessageFormatException(e);
            }
        }

        public void WriteBytes(byte[] value)
        {
            WriteBytes(value, 0, value.Length);
        }

        public void WriteBytes(byte[] value, int offset, int length)
        {
            InitializeWriting();

            try
            {
                dataOut.Write(value, offset, length);
            }
            catch (Exception e)
            {
                throw NMSExceptionSupport.Create(e);
            }
        }

        public string ReadString()
        {
            InitializeReading();
            try
            {
                return dataIn.ReadString();
            }
            catch (EndOfStreamException e)
            {
                throw NMSExceptionSupport.CreateMessageEOFException(e);
            }
            catch (IOException e)
            {
                throw NMSExceptionSupport.CreateMessageFormatException(e);
            }
        }

        public void WriteString(string value)
        {
            InitializeWriting();
            try
            {
                this.dataOut.Write(value);
            }
            catch (IOException e)
            {
                NMSExceptionSupport.CreateMessageFormatException(e);
            }
        }

        public void WriteObject(object value)
        {
            if (value == null)
                throw new ArgumentNullException(nameof(value));

            if (value is byte byteValue)
                WriteByte(byteValue);
            else if (value is char charValue)
                WriteChar(charValue);
            else if (value is bool boolValue)
                WriteBoolean(boolValue);
            else if (value is short shortValue)
                WriteInt16(shortValue);
            else if (value is int intValue)
                WriteInt32(intValue);
            else if (value is long longValue)
                WriteInt64(longValue);
            else if (value is float floatValue)
                WriteSingle(floatValue);
            else if (value is double doubleValue)
                WriteDouble(doubleValue);
            else if (value is string stringValue)
                WriteString(stringValue);
            else if (value is byte[] bytes)
                WriteBytes(bytes);
            else
                throw new MessageFormatException("Cannot write non-primitive type:" + value.GetType().FullName);
        }

        public override void ClearBody()
        {
            base.ClearBody();
            this.dataIn = null;
            this.dataOut = null;
        }

        public override void OnSend(TimeSpan producerTtl)
        {
            Reset();
            base.OnSend(producerTtl);
        }

        public void Reset()
        {
            this.facade.Reset();
            this.dataOut = null;
            this.dataIn = null;
            IsReadOnlyBody = true;
        }

        public long BodyLength
        {
            get
            {
                InitializeReading();
                return facade.BodyLength;
            }
        }

        public override string ToString()
        {
            return $"NmsBytesMessage {{ {Facade} }}";
        }

        private void InitializeWriting()
        {
            CheckReadOnlyBody();
            if (dataOut == null)
            {
                dataOut = facade.GetDataWriter();
            }
        }

        private void InitializeReading()
        {
            CheckWriteOnlyBody();
            if (dataIn?.BaseStream == null)
            {
                dataIn = facade.GetDataReader();
            }
        }

        public override NmsMessage Copy()
        {
            NmsBytesMessage copy = new NmsBytesMessage(facade.Copy() as INmsBytesMessageFacade);
            CopyInto(copy);
            return copy;
        }
    }
}