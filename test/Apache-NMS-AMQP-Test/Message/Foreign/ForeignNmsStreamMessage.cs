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

using Apache.NMS;
using Apache.NMS.AMQP.Message;
using NMS.AMQP.Test.Message.Facade;

namespace NMS.AMQP.Test.Message.Foreign
{
    public class ForeignNmsStreamMessage : ForeignNmsMessage, IStreamMessage
    {
        private readonly NmsStreamMessage message = new TestMessageFactory().CreateStreamMessage();
        
        public bool ReadBoolean()
        {
            return message.ReadBoolean();
        }

        public byte ReadByte()
        {
            return message.ReadByte();
        }

        public int ReadBytes(byte[] value)
        {
            return message.ReadBytes(value);
        }

        public char ReadChar()
        {
            return message.ReadChar();
        }

        public short ReadInt16()
        {
            return message.ReadInt16();
        }

        public int ReadInt32()
        {
            return message.ReadInt32();
        }

        public long ReadInt64()
        {
            return message.ReadInt64();
        }

        public float ReadSingle()
        {
            return message.ReadSingle();
        }

        public double ReadDouble()
        {
            return message.ReadDouble();
        }

        public string ReadString()
        {
            return message.ReadString();
        }

        public object ReadObject()
        {
            return message.ReadObject();
        }

        public void WriteBoolean(bool value)
        {
            message.WriteBoolean(value);
        }

        public void WriteByte(byte value)
        {
            message.WriteByte(value);
        }

        public void WriteBytes(byte[] value)
        {
            message.WriteBytes(value);
        }

        public void WriteBytes(byte[] value, int offset, int length)
        {
            message.WriteBytes(value, offset, length);
        }

        public void WriteChar(char value)
        {
            message.WriteChar(value);
        }

        public void WriteInt16(short value)
        {
            message.WriteInt16(value);
        }

        public void WriteInt32(int value)
        {
            message.WriteInt32(value);
        }

        public void WriteInt64(long value)
        {
            message.WriteInt64(value);
        }

        public void WriteSingle(float value)
        {
            message.WriteSingle(value);
        }

        public void WriteDouble(double value)
        {
            message.WriteDouble(value);
        }

        public void WriteString(string value)
        {
            message.WriteString(value);
        }

        public void WriteObject(object value)
        {
            message.WriteObject(value);
        }

        public void Reset()
        {
            message.Reset();
        }
    }
}