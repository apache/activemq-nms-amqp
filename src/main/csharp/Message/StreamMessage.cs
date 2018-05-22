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
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Apache.NMS;
using Apache.NMS.Util;
using NMS.AMQP.Util.Types;

namespace NMS.AMQP.Message
{
    using Cloak;
    class StreamMessage : Message, IStreamMessage
    {
        
        private const int NO_BYTES_IN_BUFFER = -1;

        private readonly new IStreamMessageCloak cloak;

        private int RemainingBytesInBuffer = NO_BYTES_IN_BUFFER;

        private byte[] Buffer = null;

        internal StreamMessage(IStreamMessageCloak message) : base(message)
        {
            cloak = message;
        }
        
        #region IStreamMessage Methods

        public bool ReadBoolean()
        {
            FailIfWriteOnlyMsgBody();
            FailIfBytesInBuffer();
            bool result;
            object value = cloak.Peek();
            if(value == null)
            {
                result = Convert.ToBoolean(value);
            }
            else
            {
                result = ConversionSupport.ConvertNMSType<bool>(value);
            }
            cloak.Pop();
            return result;
        }

        public byte ReadByte()
        {
            FailIfWriteOnlyMsgBody();
            FailIfBytesInBuffer();
            byte result;
            object value = cloak.Peek();
            if(value == null)
            {
                result = Convert.ToByte(null);
            }
            else
            {
                result = ConversionSupport.ConvertNMSType<byte>(value);
            }

            cloak.Pop();
            return result;
        }

        public int ReadBytes(byte[] value)
        {
            FailIfWriteOnlyMsgBody();
            if (value == null)
            {
                throw new NullReferenceException("Target byte array is null.");
            }
            if (RemainingBytesInBuffer == NO_BYTES_IN_BUFFER)
            {
                object data = cloak.Peek();
                if (data == null)
                {
                    return -1;
                }
                else if (!(data is byte[]))
                {
                    throw new MessageFormatException("Next stream value is not a byte array.");
                }
                Buffer = data as byte[];
                RemainingBytesInBuffer = Buffer.Length;
            } 
            int bufferOffset = Buffer.Length - RemainingBytesInBuffer;
            int copyLength = Math.Min(value.Length, RemainingBytesInBuffer);
            if(copyLength > 0)
                Array.Copy(Buffer, bufferOffset, value, 0, copyLength);
            RemainingBytesInBuffer -= copyLength;
            if(RemainingBytesInBuffer == 0)
            {
                RemainingBytesInBuffer = NO_BYTES_IN_BUFFER;
                Buffer = null;
                cloak.Pop();
            }
            return copyLength;
        }

        public char ReadChar()
        {
            FailIfWriteOnlyMsgBody();
            FailIfBytesInBuffer();
            char result;
            object value = cloak.Peek();
            if (value == null)
            {
                throw new NullReferenceException("Cannot convert NULL value to char.");
            }
            else
            {
                result = ConversionSupport.ConvertNMSType<char>(value);
            }

            cloak.Pop();
            return result;
        }

        public double ReadDouble()
        {
            FailIfWriteOnlyMsgBody();
            FailIfBytesInBuffer();
            double result;
            object value = cloak.Peek();
            if (value == null)
            {
                result = Convert.ToDouble(null);
            }
            else
            {
                result = ConversionSupport.ConvertNMSType<double>(value);
            }

            cloak.Pop();
            return result;
        }

        public short ReadInt16()
        {
            FailIfWriteOnlyMsgBody();
            FailIfBytesInBuffer();
            short result;
            object value = cloak.Peek();
            if (value == null)
            {
                result = Convert.ToInt16(null);
            }
            else
            {
                result = ConversionSupport.ConvertNMSType<short>(value);
            }

            cloak.Pop();
            return result;
        }

        public int ReadInt32()
        {
            FailIfWriteOnlyMsgBody();
            FailIfBytesInBuffer();
            int result;
            object value = cloak.Peek();
            if (value == null)
            {
                result = Convert.ToInt32(null);
            }
            else
            {
                result = ConversionSupport.ConvertNMSType<int>(value);
            }

            cloak.Pop();
            return result;
        }

        public long ReadInt64()
        {
            FailIfWriteOnlyMsgBody();
            FailIfBytesInBuffer();
            long result;
            object value = cloak.Peek();
            if (value == null)
            {
                result = Convert.ToInt64(null);
            }
            else
            {
                result = ConversionSupport.ConvertNMSType<long>(value);
            }

            cloak.Pop();
            return result;
        }

        public object ReadObject()
        {
            FailIfWriteOnlyMsgBody();
            FailIfBytesInBuffer();
            object result = null;
            object value = null;
            try
            {
                value = cloak.Peek();
                if (value == null)
                {
                    result = null;
                }
                else if (value is byte[])
                {
                    byte[] buffer = value as byte[];
                    result = new byte[buffer.Length];
                    Array.Copy(buffer, 0, result as byte[], 0, buffer.Length);
                }
                else if (ConversionSupport.IsNMSType(value))
                {
                    result = value;
                }
            }
            catch (EndOfStreamException eos)
            {
                throw NMSExceptionSupport.CreateMessageEOFException(eos);
            }
            catch (IOException ioe)
            {
                throw NMSExceptionSupport.CreateMessageFormatException(ioe);
            }
            catch (Exception e)
            {
                Tracer.InfoFormat("Unexpected exception caught reading Object stream. Exception = {0}", e);
                throw NMSExceptionSupport.Create("Unexpected exception caught reading Object stream.", e);
            }
            cloak.Pop();
            return result;
        }

        public float ReadSingle()
        {
            FailIfWriteOnlyMsgBody();
            FailIfBytesInBuffer();
            float result;
            object value = cloak.Peek();
            if (value == null)
            {
                result = Convert.ToSingle(null);
            }
            else
            {
                result = ConversionSupport.ConvertNMSType<float>(value);
            }

            cloak.Pop();
            return result;
        }

        public string ReadString()
        {
            FailIfWriteOnlyMsgBody();
            FailIfBytesInBuffer();
            string result;
            object value = cloak.Peek();
            if (value == null)
            {
                result = Convert.ToString(null);
            }
            else
            {
                result = ConversionSupport.ConvertNMSType<string>(value);
            }

            cloak.Pop();
            return result;
        }

        public void Reset()
        {
            RemainingBytesInBuffer = NO_BYTES_IN_BUFFER;
            Buffer = null;
            IsReadOnly = true;
            cloak.Reset();
        }

        public override void ClearBody()
        {
            RemainingBytesInBuffer = NO_BYTES_IN_BUFFER;
            Buffer = null;
            IsReadOnly = false;

            base.ClearBody();
        }

        public void WriteBoolean(bool value)
        {
            FailIfReadOnlyMsgBody();
            cloak.Put(value);
        }

        public void WriteByte(byte value)
        {
            FailIfReadOnlyMsgBody();
            cloak.Put(value);
        }

        public void WriteBytes(byte[] value)
        {
            WriteBytes(value, 0, value.Length);
        }

        public void WriteBytes(byte[] value, int offset, int length)
        {
            FailIfReadOnlyMsgBody();
            byte[] entry = new byte[length];
            Array.Copy(value, offset, entry, 0, length);
            cloak.Put(entry);
        }

        public void WriteChar(char value)
        {
            FailIfReadOnlyMsgBody();
            cloak.Put(value);
        }

        public void WriteDouble(double value)
        {
            FailIfReadOnlyMsgBody();
            cloak.Put(value);
        }

        public void WriteInt16(short value)
        {
            FailIfReadOnlyMsgBody();
            cloak.Put(value);
        }

        public void WriteInt32(int value)
        {
            FailIfReadOnlyMsgBody();
            cloak.Put(value);
        }

        public void WriteInt64(long value)
        {
            FailIfReadOnlyMsgBody();
            cloak.Put(value);
        }

        public void WriteObject(object value)
        {
            FailIfReadOnlyMsgBody();
            if(value == null)
            {
                cloak.Put(value);
            }
            else if(value is byte[])
            {
                WriteBytes(value as byte[]);
            }
            else if (ConversionSupport.IsNMSType(value))
            {
                cloak.Put(value);
            }
            else
            {
                throw NMSExceptionSupport.CreateMessageFormatException(new Exception("Unsupported Object type: " + value.GetType().Name));
            }
        }

        public void WriteSingle(float value)
        {
            FailIfReadOnlyMsgBody();
            cloak.Put(value);
        }

        public void WriteString(string value)
        {
            FailIfReadOnlyMsgBody();
            cloak.Put(value);
        }

        #endregion

        #region Validation Methods
        
        protected void FailIfBytesInBuffer()
        {
            if(RemainingBytesInBuffer != NO_BYTES_IN_BUFFER)
            {
                throw new MessageFormatException("Unfinished Buffered read for ReadBytes(byte[] value)");
            }
        }

        #endregion

        internal override Message Copy()
        {
            return new StreamMessage(this.cloak.Copy());
        }
    }
}
