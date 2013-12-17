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
using Apache.NMS.Util;

namespace Apache.NMS.Amqp
{
	public class StreamMessage : BaseMessage, IStreamMessage
	{
		private EndianBinaryReader dataIn = null;
		private EndianBinaryWriter dataOut = null;
		private MemoryStream byteBuffer = null;
		private int bytesRemaining = -1;

		public bool ReadBoolean()
		{
			InitializeReading();

			try
			{
				long startingPos = this.byteBuffer.Position;
				try
				{
					int type = this.dataIn.ReadByte();

					if(type == PrimitiveMap.BOOLEAN_TYPE)
					{
						return this.dataIn.ReadBoolean();
					}
					else if(type == PrimitiveMap.STRING_TYPE)
					{
						return Boolean.Parse(this.dataIn.ReadString16());
					}
					else if(type == PrimitiveMap.NULL)
					{
						this.byteBuffer.Seek(startingPos, SeekOrigin.Begin);
						throw new NMSException("Cannot convert Null type to a bool");
					}
					else
					{
						this.byteBuffer.Seek(startingPos, SeekOrigin.Begin);
						throw new MessageFormatException("Value is not a Boolean type.");
					}
				}
				catch(FormatException e)
				{
					this.byteBuffer.Seek(startingPos, SeekOrigin.Begin);
					throw NMSExceptionSupport.CreateMessageFormatException(e);
				}
			}
			catch(EndOfStreamException e)
			{
				throw NMSExceptionSupport.CreateMessageEOFException(e);
			}
			catch(IOException e)
			{
				throw NMSExceptionSupport.CreateMessageFormatException(e);
			}
		}

		public byte ReadByte()
		{
			InitializeReading();

			try
			{
				long startingPos = this.byteBuffer.Position;
				try
				{
					int type = this.dataIn.ReadByte();

					if(type == PrimitiveMap.BYTE_TYPE)
					{
						return this.dataIn.ReadByte();
					}
					else if(type == PrimitiveMap.STRING_TYPE)
					{
						return Byte.Parse(this.dataIn.ReadString16());
					}
					else if(type == PrimitiveMap.NULL)
					{
						this.byteBuffer.Seek(startingPos, SeekOrigin.Begin);
						throw new NMSException("Cannot convert Null type to a byte");
					}
					else
					{
						this.byteBuffer.Seek(startingPos, SeekOrigin.Begin);
						throw new MessageFormatException("Value is not a Byte type.");
					}
				}
				catch(FormatException e)
				{
					this.byteBuffer.Seek(startingPos, SeekOrigin.Begin);
					throw NMSExceptionSupport.CreateMessageFormatException(e);
				}
			}
			catch(EndOfStreamException e)
			{
				throw NMSExceptionSupport.CreateMessageEOFException(e);
			}
			catch(IOException e)
			{
				throw NMSExceptionSupport.CreateMessageFormatException(e);
			}
		}

		public char ReadChar()
		{
			InitializeReading();

			try
			{
				long startingPos = this.byteBuffer.Position;
				try
				{
					int type = this.dataIn.ReadByte();

					if(type == PrimitiveMap.CHAR_TYPE)
					{
						return this.dataIn.ReadChar();
					}
					else if(type == PrimitiveMap.NULL)
					{
						this.byteBuffer.Seek(startingPos, SeekOrigin.Begin);
						throw new NMSException("Cannot convert Null type to a char");
					}
					else
					{
						this.byteBuffer.Seek(startingPos, SeekOrigin.Begin);
						throw new MessageFormatException("Value is not a Char type.");
					}
				}
				catch(FormatException e)
				{
					this.byteBuffer.Seek(startingPos, SeekOrigin.Begin);
					throw NMSExceptionSupport.CreateMessageFormatException(e);
				}
			}
			catch(EndOfStreamException e)
			{
				throw NMSExceptionSupport.CreateMessageEOFException(e);
			}
			catch(IOException e)
			{
				throw NMSExceptionSupport.CreateMessageFormatException(e);
			}
		}

		public short ReadInt16()
		{
			InitializeReading();

			try
			{
				long startingPos = this.byteBuffer.Position;
				try
				{
					int type = this.dataIn.ReadByte();

					if(type == PrimitiveMap.SHORT_TYPE)
					{
						return this.dataIn.ReadInt16();
					}
					else if(type == PrimitiveMap.BYTE_TYPE)
					{
						return this.dataIn.ReadByte();
					}
					else if(type == PrimitiveMap.STRING_TYPE)
					{
						return Int16.Parse(this.dataIn.ReadString16());
					}
					else if(type == PrimitiveMap.NULL)
					{
						this.byteBuffer.Seek(startingPos, SeekOrigin.Begin);
						throw new NMSException("Cannot convert Null type to a short");
					}
					else
					{
						this.byteBuffer.Seek(startingPos, SeekOrigin.Begin);
						throw new MessageFormatException("Value is not a Int16 type.");
					}
				}
				catch(FormatException e)
				{
					this.byteBuffer.Seek(startingPos, SeekOrigin.Begin);
					throw NMSExceptionSupport.CreateMessageFormatException(e);
				}
			}
			catch(EndOfStreamException e)
			{
				throw NMSExceptionSupport.CreateMessageEOFException(e);
			}
			catch(IOException e)
			{
				throw NMSExceptionSupport.CreateMessageFormatException(e);
			}
		}

		public int ReadInt32()
		{
			InitializeReading();

			try
			{
				long startingPos = this.byteBuffer.Position;
				try
				{
					int type = this.dataIn.ReadByte();

					if(type == PrimitiveMap.INTEGER_TYPE)
					{
						return this.dataIn.ReadInt32();
					}
					else if(type == PrimitiveMap.SHORT_TYPE)
					{
						return this.dataIn.ReadInt16();
					}
					else if(type == PrimitiveMap.BYTE_TYPE)
					{
						return this.dataIn.ReadByte();
					}
					else if(type == PrimitiveMap.STRING_TYPE)
					{
						return Int32.Parse(this.dataIn.ReadString16());
					}
					else if(type == PrimitiveMap.NULL)
					{
						this.byteBuffer.Seek(startingPos, SeekOrigin.Begin);
						throw new NMSException("Cannot convert Null type to a int");
					}
					else
					{
						this.byteBuffer.Seek(startingPos, SeekOrigin.Begin);
						throw new MessageFormatException("Value is not a Int32 type.");
					}
				}
				catch(FormatException e)
				{
					this.byteBuffer.Seek(startingPos, SeekOrigin.Begin);
					throw NMSExceptionSupport.CreateMessageFormatException(e);
				}
			}
			catch(EndOfStreamException e)
			{
				throw NMSExceptionSupport.CreateMessageEOFException(e);
			}
			catch(IOException e)
			{
				throw NMSExceptionSupport.CreateMessageFormatException(e);
			}
		}

		public long ReadInt64()
		{
			InitializeReading();

			try
			{
				long startingPos = this.byteBuffer.Position;
				try
				{
					int type = this.dataIn.ReadByte();

					if(type == PrimitiveMap.LONG_TYPE)
					{
						return this.dataIn.ReadInt64();
					}
					else if(type == PrimitiveMap.INTEGER_TYPE)
					{
						return this.dataIn.ReadInt32();
					}
					else if(type == PrimitiveMap.SHORT_TYPE)
					{
						return this.dataIn.ReadInt16();
					}
					else if(type == PrimitiveMap.BYTE_TYPE)
					{
						return this.dataIn.ReadByte();
					}
					else if(type == PrimitiveMap.STRING_TYPE)
					{
						return Int64.Parse(this.dataIn.ReadString16());
					}
					else if(type == PrimitiveMap.NULL)
					{
						this.byteBuffer.Seek(startingPos, SeekOrigin.Begin);
						throw new NMSException("Cannot convert Null type to a long");
					}
					else
					{
						this.byteBuffer.Seek(startingPos, SeekOrigin.Begin);
						throw new MessageFormatException("Value is not a Int64 type.");
					}
				}
				catch(FormatException e)
				{
					this.byteBuffer.Seek(startingPos, SeekOrigin.Begin);
					throw NMSExceptionSupport.CreateMessageFormatException(e);
				}
			}
			catch(EndOfStreamException e)
			{
				throw NMSExceptionSupport.CreateMessageEOFException(e);
			}
			catch(IOException e)
			{
				throw NMSExceptionSupport.CreateMessageFormatException(e);
			}
		}

		public float ReadSingle()
		{
			InitializeReading();

			try
			{
				long startingPos = this.byteBuffer.Position;
				try
				{
					int type = this.dataIn.ReadByte();

					if(type == PrimitiveMap.FLOAT_TYPE)
					{
						return this.dataIn.ReadSingle();
					}
					else if(type == PrimitiveMap.STRING_TYPE)
					{
						return Single.Parse(this.dataIn.ReadString16());
					}
					else if(type == PrimitiveMap.NULL)
					{
						this.byteBuffer.Seek(startingPos, SeekOrigin.Begin);
						throw new NMSException("Cannot convert Null type to a float");
					}
					else
					{
						this.byteBuffer.Seek(startingPos, SeekOrigin.Begin);
						throw new MessageFormatException("Value is not a Single type.");
					}
				}
				catch(FormatException e)
				{
					this.byteBuffer.Seek(startingPos, SeekOrigin.Begin);
					throw NMSExceptionSupport.CreateMessageFormatException(e);
				}
			}
			catch(EndOfStreamException e)
			{
				throw NMSExceptionSupport.CreateMessageEOFException(e);
			}
			catch(IOException e)
			{
				throw NMSExceptionSupport.CreateMessageFormatException(e);
			}
		}

		public double ReadDouble()
		{
			InitializeReading();

			try
			{
				long startingPos = this.byteBuffer.Position;
				try
				{
					int type = this.dataIn.ReadByte();

					if(type == PrimitiveMap.DOUBLE_TYPE)
					{
						return this.dataIn.ReadDouble();
					}
					else if(type == PrimitiveMap.FLOAT_TYPE)
					{
						return this.dataIn.ReadSingle();
					}
					else if(type == PrimitiveMap.STRING_TYPE)
					{
						return Single.Parse(this.dataIn.ReadString16());
					}
					else if(type == PrimitiveMap.NULL)
					{
						this.byteBuffer.Seek(startingPos, SeekOrigin.Begin);
						throw new NMSException("Cannot convert Null type to a double");
					}
					else
					{
						this.byteBuffer.Seek(startingPos, SeekOrigin.Begin);
						throw new MessageFormatException("Value is not a Double type.");
					}
				}
				catch(FormatException e)
				{
					this.byteBuffer.Seek(startingPos, SeekOrigin.Begin);
					throw NMSExceptionSupport.CreateMessageFormatException(e);
				}
			}
			catch(EndOfStreamException e)
			{
				throw NMSExceptionSupport.CreateMessageEOFException(e);
			}
			catch(IOException e)
			{
				throw NMSExceptionSupport.CreateMessageFormatException(e);
			}
		}

		public string ReadString()
		{
			InitializeReading();

			long startingPos = this.byteBuffer.Position;

			try
			{
				int type = this.dataIn.ReadByte();

				if(type == PrimitiveMap.BIG_STRING_TYPE)
				{
					return this.dataIn.ReadString32();
				}
				else if(type == PrimitiveMap.STRING_TYPE)
				{
					return this.dataIn.ReadString16();
				}
				else if(type == PrimitiveMap.LONG_TYPE)
				{
					return this.dataIn.ReadInt64().ToString();
				}
				else if(type == PrimitiveMap.INTEGER_TYPE)
				{
					return this.dataIn.ReadInt32().ToString();
				}
				else if(type == PrimitiveMap.SHORT_TYPE)
				{
					return this.dataIn.ReadInt16().ToString();
				}
				else if(type == PrimitiveMap.FLOAT_TYPE)
				{
					return this.dataIn.ReadSingle().ToString();
				}
				else if(type == PrimitiveMap.DOUBLE_TYPE)
				{
					return this.dataIn.ReadDouble().ToString();
				}
				else if(type == PrimitiveMap.CHAR_TYPE)
				{
					return this.dataIn.ReadChar().ToString();
				}
				else if(type == PrimitiveMap.BYTE_TYPE)
				{
					return this.dataIn.ReadByte().ToString();
				}
				else if(type == PrimitiveMap.BOOLEAN_TYPE)
				{
					return this.dataIn.ReadBoolean().ToString();
				}
				else if(type == PrimitiveMap.NULL)
				{
					return null;
				}
				else
				{
					this.byteBuffer.Seek(startingPos, SeekOrigin.Begin);
					throw new MessageFormatException("Value is not a known type.");
				}
			}
			catch(FormatException e)
			{
				this.byteBuffer.Seek(startingPos, SeekOrigin.Begin);
				throw NMSExceptionSupport.CreateMessageFormatException(e);
			}
			catch(EndOfStreamException e)
			{
				throw NMSExceptionSupport.CreateMessageEOFException(e);
			}
			catch(IOException e)
			{
				throw NMSExceptionSupport.CreateMessageFormatException(e);
			}
		}

		public int ReadBytes(byte[] value)
		{
			InitializeReading();

			if(value == null)
			{
				throw new NullReferenceException("Passed Byte Array is null");
			}

			try
			{
				if(this.bytesRemaining == -1)
				{
					long startingPos = this.byteBuffer.Position;
					byte type = this.dataIn.ReadByte();

					if(type != PrimitiveMap.BYTE_ARRAY_TYPE)
					{
						this.byteBuffer.Seek(startingPos, SeekOrigin.Begin);
						throw new MessageFormatException("Not a byte array");
					}

					this.bytesRemaining = this.dataIn.ReadInt32();
				}
				else if(this.bytesRemaining == 0)
				{
					this.bytesRemaining = -1;
					return -1;
				}

				if(value.Length <= this.bytesRemaining)
				{
					// small buffer
					this.bytesRemaining -= value.Length;
					this.dataIn.Read(value, 0, value.Length);
					return value.Length;
				}
				else
				{
					// big buffer
					int rc = this.dataIn.Read(value, 0, this.bytesRemaining);
					this.bytesRemaining = 0;
					return rc;
				}
			}
			catch(EndOfStreamException ex)
			{
				throw NMSExceptionSupport.CreateMessageEOFException(ex);
			}
			catch(IOException ex)
			{
				throw NMSExceptionSupport.CreateMessageFormatException(ex);
			}
		}

		public Object ReadObject()
		{
			InitializeReading();

			long startingPos = this.byteBuffer.Position;

			try
			{
				int type = this.dataIn.ReadByte();

				if(type == PrimitiveMap.BIG_STRING_TYPE)
				{
					return this.dataIn.ReadString32();
				}
				else if(type == PrimitiveMap.STRING_TYPE)
				{
					return this.dataIn.ReadString16();
				}
				else if(type == PrimitiveMap.LONG_TYPE)
				{
					return this.dataIn.ReadInt64();
				}
				else if(type == PrimitiveMap.INTEGER_TYPE)
				{
					return this.dataIn.ReadInt32();
				}
				else if(type == PrimitiveMap.SHORT_TYPE)
				{
					return this.dataIn.ReadInt16();
				}
				else if(type == PrimitiveMap.FLOAT_TYPE)
				{
					return this.dataIn.ReadSingle();
				}
				else if(type == PrimitiveMap.DOUBLE_TYPE)
				{
					return this.dataIn.ReadDouble();
				}
				else if(type == PrimitiveMap.CHAR_TYPE)
				{
					return this.dataIn.ReadChar();
				}
				else if(type == PrimitiveMap.BYTE_TYPE)
				{
					return this.dataIn.ReadByte();
				}
				else if(type == PrimitiveMap.BOOLEAN_TYPE)
				{
					return this.dataIn.ReadBoolean();
				}
				else if(type == PrimitiveMap.BYTE_ARRAY_TYPE)
				{
					int length = this.dataIn.ReadInt32();
					byte[] data = new byte[length];
					this.dataIn.Read(data, 0, length);
					return data;
				}
				else if(type == PrimitiveMap.NULL)
				{
					return null;
				}
				else
				{
					this.byteBuffer.Seek(startingPos, SeekOrigin.Begin);
					throw new MessageFormatException("Value is not a known type.");
				}
			}
			catch(FormatException e)
			{
				this.byteBuffer.Seek(startingPos, SeekOrigin.Begin);
				throw NMSExceptionSupport.CreateMessageFormatException(e);
			}
			catch(EndOfStreamException e)
			{
				throw NMSExceptionSupport.CreateMessageEOFException(e);
			}
			catch(IOException e)
			{
				throw NMSExceptionSupport.CreateMessageFormatException(e);
			}
		}

		public void WriteBoolean(bool value)
		{
			InitializeWriting();
			try
			{
				this.dataOut.Write(PrimitiveMap.BOOLEAN_TYPE);
				this.dataOut.Write(value);
			}
			catch(IOException e)
			{
				NMSExceptionSupport.Create(e);
			}
		}

		public void WriteByte(byte value)
		{
			InitializeWriting();
			try
			{
				this.dataOut.Write(PrimitiveMap.BYTE_TYPE);
				this.dataOut.Write(value);
			}
			catch(IOException e)
			{
				NMSExceptionSupport.Create(e);
			}
		}

		public void WriteBytes(byte[] value)
		{
			InitializeWriting();
			this.WriteBytes(value, 0, value.Length);
		}

		public void WriteBytes(byte[] value, int offset, int length)
		{
			InitializeWriting();
			try
			{
				this.dataOut.Write(PrimitiveMap.BYTE_ARRAY_TYPE);
				this.dataOut.Write((int) length);
				this.dataOut.Write(value, offset, length);
			}
			catch(IOException e)
			{
				NMSExceptionSupport.Create(e);
			}
		}

		public void WriteChar(char value)
		{
			InitializeWriting();
			try
			{
				this.dataOut.Write(PrimitiveMap.CHAR_TYPE);
				this.dataOut.Write(value);
			}
			catch(IOException e)
			{
				NMSExceptionSupport.Create(e);
			}
		}

		public void WriteInt16(short value)
		{
			InitializeWriting();
			try
			{
				this.dataOut.Write(PrimitiveMap.SHORT_TYPE);
				this.dataOut.Write(value);
			}
			catch(IOException e)
			{
				NMSExceptionSupport.Create(e);
			}
		}

		public void WriteInt32(int value)
		{
			InitializeWriting();
			try
			{
				this.dataOut.Write(PrimitiveMap.INTEGER_TYPE);
				this.dataOut.Write(value);
			}
			catch(IOException e)
			{
				NMSExceptionSupport.Create(e);
			}
		}

		public void WriteInt64(long value)
		{
			InitializeWriting();
			try
			{
				this.dataOut.Write(PrimitiveMap.LONG_TYPE);
				this.dataOut.Write(value);
			}
			catch(IOException e)
			{
				NMSExceptionSupport.Create(e);
			}
		}

		public void WriteSingle(float value)
		{
			InitializeWriting();
			try
			{
				this.dataOut.Write(PrimitiveMap.FLOAT_TYPE);
				this.dataOut.Write(value);
			}
			catch(IOException e)
			{
				NMSExceptionSupport.Create(e);
			}
		}

		public void WriteDouble(double value)
		{
			InitializeWriting();
			try
			{
				this.dataOut.Write(PrimitiveMap.DOUBLE_TYPE);
				this.dataOut.Write(value);
			}
			catch(IOException e)
			{
				NMSExceptionSupport.Create(e);
			}
		}

		public void WriteString(string value)
		{
			InitializeWriting();
			try
			{
				if(value.Length > 8192)
				{
					this.dataOut.Write(PrimitiveMap.BIG_STRING_TYPE);
					this.dataOut.WriteString32(value);
				}
				else
				{
					this.dataOut.Write(PrimitiveMap.STRING_TYPE);
					this.dataOut.WriteString16(value);
				}
			}
			catch(IOException e)
			{
				NMSExceptionSupport.Create(e);
			}
		}

		public void WriteObject(Object value)
		{
			InitializeWriting();
			if(value is System.Byte)
			{
				this.WriteByte((byte) value);
			}
			else if(value is Char)
			{
				this.WriteChar((char) value);
			}
			else if(value is Boolean)
			{
				this.WriteBoolean((bool) value);
			}
			else if(value is Int16)
			{
				this.WriteInt16((short) value);
			}
			else if(value is Int32)
			{
				this.WriteInt32((int) value);
			}
			else if(value is Int64)
			{
				this.WriteInt64((long) value);
			}
			else if(value is Single)
			{
				this.WriteSingle((float) value);
			}
			else if(value is Double)
			{
				this.WriteDouble((double) value);
			}
			else if(value is byte[])
			{
				this.WriteBytes((byte[]) value);
			}
			else if(value is String)
			{
				this.WriteString((string) value);
			}
			else
			{
				throw new MessageFormatException("Cannot write non-primitive type:" + value.GetType());
			}
		}

		public override void ClearBody()
		{
			base.ClearBody();
			this.byteBuffer = null;
			this.dataIn = null;
			this.dataOut = null;
			this.bytesRemaining = -1;
		}

		public void Reset()
		{
			StoreContent();
			this.dataIn = null;
			this.dataOut = null;
			this.byteBuffer = null;
			this.bytesRemaining = -1;
			this.ReadOnlyBody = true;
		}

		private void InitializeReading()
		{
			FailIfWriteOnlyBody();
			if(this.dataIn == null)
			{
				// TODO - Add support for Message Compression.
				this.byteBuffer = new MemoryStream(this.Content, false);
				dataIn = new EndianBinaryReader(byteBuffer);
			}
		}

		private void InitializeWriting()
		{
			FailIfReadOnlyBody();
			if(this.dataOut == null)
			{
				// TODO - Add support for Message Compression.
				this.byteBuffer = new MemoryStream();
				this.dataOut = new EndianBinaryWriter(byteBuffer);
			}
		}

		private void StoreContent()
		{
			if(dataOut != null)
			{
				dataOut.Close();
				// TODO - Add support for Message Compression.

				this.Content = byteBuffer.ToArray();
				this.dataOut = null;
				this.byteBuffer = null;
			}
		}
	}
}
