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


using Apache.NMS.Util;
using System.IO;
using System;
namespace Apache.NMS.Amqp
{
	/// <summary>
	///
	/// A BytesMessage object is used to send a message containing a stream of uninterpreted
	/// bytes. It inherits from the Message interface and adds a bytes message body. The
	/// receiver of the message supplies the interpretation of the bytes.
	///
	/// This message type is for client encoding of existing message formats. If possible,
	/// one of the other self-defining message types should be used instead.
	///
	/// Although the NMS API allows the use of message properties with byte messages, they
	/// are typically not used, since the inclusion of properties may affect the format.
	///
	/// When the message is first created, and when ClearBody is called, the body of the
	/// message is in write-only mode. After the first call to Reset has been made, the
	/// message body is in read-only mode. After a message has been sent, the client that
	/// sent it can retain and modify it without affecting the message that has been sent.
	/// The same message object can be sent multiple times. When a message has been received,
	/// the provider has called Reset so that the message body is in read-only mode for the
	/// client.
	///
	/// If ClearBody is called on a message in read-only mode, the message body is cleared and
	/// the message is in write-only mode.
	///
	/// If a client attempts to read a message in write-only mode, a MessageNotReadableException
	/// is thrown.
	///
	/// If a client attempts to write a message in read-only mode, a MessageNotWriteableException
	/// is thrown.
	/// </summary>
	public class BytesMessage : BaseMessage, IBytesMessage
	{
		private EndianBinaryReader dataIn = null;
		private EndianBinaryWriter dataOut = null;
		private MemoryStream outputBuffer = null;

		// Need this later when we add compression to store true content length.
		private long length = 0;

		public override void ClearBody()
		{
			base.ClearBody();
			this.outputBuffer = null;
			this.dataIn = null;
			this.dataOut = null;
			this.length = 0;
		}

		public long BodyLength
		{
			get
			{
				InitializeReading();
				return this.length;
			}
		}

		public byte ReadByte()
		{
			InitializeReading();
			try
			{
				return dataIn.ReadByte();
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

		public void WriteByte( byte value )
		{
			InitializeWriting();
			try
			{
				dataOut.Write( value );
			}
			catch(Exception e)
			{
				throw NMSExceptionSupport.Create(e);
			}
		}

		public bool ReadBoolean()
		{
			InitializeReading();
			try
			{
				return dataIn.ReadBoolean();
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

		public void WriteBoolean( bool value )
		{
			InitializeWriting();
			try
			{
				dataOut.Write( value );
			}
			catch(Exception e)
			{
				throw NMSExceptionSupport.Create(e);
			}
		}

		public char ReadChar()
		{
			InitializeReading();
			try
			{
				return dataIn.ReadChar();
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

		public void WriteChar( char value )
		{
			InitializeWriting();
			try
			{
				dataOut.Write( value );
			}
			catch(Exception e)
			{
				throw NMSExceptionSupport.Create(e);
			}
		}

		public short ReadInt16()
		{
			InitializeReading();
			try
			{
				return dataIn.ReadInt16();
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

		public void WriteInt16( short value )
		{
			InitializeWriting();
			try
			{
				dataOut.Write( value );
			}
			catch(Exception e)
			{
				throw NMSExceptionSupport.Create(e);
			}
		}

		public int ReadInt32()
		{
			InitializeReading();
			try
			{
				return dataIn.ReadInt32();
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

		public void WriteInt32( int value )
		{
			InitializeWriting();
			try
			{
				dataOut.Write( value );
			}
			catch(Exception e)
			{
				throw NMSExceptionSupport.Create(e);
			}
		}

		public long ReadInt64()
		{
			InitializeReading();
			try
			{
				return dataIn.ReadInt64();
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

		public void WriteInt64( long value )
		{
			InitializeWriting();
			try
			{
				dataOut.Write( value );
			}
			catch(Exception e)
			{
				throw NMSExceptionSupport.Create(e);
			}
		}

		public float ReadSingle()
		{
			InitializeReading();
			try
			{
				return dataIn.ReadSingle();
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

		public void WriteSingle( float value )
		{
			InitializeWriting();
			try
			{
				dataOut.Write( value );
			}
			catch(Exception e)
			{
				throw NMSExceptionSupport.Create(e);
			}
		}

		public double ReadDouble()
		{
			InitializeReading();
			try
			{
				return dataIn.ReadDouble();
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

		public void WriteDouble( double value )
		{
			InitializeWriting();
			try
			{
				dataOut.Write( value );
			}
			catch(Exception e)
			{
				throw NMSExceptionSupport.Create(e);
			}
		}

		public int ReadBytes( byte[] value )
		{
			InitializeReading();
			try
			{
				return dataIn.Read( value, 0, value.Length );
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

		public int ReadBytes( byte[] value, int length )
		{
			InitializeReading();
			try
			{
				return dataIn.Read( value, 0, length );
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

		public void WriteBytes( byte[] value )
		{
			InitializeWriting();
			try
			{
				dataOut.Write( value, 0, value.Length );
			}
			catch(Exception e)
			{
				throw NMSExceptionSupport.Create(e);
			}
		}

		public void WriteBytes( byte[] value, int offset, int length )
		{
			InitializeWriting();
			try
			{
				dataOut.Write( value, offset, length );
			}
			catch(Exception e)
			{
				throw NMSExceptionSupport.Create(e);
			}
		}

		public string ReadString()
		{
			InitializeReading();
			try
			{
				// JMS, CMS and NMS all encode the String using a 16 bit size header.
				return dataIn.ReadString16();
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

		public void WriteString( string value )
		{
			InitializeWriting();
			try
			{
				// JMS, CMS and NMS all encode the String using a 16 bit size header.
				dataOut.WriteString16(value);
			}
			catch(Exception e)
			{
				throw NMSExceptionSupport.Create(e);
			}
		}

		public void WriteObject( System.Object value )
		{
			InitializeWriting();
			if( value is System.Byte )
			{
				this.dataOut.Write( (byte) value );
			}
			else if( value is Char )
			{
				this.dataOut.Write( (char) value );
			}
			else if( value is Boolean )
			{
				this.dataOut.Write( (bool) value );
			}
			else if( value is Int16 )
			{
				this.dataOut.Write( (short) value );
			}
			else if( value is Int32 )
			{
				this.dataOut.Write( (int) value );
			}
			else if( value is Int64 )
			{
				this.dataOut.Write( (long) value );
			}
			else if( value is Single )
			{
				this.dataOut.Write( (float) value );
			}
			else if( value is Double )
			{
				this.dataOut.Write( (double) value );
			}
			else if( value is byte[] )
			{
				this.dataOut.Write( (byte[]) value );
			}
			else if( value is String )
			{
				this.dataOut.WriteString16( (string) value );
			}
			else
			{
				throw new MessageFormatException("Cannot write non-primitive type:" + value.GetType());
			}
		}

		public void Reset()
		{
			StoreContent();
			this.dataIn = null;
			this.dataOut = null;
			this.outputBuffer = null;
			this.ReadOnlyBody = true;
		}

		private void InitializeReading()
		{
			FailIfWriteOnlyBody();
			if(this.dataIn == null)
			{
				if(this.Content != null)
				{
					this.length = this.Content.Length;
				}

				// TODO - Add support for Message Compression.
				MemoryStream bytesIn = new MemoryStream(this.Content, false);
				dataIn = new EndianBinaryReader(bytesIn);
			}
		}

		private void InitializeWriting()
		{
			FailIfReadOnlyBody();
			if(this.dataOut == null)
			{
				// TODO - Add support for Message Compression.
				this.outputBuffer = new MemoryStream();
				this.dataOut = new EndianBinaryWriter(outputBuffer);
			}
		}

		private void StoreContent()
		{
			if( dataOut != null)
			{
				dataOut.Close();
				// TODO - Add support for Message Compression.

				this.Content = outputBuffer.ToArray();
				this.dataOut = null;
				this.outputBuffer = null;
			}
		}
	}
}

