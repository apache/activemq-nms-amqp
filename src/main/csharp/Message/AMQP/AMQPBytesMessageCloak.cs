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
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Apache.NMS;
using Apache.NMS.Util;
using Amqp.Framing;
using Amqp.Types;

namespace NMS.AMQP.Message.AMQP
{
    
    using Cloak;
    using Util;
    using Factory;
    class AMQPBytesMessageCloak : AMQPMessageCloak, IBytesMessageCloak
    {

        private static readonly Data EMPTY_DATA;

        static AMQPBytesMessageCloak()
        {
            EMPTY_DATA = new Data();
            EMPTY_DATA.Binary = new byte[0];
        }

        private EndianBinaryReader byteIn=null;
        private EndianBinaryWriter byteOut=null;

        internal AMQPBytesMessageCloak(Connection c) : base(c)
        {
            Content = null;
        }

        internal AMQPBytesMessageCloak(MessageConsumer c, Amqp.Message msg) : base (c, msg) { }

        internal override byte JMSMessageType { get { return MessageSupport.JMS_TYPE_BYTE; } }

        public override byte[] Content
        {
            get
            {   
                return this.GetBinaryFromBody().Binary;
            }
            set
            {
                Data result = EMPTY_DATA;
                if (value != null && value.Length>0)
                {
                    result = new Data();
                    result.Binary = value;
                }
                this.message.BodySection = result;
                base.Content = result.Binary;
            }
        }

        public int BodyLength
        {
            get
            {
                
                return Content != null ? Content.Length : -1;
            }
        }

        public BinaryReader getDataReader()
        {
            if(byteOut != null)
            {
                throw new IllegalStateException("Cannot read message while writing.");
            }
            if (byteIn == null)
            {
                byte[] data = Content;
                if (Content == null)
                {
                    data = EMPTY_DATA.Binary;
                }
                Stream dataStream = new MemoryStream(data, false);
                    
                byteIn = new EndianBinaryReader(dataStream);
            }
            return byteIn;
        }
        public BinaryWriter getDataWriter()
        {
            if (byteIn != null)
            {
                throw new IllegalStateException("Cannot write message while reading.");
            }
            if (byteOut == null)
            {
                MemoryStream outputBuffer = new MemoryStream();
                this.byteOut = new EndianBinaryWriter(outputBuffer);
                message.BodySection = EMPTY_DATA;

            }
            return byteOut;
        }

        public void Reset()
        {
            if (byteOut != null)
            {

                MemoryStream byteStream = new MemoryStream((int)byteOut.BaseStream.Length);
                byteOut.BaseStream.Position = 0;
                byteOut.BaseStream.CopyTo(byteStream);
                
                byte[] value = byteStream.ToArray();
                Content = value;
                
                byteStream.Close();
                byteOut.Close();
                byteOut = null;
            }
            if (byteIn != null)
            {
                byteIn.Close();
                byteIn = null;
            }
        }

        IBytesMessageCloak IBytesMessageCloak.Copy()
        {
            IBytesMessageCloak bcloak = new AMQPBytesMessageCloak(connection);
            this.CopyInto(bcloak);
            return bcloak;
        }

        public override void ClearBody()
        {
            this.Reset();
            Content = null;
        }

        protected override void CopyInto(IMessageCloak msg)
        {
            base.CopyInto(msg);
            this.Reset();
            IBytesMessageCloak bmsg = msg as IBytesMessageCloak;
            bmsg.Content = this.Content;
            
            
        }

        private Data GetBinaryFromBody()
        {
            RestrictedDescribed body = message.BodySection;
            Data result = EMPTY_DATA;
            if(body == null)
            {
                return result;
            }
            else if (body is Data)
            {
                byte[] binary = (body as Data).Binary;
                if(binary != null && binary.Length != 0)
                {
                    return body as Data;
                }
            }
            else if (body is AmqpValue)
            {
                object value = (body as AmqpValue).Value;
                if(value == null) { return result; }
                if(value is byte[])
                {
                    byte[] dataValue = value as byte[];
                    if (dataValue.Length > 0)
                    {
                        result = new Data();
                        result.Binary = dataValue;
                    }
                }
                else
                {
                    throw new IllegalStateException("Unexpected Amqp value content-type: " + value.GetType().FullName);
                }
            }
            else
            {
                throw new IllegalStateException("Unexpected body content-type: " + body.GetType().FullName);
            }

            return result;
        }
        public override string ToString()
        {
            string result = base.ToString();
            if (this.Content != null)
            {
                result += string.Format("\nMessage Body: {0}\n", this.Content.ToString());
            }
            return result;
        }
    }
}
