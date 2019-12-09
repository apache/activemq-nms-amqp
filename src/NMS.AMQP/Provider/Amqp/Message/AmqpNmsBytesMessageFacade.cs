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
using Amqp.Framing;
using Amqp.Types;
using Apache.NMS.AMQP.Message;
using Apache.NMS.AMQP.Message.Facade;
using Apache.NMS.AMQP.Util;
using Apache.NMS.Util;

namespace Apache.NMS.AMQP.Provider.Amqp.Message
{
    public class AmqpNmsBytesMessageFacade : AmqpNmsMessageFacade, INmsBytesMessageFacade
    {
        private EndianBinaryReader byteIn = null;
        private EndianBinaryWriter byteOut = null;

        private static readonly Data EMPTY_DATA = new Data { Binary = new byte[0] };

        public override sbyte? JmsMsgType => MessageSupport.JMS_TYPE_BYTE;
        public long BodyLength => GetBinaryFromBody().Binary.LongLength;

        public BinaryReader GetDataReader()
        {
            if (byteOut != null)
            {
                throw new IllegalStateException("Body is being written to, cannot perform a read.");
            }

            if (byteIn == null)
            {
                Data body = GetBinaryFromBody();
                Stream dataStream = new MemoryStream(body.Binary, false);
                byteIn = new EndianBinaryReader(dataStream);
            }

            return byteIn;
        }

        private Data GetBinaryFromBody()
        {
            RestrictedDescribed body = Message.BodySection;
            Data result = EMPTY_DATA;
            if (body == null)
            {
                return result;
            }
            else if (body is Data)
            {
                byte[] binary = (body as Data).Binary;
                if (binary != null && binary.Length != 0)
                {
                    return body as Data;
                }
            }
            else if (body is AmqpValue)
            {
                object value = (body as AmqpValue).Value;
                if (value == null)
                {
                    return result;
                }

                if (value is byte[])
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

        public BinaryWriter GetDataWriter()
        {
            if (byteIn != null)
            {
                throw new IllegalStateException("Body is being read from, cannot perform a write.");
            }

            if (byteOut == null)
            {
                MemoryStream outputBuffer = new MemoryStream();
                byteOut = new EndianBinaryWriter(outputBuffer);
                Message.BodySection = EMPTY_DATA;
            }

            return byteOut;
        }

        protected override void InitializeEmptyBody()
        {
            ContentType = SymbolUtil.OCTET_STREAM_CONTENT_TYPE;
            Message.BodySection = EMPTY_DATA;
        }

        public void Reset()
        {
            if (byteOut != null)
            {
                try
                {
                    using (MemoryStream byteStream = new MemoryStream((int) byteOut.BaseStream.Length))
                    {
                        byteOut.BaseStream.Position = 0;
                        byteOut.BaseStream.CopyTo(byteStream);

                        byte[] value = byteStream.ToArray();
                        Message.BodySection = new Data() { Binary = value };
                    }
                }
                finally
                {
                    byteOut.Dispose();
                    byteOut = null;
                }

            }

            if (byteIn != null)
            {
                byteIn.Dispose();
                byteIn = null;
            }
        }

        public override NmsMessage AsMessage()
        {
            return new NmsBytesMessage(this);
        }

        public override void ClearBody()
        {
            Reset();
            Message.BodySection = EMPTY_DATA;
        }

        public virtual bool HasBody()
        {
            if (byteOut != null)
                return byteOut.BaseStream.Length > 0;

            return BodyLength > 0;
        }

        public override void OnSend(TimeSpan producerTtl)
        {
            base.OnSend(producerTtl);

            Reset();
        }

        public override INmsMessageFacade Copy()
        {
            Reset();
            AmqpNmsBytesMessageFacade copy = new AmqpNmsBytesMessageFacade();
            CopyInto(copy);

            if (!copy.HasBody())
            {
                copy.Message.BodySection = EMPTY_DATA;
            }

            return copy;
        }
    }
}