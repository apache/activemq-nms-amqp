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
using Apache.NMS;
using Apache.NMS.AMQP.Message.Facade;

namespace NMS.AMQP.Test.Message.Facade
{
    public class NmsTestBytesMessageFacade : NmsTestMessageFacade, INmsBytesMessageFacade
    {
        private BinaryWriter bytesOut = null;
        private BinaryReader bytesIn = null;

        public NmsTestBytesMessageFacade()
        {
            Content = new byte[0];
        }

        public NmsTestBytesMessageFacade(byte[] content)
        {
            this.Content = content;
        }

        public BinaryReader GetDataReader()
        {
            if (bytesOut != null)
            {
                throw new IllegalStateException("Body is being written to, cannot perform a read.");
            }
            
            return bytesIn ?? (bytesIn = new BinaryReader(new MemoryStream(Content)));
        }

        public BinaryWriter GetDataWriter()
        {
            if (bytesIn != null) {
                throw new IllegalStateException("Body is being read from, cannot perform a write.");
            }

            return bytesOut ?? (bytesOut = new BinaryWriter(new MemoryStream()));
        }

        public void Reset()
        {
            if (bytesOut != null)
            {
                MemoryStream byteStream = new MemoryStream((int) bytesOut.BaseStream.Length);
                bytesOut.BaseStream.Position = 0;
                bytesOut.BaseStream.CopyTo(byteStream);

                Content = byteStream.ToArray();

                byteStream.Close();
                bytesOut.Close();
                bytesOut = null;
            }
            else if (bytesIn != null)
            {
                bytesIn.Close();
                bytesIn = null;
            }
        }

        public override void ClearBody()
        {
            this.Reset();
            Content = new byte[0];
        }

        public long BodyLength => Content?.LongLength ?? 0;
        public byte[] Content { get; set; }
    }
}