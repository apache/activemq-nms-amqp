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

using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using Amqp.Framing;
using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP.Provider.Amqp.Message
{
    public class AmqpSerializedObjectDelegate : IAmqpObjectTypeDelegate
    {
        public static readonly Data NULL_OBJECT_BODY = new Data() {Binary = new byte[] {0xac, 0xed, 0x00, 0x05, 0x70}};

        private readonly AmqpNmsObjectMessageFacade facade;

        public AmqpSerializedObjectDelegate(AmqpNmsObjectMessageFacade facade)
        {
            this.facade = facade;
            facade.ContentType = MessageSupport.SERIALIZED_DOTNET_OBJECT_CONTENT_TYPE;
        }

        public object Object
        {
            get
            {
                if (facade.Message.BodySection == null || facade.Message.BodySection == NULL_OBJECT_BODY)
                    return null;

                if (facade.Message.BodySection is Data data)
                {
                    byte[] dataBinary = data.Binary;
                    return dataBinary != null && dataBinary.Length > 0 ? Deserialize(dataBinary) : null;
                }

                throw new IllegalStateException("Unexpected body type: " + facade.Message.BodySection.GetType().FullName);
            }
            set
            {
                if (value != null)
                {
                    byte[] bytes = Serialize(value);
                    if (bytes == null || bytes.Length == 0)
                        facade.Message.BodySection = MessageSupport.EMPTY_DATA;
                    else
                        facade.Message.BodySection = new Data() {Binary = bytes};
                }
                else
                {
                    facade.Message.BodySection = NULL_OBJECT_BODY;
                }
            }
        }

        public void OnSend()
        {
            facade.ContentType = MessageSupport.SERIALIZED_DOTNET_OBJECT_CONTENT_TYPE;
            if (facade.Message.BodySection == null)
                facade.Message.BodySection = NULL_OBJECT_BODY;
        }

        private object Deserialize(byte[] binary)
        {
            using (MemoryStream stream = new MemoryStream(binary))
            {
                IFormatter formatter = new BinaryFormatter();
                return formatter.Deserialize(stream);
            }
        }

        private byte[] Serialize(object o)
        {
            MemoryStream stream = null;
            byte[] result;
            try
            {
                stream = new MemoryStream();
                IFormatter formatter = new BinaryFormatter();
                formatter.Serialize(stream, o);
                result = stream.ToArray();
            }
            finally
            {
                stream?.Close();
            }

            return result;
        }
    }
}