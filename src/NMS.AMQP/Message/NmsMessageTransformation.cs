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

using System.Collections;

namespace Apache.NMS.AMQP.Message
{
    public static class NmsMessageTransformation
    {
        public static NmsMessage TransformMessage(INmsMessageFactory factory, IMessage message)
        {
            NmsMessage nmsMessage = null;

            if (message is IBytesMessage bytesMessage)
            {
                bytesMessage.Reset();
                NmsBytesMessage msg = factory.CreateBytesMessage();

                try
                {
                    while (true)
                    {
                        // Reads a byte from the message stream until the stream is empty
                        msg.WriteByte(bytesMessage.ReadByte());
                    }
                }
                catch (MessageEOFException)
                {
                    // Indicates all the bytes have been read from the source.
                }

                nmsMessage = msg;
            }
            else if (message is IMapMessage mapMessage)
            {
                NmsMapMessage msg = factory.CreateMapMessage();
                CopyMap(mapMessage.Body, msg.Body);
                nmsMessage = msg;
            }
            else if (message is IObjectMessage objectMessage)
            {
                NmsObjectMessage msg = factory.CreateObjectMessage();
                msg.Body = objectMessage.Body;
                nmsMessage = msg;
            }
            else if (message is IStreamMessage streamMessage)
            {
                streamMessage.Reset();
                NmsStreamMessage msg = factory.CreateStreamMessage();
                
                try
                {
                    while (true)
                    {
                        // Reads a byte from the message stream until the stream is empty
                        msg.WriteObject(streamMessage.ReadObject());
                    }
                }
                catch (MessageEOFException)
                {
                    // Indicates all the stream have been read from the source.
                }

                nmsMessage = msg;
            }
            else if (message is ITextMessage textMessage)
            {
                NmsTextMessage msg = factory.CreateTextMessage();
                msg.Text = textMessage.Text;
                nmsMessage = msg;
            }
            else
                nmsMessage = factory.CreateMessage();


            CopyProperties(message, nmsMessage);
            return nmsMessage;
        }

        private static void CopyProperties(IMessage source, NmsMessage target)
        {
            target.NMSMessageId = source.NMSMessageId;
            target.NMSCorrelationID = source.NMSCorrelationID;
            target.NMSDestination = source.NMSDestination;
            target.NMSReplyTo = source.NMSReplyTo;
            target.NMSDeliveryMode = source.NMSDeliveryMode;
            target.NMSRedelivered = source.NMSRedelivered;
            target.NMSType = source.NMSType;
            target.NMSPriority = source.NMSPriority;
            target.NMSTimestamp = source.NMSTimestamp;
            
            CopyMap(source.Properties, target.Properties);
        }

        private static void CopyMap(IPrimitiveMap source, IPrimitiveMap target)
        {
            foreach (object key in source.Keys)
            {
                string name = key.ToString();
                object value = source[name];

                switch (value)
                {
                    case bool boolValue:
                        target.SetBool(name, boolValue);
                        break;
                    case char charValue:
                        target.SetChar(name, charValue);
                        break;
                    case string stringValue:
                        target.SetString(name, stringValue);
                        break;
                    case byte byteValue:
                        target.SetByte(name, byteValue);
                        break;
                    case short shortValue:
                        target.SetShort(name, shortValue);
                        break;
                    case int intValue:
                        target.SetInt(name, intValue);
                        break;
                    case long longValue:
                        target.SetLong(name, longValue);
                        break;
                    case float floatValue:
                        target.SetFloat(name, floatValue);
                        break;
                    case double doubleValue:
                        target.SetDouble(name, doubleValue);
                        break;
                    case byte[] bytesValue:
                        target.SetBytes(name, bytesValue);
                        break;
                    case IList listValue:
                        target.SetList(name, listValue);
                        break;
                    case IDictionary dictionaryValue:
                        target.SetDictionary(name, dictionaryValue);
                        break;
                }
            }
        }
    }
}