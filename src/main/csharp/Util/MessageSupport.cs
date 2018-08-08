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
using Apache.NMS;
using Amqp.Framing;
using Amqp.Types;
using Apache.NMS.AMQP.Message;

namespace Apache.NMS.AMQP.Util
{
    /// <summary>
    /// Utility class containing constant values for NMS message fields, and values.
    /// Also contains Utility methods for NMS messagId/correlationId and Destinations.
    /// </summary>
    class MessageSupport
    {
        // "x-opt-jms-msg-type" values
        public const byte JMS_TYPE_MSG  = 0x00;
        public const byte JMS_TYPE_OBJ  = 0x01;
        public const byte JMS_TYPE_MAP  = 0x02;
        public const byte JMS_TYPE_BYTE = 0x03;
        public const byte JMS_TYPE_STRM = 0x04;
        public const byte JMS_TYPE_TXT  = 0x05;

        // "x-opt-jms-dest" and "x-opt-jms-reply-to" values
        public const byte JMS_DEST_TYPE_QUEUE      = 0x00;
        public const byte JMS_DEST_TYPE_TOPIC      = 0x01;
        public const byte JMS_DEST_TYPE_TEMP_QUEUE = 0x02;
        public const byte JMS_DEST_TYPE_TEMP_TOPIC = 0x03;

        // Message Serialization ENCODING Annotation keys
        public const string JMS_AMQP_TYPE_ENCODING = "JMS_AMQP_TYPE_ENCODING";
        public const string JMS_JAVA_ENCODING = "JMS_JAVA_ENCODING";
        public const string JMS_DONET_ENCODING = "JMS_DOTNET_ENCODING";

        // Message Content-type values
        public const string OCTET_STREAM_CONTENT_TYPE = "application/octet-stream";
        public const string SERIALIZED_JAVA_OBJECT_CONTENT_TYPE = "application/x-java-serialized-object";

        // Amqp.Message priority default value
        public static readonly byte DEFAULT_PRIORITY_BYTE = Convert.ToByte((int)NMSConstants.defaultPriority);

        public static readonly Data EMPTY_DATA = new Data() { Binary = new byte[] { } };
        public static readonly AmqpValue NULL_AMQP_VALUE_BODY = new AmqpValue() { Value = null };

        // Amqp Message Outcome instances
        public static readonly Amqp.Framing.Accepted ACCEPTED_INSTANCE = new Amqp.Framing.Accepted();
        public static readonly Amqp.Framing.Released RELEASED_INSTANCE = new Amqp.Framing.Released();
        public static readonly Amqp.Framing.Rejected REJECTED_INSTANCE = new Amqp.Framing.Rejected();
        public static readonly Amqp.Framing.Modified MODIFIED_INSTANCE = new Amqp.Framing.Modified();
        public static readonly Amqp.Framing.Modified MODIFIED_FAILED_INSTANCE = new Amqp.Framing.Modified() { DeliveryFailed = true };
        public static readonly AckType DEFAULT_ACK_TYPE = AckType.ACCEPTED;

        // Message Id constants
        public const string NMS_ID_PREFIX = "ID:";
        public const string AMQP_STRING_PREFIX = "AMQP_STRING:";
        public const string AMQP_ULONG_PREFIX = "AMQP_ULONG:";
        public const string AMQP_BINARY_PREFIX = "AMQP_BINARY:";
        public const string AMQP_UUID_PREFIX = "AMQP_UUID:";
        public const string AMQP_NO_PREFIX = "AMQP_NO_PREFIX:";

        private const string AMQP_TYPE = "AMQP_";
        private static readonly int NMS_ID_PREFIX_LENGTH = NMS_ID_PREFIX.Length;
        private static readonly int AMQP_TYPE_LENGTH = AMQP_TYPE.Length;
        private static readonly int AMQP_STRING_PREFIX_LENGTH = AMQP_STRING_PREFIX.Length;
        private static readonly int AMQP_BINARY_PREFIX_LENGTH = AMQP_BINARY_PREFIX.Length;
        private static readonly int AMQP_ULONG_PREFIX_LENGTH = AMQP_ULONG_PREFIX.Length;
        private static readonly int AMQP_UUID_PREFIX_LENGTH = AMQP_UUID_PREFIX.Length;
        private static readonly int AMQP_NO_PREFIX_LENGTH = AMQP_NO_PREFIX.Length;
        private static readonly char[] HEX_CHARS = "0123456789ABCDEF".ToCharArray();

        public static IDestination CreateDestinationFromMessage(Connection source, Properties properties, byte type, bool replyTo = false)
        {
            IDestination dest = null;
            if(properties==null || source == null) { return dest; }
            bool isPropertyNull = (!replyTo) ? properties.To != null : properties.ReplyTo != null;
            if (isPropertyNull)
            {
                string destname = (!replyTo) ? properties.To : properties.ReplyTo;
                destname = UriUtil.GetDestinationName(destname, source);
                switch (type)
                {
                    case JMS_DEST_TYPE_TEMP_QUEUE:
                        dest = new TemporaryQueue(source, destname);
                        break;
                    case JMS_DEST_TYPE_QUEUE:
                        dest = new Queue(source, destname);
                        break;
                    case JMS_DEST_TYPE_TEMP_TOPIC:
                        dest = new TemporaryTopic(source, destname);
                        break;
                    case JMS_DEST_TYPE_TOPIC:
                        dest = new Topic(source, destname);
                        break;
                }
            }
            return dest;
        }

        public static sbyte GetValueForDestination(IDestination destination)
        {
            sbyte b = 0x00; // same as queue.
            if (destination != null)
            {
                if (destination.IsTopic)
                {
                    if (destination.IsTemporary)
                    {
                        return (sbyte)JMS_DEST_TYPE_TEMP_TOPIC;
                    }
                    else
                    {
                        return (sbyte)JMS_DEST_TYPE_TOPIC;
                    }
                }
                else if (destination.IsQueue)
                {
                    if (destination.IsTemporary)
                    {
                        return (sbyte)JMS_DEST_TYPE_TEMP_QUEUE;
                    }
                    else
                    {
                        return (sbyte)JMS_DEST_TYPE_QUEUE;
                    }
                }
            }
            return b;
        }

        public static byte GetValueForPriority(MsgPriority mp)
        {
            return Convert.ToByte((int)mp);
        }

        public static MsgPriority GetPriorityFromValue(byte value)
        {
            MsgPriority result = NMSConstants.defaultPriority;

            switch (value)
            {
                case 0x00:
                    result = MsgPriority.Lowest;
                    break;
                case 0x01:
                    result = MsgPriority.VeryLow;
                    break;
                case 0x02:
                    result = MsgPriority.Low;
                    break;
                case 0x03:
                    result = MsgPriority.AboveLow;
                    break;
                case 0x04:
                    result = MsgPriority.BelowNormal;
                    break;
                case 0x05:
                    result = MsgPriority.Normal;
                    break;
                case 0x06:
                    result = MsgPriority.AboveNormal;
                    break;
                case 0x07:
                    result = MsgPriority.High;
                    break;
                case 0x08:
                    result = MsgPriority.VeryHigh;
                    break;
                case 0x09:
                    result = MsgPriority.Highest;
                    break;
                default:
                    break;
            }
            
            return result;
        }

        public static object CreateAMQPMessageId(string nmsId)
        {
            if(nmsId == null)
            {
                return null;
            }
            if (!HasNMSIdPrefix(nmsId))
            {
                // application specific msg id return as is.
                return nmsId;
            }

            try
            {
                if (HasAMQPNoPrefix(nmsId, NMS_ID_PREFIX_LENGTH))
                {
                    return nmsId.Substring(NMS_ID_PREFIX_LENGTH + AMQP_NO_PREFIX_LENGTH);
                }
                else if (HasAMQPStringPrefix(nmsId, NMS_ID_PREFIX_LENGTH))
                {
                    return nmsId.Substring(NMS_ID_PREFIX_LENGTH + AMQP_STRING_PREFIX_LENGTH);
                }
                else if (HasAMQPBinaryPrefix(nmsId, NMS_ID_PREFIX_LENGTH))
                {
                    string hexString = nmsId.Substring(NMS_ID_PREFIX_LENGTH + AMQP_BINARY_PREFIX_LENGTH).ToUpper();
                    return ToByteArray(hexString, nmsId);
                }
                else if (HasAMQPULongPrefix(nmsId, NMS_ID_PREFIX_LENGTH))
                {
                    string ulongString = nmsId.Substring(NMS_ID_PREFIX_LENGTH + AMQP_ULONG_PREFIX_LENGTH);
                    return Convert.ToUInt64(ulongString);
                }
                else if (HasAMQPUuidPrefix(nmsId, NMS_ID_PREFIX_LENGTH))
                {
                    string guidString = nmsId.Substring(NMS_ID_PREFIX_LENGTH + AMQP_UUID_PREFIX_LENGTH);
                    return Guid.Parse(guidString);
                }
                else
                {
                    return nmsId;
                }

            }
            catch (Exception e)
            {
                throw new NMSException("Id Conversion Failure. Provided Id: " + nmsId, e);
            }
        }

        public static string CreateNMSMessageId(object amqpId)
        {
            if(null == amqpId)
            {
                return null;
            }
            if(amqpId is string)
            {
                string id = amqpId as string;
                if (!HasNMSIdPrefix(id))
                {
                    return NMS_ID_PREFIX + AMQP_NO_PREFIX + id;
                }
                else if (HasEncodingTypePrefix(id, NMS_ID_PREFIX_LENGTH))
                {
                    return NMS_ID_PREFIX + AMQP_STRING_PREFIX + id;
                }
                else
                {
                    return id;
                }
            }
            else if (amqpId is Guid)
            {
                return NMS_ID_PREFIX + AMQP_UUID_PREFIX + amqpId.ToString();
            }
            else if (amqpId is ulong)
            {
                return NMS_ID_PREFIX + AMQP_ULONG_PREFIX + amqpId.ToString();
            }
            else if (amqpId is byte[])
            {
                return NMS_ID_PREFIX + AMQP_BINARY_PREFIX + ToHexString(amqpId as byte[]);
            }
            else
            {
                throw new NMSException("Unsupported Id Type provided: ", amqpId.GetType().FullName);
            }
        }

        private static bool HasNMSIdPrefix(string id)
        {
            if(id == null)
            {
                return false;
            }
            return id.StartsWith(NMS_ID_PREFIX);
        }

        private static bool HasAMQPNoPrefix(string id, int offset)
        {
            if (id.Length - offset < AMQP_NO_PREFIX_LENGTH)
            {
                return false;
            }
            return id.IndexOf(AMQP_NO_PREFIX, offset, AMQP_NO_PREFIX_LENGTH) - offset == 0;
        }

        private static bool HasAMQPBinaryPrefix(string id, int offset)
        {
            if (id.Length - offset < AMQP_BINARY_PREFIX_LENGTH)
            {
                return false;
            }
            return id.IndexOf(AMQP_BINARY_PREFIX, offset, AMQP_BINARY_PREFIX_LENGTH) - offset == 0;
        }

        private static bool HasAMQPStringPrefix(string id, int offset)
        {
            if (id.Length - offset < AMQP_STRING_PREFIX_LENGTH)
            {
                return false;
            }
            return id.IndexOf(AMQP_STRING_PREFIX, offset, AMQP_STRING_PREFIX_LENGTH) - offset == 0;
        }

        private static bool HasAMQPULongPrefix(string id, int offset)
        {
            if (id.Length - offset < AMQP_ULONG_PREFIX_LENGTH)
            {
                return false;
            }
            return id.IndexOf(AMQP_ULONG_PREFIX, offset, AMQP_ULONG_PREFIX_LENGTH) - offset == 0;
        }

        private static bool HasAMQPUuidPrefix(string id, int offset)
        {
            if (id.Length - offset < AMQP_UUID_PREFIX_LENGTH)
            {
                return false;
            }
            return id.IndexOf(AMQP_UUID_PREFIX, offset, AMQP_UUID_PREFIX_LENGTH) - offset == 0;
        }

        private static bool HasEncodingTypePrefix(string id, int offset)
        {
            if (id.Length - offset < AMQP_NO_PREFIX_LENGTH)
            {
                return false;
            }
            if (id.IndexOf(AMQP_TYPE,offset, AMQP_TYPE_LENGTH) != 0)
            {
                return false;
            }
            return HasAMQPBinaryPrefix(id, offset) ||
                HasAMQPNoPrefix(id, offset) ||
                HasAMQPStringPrefix(id, offset) ||
                HasAMQPULongPrefix(id, offset) ||
                HasAMQPUuidPrefix(id, offset);
        } 

        private static string ToHexString(byte[] bytes)
        {
            if(bytes == null)
            {
                return null;
            }
            StringBuilder sb = new StringBuilder(bytes.Length);
            foreach(byte b in bytes)
            {
                int upper = (b >> 4) & 0x0F;
                int lower = b & 0x0F;
                sb.Append(HEX_CHARS[upper]);
                sb.Append(HEX_CHARS[lower]);
            }
            return sb.ToString();
        }

        private static byte[] ToByteArray(string hex, string originalId)
        {
            if (hex == null || hex.Length % 2 != 0)
            {
                throw new NMSException("Invalid Binary MessageId " + originalId);
            }
            int size = hex.Length / 2;
            int index = 0;
            byte[] result = new byte[size];
            for (int i=0; i<result.Length; i++)
            {
                char upper = hex[index];
                index++;
                char lower = hex[index];
                index++;
                char[] subchars = { upper, lower };
                string substring = new string(subchars);
                result[i] = Convert.ToByte(substring, 16);
            }
            return result;
        }

    }
}
