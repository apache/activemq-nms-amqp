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
using System.Text;
using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP.Provider.Amqp
{
    public static class AmqpMessageIdHelper
    {
        // Message Id constants
        public const string NMS_ID_PREFIX = "ID:";
        public const string AMQP_STRING_PREFIX = "AMQP_STRING:";
        public const string AMQP_ULONG_PREFIX = "AMQP_ULONG:";
        public const string AMQP_BINARY_PREFIX = "AMQP_BINARY:";
        public const string AMQP_UUID_PREFIX = "AMQP_UUID:";
        public const string AMQP_NO_PREFIX = "AMQP_NO_PREFIX:";

        public const string AMQP_TYPE = "AMQP_";
        public static readonly int NMS_ID_PREFIX_LENGTH = NMS_ID_PREFIX.Length;
        public static readonly int AMQP_TYPE_LENGTH = AMQP_TYPE.Length;
        private static readonly int AMQP_STRING_PREFIX_LENGTH = AMQP_STRING_PREFIX.Length;
        private static readonly int AMQP_BINARY_PREFIX_LENGTH = AMQP_BINARY_PREFIX.Length;
        private static readonly int AMQP_ULONG_PREFIX_LENGTH = AMQP_ULONG_PREFIX.Length;
        private static readonly int AMQP_UUID_PREFIX_LENGTH = AMQP_UUID_PREFIX.Length;
        public static readonly int AMQP_NO_PREFIX_LENGTH = AMQP_NO_PREFIX.Length;
        private static readonly char[] HEX_CHARS = "0123456789ABCDEF".ToCharArray();

        public static bool HasMessageIdPrefix(string id)
        {
            if (id == null)
            {
                return false;
            }

            return id.StartsWith(NMS_ID_PREFIX);
        }

        public static string ToCorrelationIdString(object idObject)
        {
            if (idObject is string stringId)
            {
                bool hasMessageIdPrefix = HasMessageIdPrefix(stringId);
                if (!hasMessageIdPrefix)
                {
                    // For JMSCorrelationID, has no "ID:" prefix, use it as-is.
                    return stringId;
                }
                else if (HasTypeEncodingPrefix(stringId, NMS_ID_PREFIX_LENGTH))
                {
                    // We are for a JMSCorrelationID value, but have 'ID:' followed by
                    // one of the encoding prefixes. Need to escape the entire string
                    // to preserve for later re-use as a JMSCorrelationID.
                    return NMS_ID_PREFIX + AMQP_STRING_PREFIX + stringId;
                }
                else
                {
                    // It has "ID:" prefix and doesn't have encoding prefix, use it as-is.
                    return stringId;
                }
            }
            else
            {
                // Not a string, convert it
                return ConvertToIdString(idObject);
            }
        }

        public static string ToMessageIdString(object idObject)
        {
            if (idObject is string stringId)
            {
                bool hasMessageIdPrefix = HasMessageIdPrefix(stringId);
                if (!hasMessageIdPrefix)
                {
                    // For JMSMessageID, has no "ID:" prefix, we need to record
                    // that for later use as a JMSCorrelationID.
                    return NMS_ID_PREFIX + AMQP_NO_PREFIX + stringId;
                }
                else if (HasTypeEncodingPrefix(stringId, NMS_ID_PREFIX_LENGTH))
                {
                    // We are for a JMSMessageID value, but have 'ID:' followed by
                    // one of the encoding prefixes. Need to escape the entire string
                    // to preserve for later re-use as a JMSCorrelationID.
                    return NMS_ID_PREFIX + AMQP_STRING_PREFIX + stringId;
                }
                else
                {
                    // It has "ID:" prefix and doesn't have encoding prefix, use it as-is.
                    return stringId;
                }
            }
            else
            {
                // Not a string, convert it
                return ConvertToIdString(idObject);
            }
        }

        private static bool HasTypeEncodingPrefix(string stringId, int offset)
        {
            if (stringId.Length - offset < AMQP_NO_PREFIX_LENGTH)
            {
                return false;
            }

            if (stringId.IndexOf(AMQP_TYPE, offset, AMQP_TYPE_LENGTH) != 0)
            {
                return false;
            }

            return HasAmqpBinaryPrefix(stringId, offset) ||
                   HasAmqpUuidPrefix(stringId, offset) ||
                   HasAmqpUlongPrefix(stringId, offset) ||
                   HasAmqpStringPrefix(stringId, offset) ||
                   HasAmqpNoPrefix(stringId, offset);
        }

        private static bool HasAmqpBinaryPrefix(string id, int offset)
        {
            if (id.Length - offset < AMQP_BINARY_PREFIX_LENGTH)
            {
                return false;
            }

            return id.IndexOf(AMQP_BINARY_PREFIX, offset, AMQP_BINARY_PREFIX_LENGTH) - offset == 0;
        }
        
        private static bool HasAmqpUuidPrefix(string id, int offset)
        {
            if (id.Length - offset < AMQP_UUID_PREFIX_LENGTH)
            {
                return false;
            }

            return id.IndexOf(AMQP_UUID_PREFIX, offset, AMQP_UUID_PREFIX_LENGTH) - offset == 0;
        }
        
        private static bool HasAmqpUlongPrefix(string id, int offset)
        {
            if (id.Length - offset < AMQP_ULONG_PREFIX_LENGTH)
            {
                return false;
            }

            return id.IndexOf(AMQP_ULONG_PREFIX, offset, AMQP_ULONG_PREFIX_LENGTH) - offset == 0;
        }
        
        private static bool HasAmqpStringPrefix(string id, int offset)
        {
            if (id.Length - offset < AMQP_STRING_PREFIX_LENGTH)
            {
                return false;
            }

            return id.IndexOf(AMQP_STRING_PREFIX, offset, AMQP_STRING_PREFIX_LENGTH) - offset == 0;
        }
        
        private static bool HasAmqpNoPrefix(string id, int offset)
        {
            if (id.Length - offset < AMQP_NO_PREFIX_LENGTH)
            {
                return false;
            }

            return id.IndexOf(AMQP_NO_PREFIX, offset, AMQP_NO_PREFIX_LENGTH) - offset == 0;
        }

        private static string ConvertToIdString(object idObject)
        {
            if (null == idObject)
            {
                return null;
            }
            else if (idObject is Guid)
            {
                return NMS_ID_PREFIX + AMQP_UUID_PREFIX + idObject.ToString();
            }
            else if (idObject is ulong)
            {
                return NMS_ID_PREFIX + AMQP_ULONG_PREFIX + idObject.ToString();
            }
            else if (idObject is byte[] bytes)
            {
                return NMS_ID_PREFIX + AMQP_BINARY_PREFIX + ConvertBinaryToHexString(bytes);
            }
            else
            {
                throw new NMSException("Unsupported Id Type provided: ", idObject.GetType().FullName);
            }
        }
        
        /// <summary>
        /// Takes the provided id string and return the appropriate amqp messageId style object.
        /// Converts the type based on any relevant encoding information found as a prefix.
        /// </summary>
        public static object ToIdObject(string origId)
        {
            if (origId == null) {
                return null;
            }
            
            if (!HasMessageIdPrefix(origId)) {
                // We have a string without any "ID:" prefix, it is an
                // application-specific String, use it as-is.
                return origId;
            }
            
            try
            {
                if (HasAmqpNoPrefix(origId, NMS_ID_PREFIX_LENGTH))
                {
                    // Prefix telling us there was originally no "ID:" prefix,
                    // strip it and return the remainder
                    return origId.Substring(NMS_ID_PREFIX_LENGTH + AMQP_NO_PREFIX_LENGTH);
                }
                else if (HasAmqpStringPrefix(origId, NMS_ID_PREFIX_LENGTH))
                {
                    return origId.Substring(NMS_ID_PREFIX_LENGTH + AMQP_STRING_PREFIX_LENGTH);
                }
                else if (HasAmqpBinaryPrefix(origId, NMS_ID_PREFIX_LENGTH))
                {
                    string hexString = origId.Substring(NMS_ID_PREFIX_LENGTH + AMQP_BINARY_PREFIX_LENGTH).ToUpper();
                    return ConvertHexStringToBinary(hexString, origId);
                }
                else if (HasAmqpUlongPrefix(origId, NMS_ID_PREFIX_LENGTH))
                {
                    string ulongString = origId.Substring(NMS_ID_PREFIX_LENGTH + AMQP_ULONG_PREFIX_LENGTH);
                    return Convert.ToUInt64(ulongString);
                }
                else if (HasAmqpUuidPrefix(origId, NMS_ID_PREFIX_LENGTH))
                {
                    string guidString = origId.Substring(NMS_ID_PREFIX_LENGTH + AMQP_UUID_PREFIX_LENGTH);
                    return Guid.Parse(guidString);
                }
                else
                {
                    // We have a string without any encoding prefix needing processed,
                    // so transmit it as-is, including the "ID:"
                    return origId;
                }
            }
            catch (Exception e)
            {
                throw new NMSException("Id Conversion Failure. Provided Id: " + origId, e);
            }
        }

        public static string ConvertBinaryToHexString(byte[] bytes)
        {
            // Each byte is represented as 2 chars
            StringBuilder builder = new StringBuilder(bytes.Length * 2);
            foreach (byte b in bytes)
            {
                // The byte will be expanded to int before shifting, replicating the
                // sign bit, so mask everything beyond the first 4 bits afterwards
                int upper = (b >> 4) & 0x0F;
                
                // We only want the first 4 bits
                int lower = b & 0x0F;
                
                builder.Append(HEX_CHARS[upper]);
                builder.Append(HEX_CHARS[lower]);
            }

            return builder.ToString();
        }
        
        private static byte[] ConvertHexStringToBinary(string hex, string originalId)
        {
            // As each byte needs two characters in the hex encoding, the string must be an even length.
            if (hex == null || hex.Length % 2 != 0)
            {
                throw new NMSException("Invalid Binary MessageId " + originalId);
            }

            int size = hex.Length / 2;
            int index = 0;
            byte[] result = new byte[size];
            for (int i = 0; i < result.Length; i++)
            {
                char upper = hex[index];
                index++;
                char lower = hex[index];
                index++;
                char[] subchars = {upper, lower};
                string substring = new string(subchars);
                result[i] = Convert.ToByte(substring, 16);
            }

            return result;
        }
    }
}