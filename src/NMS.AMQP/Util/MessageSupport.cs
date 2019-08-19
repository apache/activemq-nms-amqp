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
using System.Threading.Tasks;
using Apache.NMS;
using Amqp.Framing;
using Amqp.Transactions;
using Amqp.Types;
using Apache.NMS.AMQP.Message;

namespace Apache.NMS.AMQP.Util
{
    /// <summary>
    /// Utility class containing constant values for NMS message fields, and values.
    /// Also contains Utility methods for NMS messagId/correlationId and Destinations.
    /// </summary>
    public class MessageSupport
    {
        // "x-opt-jms-msg-type" values
        public const sbyte JMS_TYPE_MSG = 0x00;
        public const sbyte JMS_TYPE_OBJ = 0x01;
        public const sbyte JMS_TYPE_MAP = 0x02;
        public const sbyte JMS_TYPE_BYTE = 0x03;
        public const sbyte JMS_TYPE_STRM = 0x04;
        public const sbyte JMS_TYPE_TXT = 0x05;

        // "x-opt-jms-dest" and "x-opt-jms-reply-to" values
        public const byte JMS_DEST_TYPE_QUEUE = 0x00;
        public const byte JMS_DEST_TYPE_TOPIC = 0x01;
        public const byte JMS_DEST_TYPE_TEMP_QUEUE = 0x02;
        public const byte JMS_DEST_TYPE_TEMP_TOPIC = 0x03;

        // Message Content-type values
        public const string OCTET_STREAM_CONTENT_TYPE = "application/octet-stream";
        public const string SERIALIZED_JAVA_OBJECT_CONTENT_TYPE = "application/x-java-serialized-object";
        public const string SERIALIZED_DOTNET_OBJECT_CONTENT_TYPE = "application/x-dotnet-serialized-object";

        // Amqp.Message priority default value
        public static readonly byte DEFAULT_PRIORITY_BYTE = Convert.ToByte((int) NMSConstants.defaultPriority);

        public static readonly Data EMPTY_DATA = new Data() {Binary = new byte[] { }};
        public static readonly AmqpValue NULL_AMQP_VALUE_BODY = new AmqpValue() {Value = null};

        // Amqp Message Outcome instances
        public static readonly Accepted ACCEPTED_INSTANCE = new Accepted();
        public static readonly Released RELEASED_INSTANCE = new Released();
        public static readonly Rejected REJECTED_INSTANCE = new Rejected();
        public static readonly Modified MODIFIED_INSTANCE = new Modified();
        public static readonly Modified MODIFIED_FAILED_INSTANCE = new Modified() {DeliveryFailed = true};
        public static readonly AckType DEFAULT_ACK_TYPE = AckType.ACCEPTED;
        
        // Amqp Transactions Outcome instances 
        public static readonly Declared DECLARED_INSTANCE = new Declared();

        public static byte GetValueForPriority(MsgPriority mp)
        {
            if ((int) mp > 9)
            {
                mp = MsgPriority.Highest;
            }

            return Convert.ToByte((int) mp);
        }

        public static MsgPriority GetPriorityFromValue(byte value)
        {
            if (value > 9)
                value = 9;

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
            }

            return result;
        }
    }
}