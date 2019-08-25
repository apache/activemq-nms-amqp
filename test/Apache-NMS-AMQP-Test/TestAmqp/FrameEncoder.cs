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

using Amqp;
using Amqp.Framing;
using Amqp.Types;
using NMS.AMQP.Test.TestAmqp.BasicTypes;

namespace NMS.AMQP.Test.TestAmqp
{
    public static class FrameEncoder
    {
        public static void Encode(ByteBuffer buffer, ushort channel, Transfer transfer, ByteBuffer payload)
        {
            Encode(buffer, FrameType.Amqp, channel, transfer);
            int payloadSize = payload.Length;
            int frameSize = buffer.Length + payloadSize;
            AmqpBitConverter.WriteInt(buffer.Buffer, buffer.Offset, frameSize);
            AmqpBitConverter.WriteBytes(buffer, payload.Buffer, payload.Offset, payload.Length);
            payload.Complete(payload.Length);
        }

        public static void Encode(ByteBuffer buffer, FrameType type, ushort channel, DescribedList command)
        {
            buffer.Append(4);
            AmqpBitConverter.WriteUByte(buffer, 2);
            AmqpBitConverter.WriteUByte(buffer, (byte) type);
            AmqpBitConverter.WriteUShort(buffer, channel);
            command.Encode(buffer);
            AmqpBitConverter.WriteInt(buffer.Buffer, buffer.Offset, buffer.Length);
        }
    }
}