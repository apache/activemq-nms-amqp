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
using Amqp;
using Amqp.Framing;
using Amqp.Types;
using NMS.AMQP.Test.TestAmqp.BasicTypes;

namespace NMS.AMQP.Test.TestAmqp.Matchers
{
    public class FrameContext
    {
        private readonly Stream stream;

        public FrameContext(Stream stream, ushort channel)
        {
            this.stream = stream;
            Channel = channel;
        }

        public ushort Channel { get; }

        public void SendCommand(DescribedList command, FrameType type = FrameType.Amqp) => 
            Send(buffer => FrameEncoder.Encode(buffer, type, Channel, command));

        public void SendCommand(ushort channel, DescribedList command, FrameType type = FrameType.Amqp) =>
            Send(buffer => FrameEncoder.Encode(buffer, type, channel, command));

        public void SendCommand(Transfer transfer, ByteBuffer payload) =>
            Send(buffer => FrameEncoder.Encode(buffer, Channel, transfer, payload));

        private void Send(Action<ByteBuffer> encode)
        {
            ByteBuffer buffer = new ByteBuffer(128, true);
            encode(buffer);
            stream.Write(buffer.Buffer, buffer.Offset, buffer.Length);
        }
    }

    public class FrameContext<T> : FrameContext
    {
        public T Command { get; }

        public FrameContext(Stream stream, ushort channel, T command) : base(stream, channel)
        {
            Command = command;
        }
    }
}