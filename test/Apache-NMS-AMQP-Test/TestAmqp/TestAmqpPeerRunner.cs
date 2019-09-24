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
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using Amqp;
using Amqp.Framing;
using Amqp.Types;
using NLog.Fluent;
using NMS.AMQP.Test.TestAmqp.BasicTypes;

namespace NMS.AMQP.Test.TestAmqp
{
    public class TestAmqpPeerRunner
    {
        private static readonly NLog.Logger Logger = NLog.LogManager.GetCurrentClassLogger();
        
        private readonly TestAmqpPeer testAmqpPeer;
        private readonly IPEndPoint ip;
        private Socket socket;
        private SocketAsyncEventArgs args;
        private Socket acceptSocket;

        public TestAmqpPeerRunner(TestAmqpPeer testAmqpPeer, IPEndPoint ipEndPoint)
        {
            this.testAmqpPeer = testAmqpPeer;
            this.ip = ipEndPoint;
        }

        public int Port => ((IPEndPoint) this.socket?.LocalEndPoint)?.Port ?? 0;
        public Socket ClientSocket => acceptSocket;

        public void Open()
        {
            this.args = new SocketAsyncEventArgs();
            this.args.Completed += this.OnAccept;

            this.socket = new Socket(this.ip.AddressFamily, SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
            this.socket.Bind(this.ip);
            this.socket.Listen(20);

            this.Accept();
        }

        void OnAccept(object sender, SocketAsyncEventArgs args)
        {
            if (args.SocketError == SocketError.Success)
            {
                this.acceptSocket = args.AcceptSocket;
                
                Socket s = args.AcceptSocket;
                s.NoDelay = true;

                Task.Factory.StartNew(() => this.Pump(new NetworkStream(s, true)));
            }

            bool sync = args.UserToken != null;
            args.UserToken = null;
            args.AcceptSocket = null;
            if (!sync)
            {
                this.Accept();
            }
        }

        void Accept()
        {
            Socket s = this.socket;
            while (s != null)
            {
                try
                {
                    if (this.socket.AcceptAsync(this.args))
                    {
                        break;
                    }

                    this.args.UserToken = "sync";
                    this.OnAccept(s, this.args);
                }
                catch
                {
                }

                s = this.socket;
            }
        }

        void Pump(Stream stream)
        {
            try
            {
                while (true)
                {
                    byte[] buffer = new byte[8];
                    Read(stream, buffer, 0, 8);
                    testAmqpPeer.OnHeader(stream, buffer);

                    while (true)
                    {
                        Read(stream, buffer, 0, 4);
                        int len = AmqpBitConverter.ReadInt(buffer, 0);
                        byte[] frame = new byte[len - 4];
                        Read(stream, frame, 0, frame.Length);
                        if (!OnFrame(stream, new ByteBuffer(frame, 0, frame.Length, frame.Length)))
                        {
                            break;
                        }
                    }
                }
            }
            catch(Exception e)
            {
                Logger.Info(e);
                stream.Dispose();
            }
        }

        static void Read(Stream stream, byte[] buffer, int offset, int count)
        {
            while (count > 0)
            {
                int bytes = stream.Read(buffer, offset, count);
                if (bytes == 0)
                {
                    throw new ObjectDisposedException("socket");
                }

                offset += bytes;
                count -= bytes;
            }
        }

        bool OnFrame(Stream stream, ByteBuffer buffer)
        {
            buffer.Complete(1);
            byte type = AmqpBitConverter.ReadUByte(buffer);
            ushort channel = AmqpBitConverter.ReadUShort(buffer);
            DescribedList command = (DescribedList) Encoder.ReadDescribed(buffer, Encoder.ReadFormatCode(buffer));

            Amqp.Message message = null;
            if (command.Descriptor.Code == FrameCodes.TRANSFER)
            {
                message = Amqp.Message.Decode(buffer);
            }

            return testAmqpPeer.OnFrame(stream, channel, command, message);
        }

        public void Send(ushort channel, DescribedList command, FrameType type = FrameType.Amqp)
        {
            ByteBuffer buffer = new ByteBuffer(128, true);
            FrameEncoder.Encode(buffer, type, channel, command);
            this.acceptSocket.Send(buffer.Buffer, buffer.Offset, buffer.Length, SocketFlags.None);
        }

        public void Close()
        {
            acceptSocket?.Dispose();
            acceptSocket = null;
            
            Socket s = socket;
            socket = null;
            s?.Dispose();
            args?.Dispose();
        }
    }
}