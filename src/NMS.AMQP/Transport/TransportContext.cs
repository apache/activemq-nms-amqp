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

using System.Collections.Specialized;
using System.Net.Sockets;
using System.Threading.Tasks;
using Amqp;
using Amqp.Framing;
using Amqp.Handler;
using Apache.NMS.AMQP.Provider.Amqp;
using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP.Transport
{
    /// <summary>
    /// Transport management is mainly handled by the AmqpNetLite library, Except for custom transports.
    /// TransportContext should configure the Amqp.ConnectionFactory for the tcp transport properties.
    /// </summary>
    internal class TransportContext : ITransportContext
    {
        protected readonly Amqp.ConnectionFactory connectionBuilder = null;
        
        internal TransportContext()
        {
            connectionBuilder = new Amqp.ConnectionFactory();
            connectionBuilder.SASL.Profile = Amqp.Sasl.SaslProfile.Anonymous;            
        }

        public int ReceiveBufferSize { get => this.connectionBuilder.TCP.ReceiveBufferSize; set => this.connectionBuilder.TCP.ReceiveBufferSize = value; }
        public int ReceiveTimeout { get => this.connectionBuilder.TCP.ReceiveTimeout; set => this.connectionBuilder.TCP.ReceiveTimeout = value; }
        public int SendBufferSize { get => this.connectionBuilder.TCP.SendBufferSize; set => this.connectionBuilder.TCP.SendBufferSize = value; }
        public int SendTimeout { get => this.connectionBuilder.TCP.SendTimeout; set => this.connectionBuilder.TCP.SendTimeout = value; }
        public bool TcpNoDelay { get => this.connectionBuilder.TCP.NoDelay; set => this.connectionBuilder.TCP.NoDelay = value; }

        public uint TcpKeepAliveTime
        {
            get => this.connectionBuilder.TCP.KeepAlive?.KeepAliveTime ?? default;
            set => this.TcpKeepAliveSettings.KeepAliveTime = value;
        }

        public uint TcpKeepAliveInterval
        {
            get => this.connectionBuilder.TCP.KeepAlive?.KeepAliveInterval ?? default;
            set => this.TcpKeepAliveSettings.KeepAliveInterval = value;
        }

        private TcpKeepAliveSettings TcpKeepAliveSettings => this.connectionBuilder.TCP.KeepAlive ?? (this.connectionBuilder.TCP.KeepAlive = new TcpKeepAliveSettings());

        public bool SocketLingerEnabled
        {
            get => this.connectionBuilder.TCP?.LingerOption.Enabled ?? (this.connectionBuilder.TCP.LingerOption = new LingerOption(false, 0)).Enabled;
            set
            {
                if (this.connectionBuilder.TCP.LingerOption == null)
                {
                    (this.connectionBuilder.TCP.LingerOption = new LingerOption(false, 0)).Enabled = value;
                }
                else
                {
                    this.connectionBuilder.TCP.LingerOption.Enabled = value;
                }
            }
        }

        public int SocketLingerTime
        {
            get => this.connectionBuilder.TCP?.LingerOption.LingerTime ?? (this.connectionBuilder.TCP.LingerOption = new LingerOption(false, 0)).LingerTime;
            set
            {
                if (this.connectionBuilder.TCP.LingerOption == null)
                {
                    (this.connectionBuilder.TCP.LingerOption = new LingerOption(false, 0)).LingerTime = value;
                }
                else
                {
                    this.connectionBuilder.TCP.LingerOption.LingerTime = value;
                }
            }
        }

        public virtual bool IsSecure { get; } = false;

        public virtual Task<Connection> CreateAsync(Address address, IHandler handler)
        {
            return connectionBuilder.CreateAsync(address, handler);    
        }
    }
}
