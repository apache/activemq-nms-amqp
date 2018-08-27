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
using System.Net.Sockets;
using System.Collections.Specialized;
using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP.Transport.AMQP
{

    #region Transport Context

    /// <summary>
    /// Transport management is mainly handled by the AmqpNetLite library, Except for custom transports.
    /// TransportContext should configure the Amqp.ConnectionFactory for the tcp transport properties.
    /// </summary>
    internal class TransportContext : IProviderTransportContext
    {
        protected ConnectionFactory factory = null;

        protected Amqp.ConnectionFactory connectionBuilder = null;
        
        internal TransportContext(ConnectionFactory connectionFactory)
        {
            factory = connectionFactory;
            connectionBuilder = factory.Factory as Amqp.ConnectionFactory;
            connectionBuilder.SASL.Profile = Amqp.Sasl.SaslProfile.Anonymous;
        }

        // Copy Contructor
        protected TransportContext() { }

        public Apache.NMS.IConnectionFactory Owner { get { return factory; } }

        #region Transport Options

        public int ReceiveBufferSize { get => this.connectionBuilder.TCP.ReceiveBufferSize; set => this.connectionBuilder.TCP.ReceiveBufferSize = value; }
        public int ReceiveTimeout { get => this.connectionBuilder.TCP.ReceiveTimeout; set => this.connectionBuilder.TCP.ReceiveTimeout = value; }
        public int SendBufferSize { get => this.connectionBuilder.TCP.SendBufferSize; set => this.connectionBuilder.TCP.SendBufferSize = value; }
        public int SendTimeout { get => this.connectionBuilder.TCP.SendTimeout; set => this.connectionBuilder.TCP.SendTimeout = value; }
        public bool TcpNoDelayEnabled { get => this.connectionBuilder.TCP.NoDelay; set => this.connectionBuilder.TCP.NoDelay = value; }
        public uint TcpKeepAliveTime { get => this.connectionBuilder.TCP.KeepAlive.KeepAliveTime; set => this.connectionBuilder.TCP.KeepAlive.KeepAliveTime = value; }
        public uint TcpKeepAliveInterval { get => this.connectionBuilder.TCP.KeepAlive.KeepAliveInterval; set => this.connectionBuilder.TCP.KeepAlive.KeepAliveInterval = value; }

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

        /// <summary>
        /// UseLogging Enables AmqpNetLite's Frame logging level.
        /// </summary>
        public bool UseLogging
        {
            get => ((Amqp.Trace.TraceLevel & Amqp.TraceLevel.Frame) == Amqp.TraceLevel.Frame);
            set
            {
                if (value)
                {
                    Amqp.Trace.TraceLevel = Amqp.Trace.TraceLevel | Amqp.TraceLevel.Frame;
                }
                else
                {
                    Amqp.Trace.TraceLevel = Amqp.Trace.TraceLevel & ~Amqp.TraceLevel.Frame;
                }
            }
        }
        
        #endregion

        public virtual ProviderCreateConnection CreateConnectionBuilder()
        {
            return new ProviderCreateConnection(connectionBuilder.CreateAsync);
        }

        public virtual IProviderTransportContext Copy()
        {
            TransportContext copy = new TransportContext();
            this.CopyInto(copy);
            return copy;
        }

        protected virtual void CopyInto(TransportContext copy)
        {
            copy.factory = this.factory;
            copy.UseLogging = this.UseLogging;
            Amqp.ConnectionFactory builder = new Amqp.ConnectionFactory();
            this.CopyBuilder(builder);
            copy.connectionBuilder = builder;
        }

        protected virtual void CopyBuilder(Amqp.ConnectionFactory copy)
        {
            StringDictionary amqpProperties = PropertyUtil.GetProperties(this.connectionBuilder.AMQP);
            StringDictionary tcpProperties = PropertyUtil.GetProperties(this.connectionBuilder.TCP);
            PropertyUtil.SetProperties(copy.AMQP, amqpProperties);
            PropertyUtil.SetProperties(copy.TCP, tcpProperties);
            copy.SASL.Profile = this.connectionBuilder.SASL.Profile;
        }

    }

    #endregion

}
