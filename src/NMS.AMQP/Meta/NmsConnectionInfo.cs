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
using Amqp;

namespace Apache.NMS.AMQP.Meta
{
    /// <summary>
    /// Meta object that contains the JmsConnection identification and configuration
    /// options. Providers can extend this to add Provider specific data as needed.
    /// </summary>
    public class NmsConnectionInfo : INmsResource<NmsConnectionId>
    {
        public static readonly long INFINITE = -1;
        public static readonly long DEFAULT_CONNECT_TIMEOUT = 15000;
        public static readonly long DEFAULT_CLOSE_TIMEOUT = 60000;
        public static readonly long DEFAULT_SEND_TIMEOUT = INFINITE;
        public static readonly long DEFAULT_REQUEST_TIMEOUT = INFINITE;
        public static readonly int DEFAULT_IDLE_TIMEOUT;
        public static readonly ushort DEFAULT_CHANNEL_MAX;
        public static readonly int DEFAULT_MAX_FRAME_SIZE;

        static NmsConnectionInfo()
        {
            AmqpSettings defaultAmqpSettings = new Amqp.ConnectionFactory().AMQP;
            DEFAULT_CHANNEL_MAX = defaultAmqpSettings.MaxSessionsPerConnection;
            DEFAULT_MAX_FRAME_SIZE = defaultAmqpSettings.MaxFrameSize;
            DEFAULT_IDLE_TIMEOUT = defaultAmqpSettings.IdleTimeout;
        }

        public NmsConnectionInfo(NmsConnectionId connectionId)
        {
            this.Id = connectionId ?? throw new ArgumentNullException(nameof(connectionId));
        }

        public NmsConnectionId Id { get; }
        public bool IsExplicitClientId { get; private set; }
        public string ClientId { get; private set; }
        public string UserName { get; set; }
        public string Password { get; set; }
        public Uri ConfiguredUri { get; set; }
        public long RequestTimeout { get; set; } = DEFAULT_REQUEST_TIMEOUT;
        public long SendTimeout { get; set; } = DEFAULT_SEND_TIMEOUT;
        public long CloseTimeout { get; set; } = DEFAULT_CLOSE_TIMEOUT;
        public bool LocalMessageExpiry { get; set; }
        public string QueuePrefix { get; set; }
        public string TopicPrefix { get; set; }
        public ushort ChannelMax { get; set; } = DEFAULT_CHANNEL_MAX;
        public int MaxFrameSize { get; set; } = DEFAULT_MAX_FRAME_SIZE;
        public int IdleTimeOut { get; set; } = DEFAULT_IDLE_TIMEOUT;
        

        public void SetClientId(string clientId, bool explicitClientId)
        {
            this.ClientId = clientId;
            this.IsExplicitClientId = explicitClientId;
        }

        protected bool Equals(NmsConnectionInfo other)
        {
            return Equals(Id, other.Id);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((NmsConnectionInfo) obj);
        }

        public override int GetHashCode()
        {
            return Id != null ? Id.GetHashCode() : 0;
        }

        public override string ToString()
        {
            return $"[{nameof(NmsConnectionInfo)}] {nameof(Id)}: {Id}, {nameof(ConfiguredUri)}: {ConfiguredUri}";
        }
    }
}