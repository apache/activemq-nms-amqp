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
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Reflection;
using Amqp;
using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP.Meta
{
    public class ConnectionInfo : ResourceInfo
    {
        static ConnectionInfo()
        {
            Amqp.ConnectionFactory defaultCF = new Amqp.ConnectionFactory();
            AmqpSettings defaultAMQPSettings = defaultCF.AMQP;
            
            DEFAULT_CHANNEL_MAX = defaultAMQPSettings.MaxSessionsPerConnection;
            DEFAULT_MAX_FRAME_SIZE = defaultAMQPSettings.MaxFrameSize;
            DEFAULT_IDLE_TIMEOUT = defaultAMQPSettings.IdleTimeout;
            
            DEFAULT_REQUEST_TIMEOUT = Convert.ToInt64(NMSConstants.defaultRequestTimeout.TotalMilliseconds);

        }
        public const long INFINITE = -1;
        public const long DEFAULT_CONNECT_TIMEOUT = 15000;
        public const int DEFAULT_CLOSE_TIMEOUT = 15000;
        public static readonly long DEFAULT_REQUEST_TIMEOUT;
        public static readonly long DEFAULT_IDLE_TIMEOUT;
        public static readonly long DEFAULT_SEND_TIMEOUT = INFINITE;

        public static readonly ushort DEFAULT_CHANNEL_MAX;
        public static readonly int DEFAULT_MAX_FRAME_SIZE;
        

        public ConnectionInfo() : this(null) { }
        public ConnectionInfo(Id connectionId) : base(connectionId)
        {
        }

        internal Uri remoteHost { get; set; }
        public string ClientId { get; private set; }
        public string username { get; set; } = null;
        public string password { get; set; } = null;

        public long requestTimeout { get; set; } = DEFAULT_REQUEST_TIMEOUT;
        public long connectTimeout { get; set; } = DEFAULT_CONNECT_TIMEOUT;
        public int closeTimeout { get; set; } = DEFAULT_CLOSE_TIMEOUT;
        public long idleTimout { get; set; } = DEFAULT_IDLE_TIMEOUT;

        public long SendTimeout { get; set; } = DEFAULT_SEND_TIMEOUT;

        public ushort channelMax { get; set; } = DEFAULT_CHANNEL_MAX;
        public int maxFrameSize { get; set; } = DEFAULT_MAX_FRAME_SIZE;

        public bool LocalMessageExpiry { get; set; }

        public string TopicPrefix { get; internal set; } = null;

        public string QueuePrefix { get; internal set; } = null;

        public bool IsAnonymousRelay { get; internal set; } = false;

        public bool IsDelayedDelivery { get; internal set; } = false;

        public IList<string> Capabilities { get { return new List<string>(capabilities); } }

        public bool HasCapability(string capability)
        {
            return capabilities.Contains(capability);
        }

        public void AddCapability(string capability)
        {
            if (capability != null && capability.Length > 0)
                capabilities.Add(capability);
        }

        public StringDictionary RemotePeerProperies { get => remoteConnectionProperties; }
        public bool IsExplicitClientId { get; private set; }

        private StringDictionary remoteConnectionProperties = new StringDictionary();
        private List<string> capabilities = new List<string>();

        public override string ToString()
        {
            string result = "";
            result += "connInfo = [\n";
            foreach (MemberInfo info in this.GetType().GetMembers())
            {
                if (info is PropertyInfo)
                {
                    PropertyInfo prop = info as PropertyInfo;

                    if (prop.GetGetMethod(true).IsPublic)
                    {
                        if (prop.GetGetMethod(true).ReturnParameter.ParameterType.IsEquivalentTo(typeof(List<string>)))
                        {
                            result += string.Format("{0} = {1},\n", prop.Name, PropertyUtil.ToString(prop.GetValue(this,null) as IList));
                        }
                        else
                        {
                            result += string.Format("{0} = {1},\n", prop.Name, prop.GetValue(this, null));
                        }

                    }
                }
            }
            result = result.Substring(0, result.Length - 2) + "\n]";
            return result;
        }

        public void SetClientId(string clientId, bool explicitClientId)
        {
            this.ClientId = clientId;
            this.IsExplicitClientId = explicitClientId;
        }
    }
}