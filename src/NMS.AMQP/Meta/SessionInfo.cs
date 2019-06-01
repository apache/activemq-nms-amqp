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
using System.Reflection;
using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP.Meta
{
    public class SessionInfo : ResourceInfo
    {
        public static readonly uint DEFAULT_INCOMING_WINDOW = 1024 * 10 - 1;
        public static readonly uint DEFAULT_OUTGOING_WINDOW = uint.MaxValue - 2u;
        
        internal SessionInfo(Id sessionId) : base(sessionId)
        {
            ulong endId = (ulong)sessionId.GetLastComponent(typeof(ulong));
            nextOutgoingId = Convert.ToUInt16(endId);
        }
        
        public string sessionId { get { return Id.ToString(); } }

        public AcknowledgementMode ackMode { get; set; }
        public ushort remoteChannel { get; internal set; }
        public uint nextOutgoingId { get; internal set; }
        public uint incomingWindow { get; set; } = DEFAULT_INCOMING_WINDOW;
        public uint outgoingWindow { get; set; } = DEFAULT_OUTGOING_WINDOW;
        public bool isTransacted { get => false;  set { } }
        public long requestTimeout { get; set; }
        public int closeTimeout { get; set; }
        public long sendTimeout { get; set; }

        public override string ToString()
        {
            string result = "";
            result += "sessInfo = [\n";
            foreach (MemberInfo info in this.GetType().GetMembers())
            {
                if (info is PropertyInfo)
                {
                    PropertyInfo prop = info as PropertyInfo;
                    if (prop.GetGetMethod(true).IsPublic)
                    {
                        result += string.Format("{0} = {1},\n", prop.Name, prop.GetValue(this, null));
                    }
                }
            }
            result = result.Substring(0, result.Length - 2) + "\n]";
            return result;
        }
    }
}