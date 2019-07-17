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
    public abstract class LinkInfo : ResourceInfo
    {
        protected static readonly long DEFAULT_REQUEST_TIMEOUT;
        static LinkInfo()
        {
            DEFAULT_REQUEST_TIMEOUT = Convert.ToInt64(NMSConstants.defaultRequestTimeout.TotalMilliseconds);
        }

        protected LinkInfo(Id linkId) : base(linkId)
        {

        }

        public long requestTimeout { get; set; } = DEFAULT_REQUEST_TIMEOUT;
        public long closeTimeout { get; set; } = DEFAULT_REQUEST_TIMEOUT;
        public long sendTimeout { get; set; }

        public override string ToString()
        {
            string result = "";
            result += "LinkInfo = [\n";
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