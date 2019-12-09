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
using System.Net;
using System.Threading;

namespace Apache.NMS.AMQP.Util
{
    internal class IdGenerator
    {
        public const string DEFAULT_PREFIX = "ID:";

        private readonly string prefix;
        private long sequence;
        private static string hostName;

        static IdGenerator()
        {
            try
            {
                 hostName = Dns.GetHostName();
            }
            catch (Exception e)
            {
                Tracer.Error($"Could not generate host name prefix from DNS lookup: {e}");
            }
            
            hostName = SanitizeHostName(hostName);
        }

        public IdGenerator(string prefix = DEFAULT_PREFIX)
        {
            this.prefix = prefix + (hostName != null ? hostName + ":" : "");
        }

        public string GenerateId() => $"{prefix}{Guid.NewGuid().ToString()}:{Interlocked.Increment(ref sequence).ToString()}";

        public static string SanitizeHostName(string hostName)
        {
            var sanitizedHostname = string.Concat(GetASCIICharacters(hostName));
            if (sanitizedHostname.Length != hostName.Length)
            {
                Tracer.Info($"Sanitized hostname from: {hostName} to: {sanitizedHostname}");
            }

            return sanitizedHostname;
        }

        private static IEnumerable<char> GetASCIICharacters(string hostName) => hostName.Where(ch => ch < 127);
    }
}