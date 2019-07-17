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

namespace Apache.NMS.AMQP.Provider.Failover
{
    public class FailoverUriPool
    {
        private readonly object syncRoot = new object();

        private readonly LinkedList<Uri> uris;
        public static bool DEFAULT_RANDOMIZE_ENABLED = false;

        public FailoverUriPool(IEnumerable<Uri> uris)
        {
            this.uris = new LinkedList<Uri>(uris);
        }

        public FailoverUriPool()
        {
            uris = new LinkedList<Uri>();
        }

        public bool IsRandomize { get; set; }

        public Uri GetNext()
        {
            Uri next = null;
            lock (syncRoot)
            {
                if (uris.Any())
                {
                    next = uris.First();
                    uris.RemoveFirst();
                    uris.AddLast(next);
                }
            }

            return next;
        }

        public int Size()
        {
            lock (syncRoot)
            {
                return uris.Count;
            }
        }

        public bool Any()
        {
            lock (syncRoot)
            {
                return uris.Count > 0;
            }
        }
    }
}