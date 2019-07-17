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

using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP.Meta
{
    public class ConsumerInfo : LinkInfo
    {
        public Id SessionId { get; }

        protected const int DEFAULT_CREDIT = 200;

        private int? credit = null;

        internal ConsumerInfo(Id id, Id sessionId) : base(id)
        {
            SessionId = sessionId;
        }

        public int LinkCredit
        {
            get { return credit ?? DEFAULT_CREDIT; }
            internal set { credit = value; }
        }

        public string Selector { get; internal set; } = null;
        public string SubscriptionName { get; internal set; } = null;

        public bool NoLocal { get; internal set; } = false;
        public bool HasSelector => !string.IsNullOrEmpty(Selector);
        public bool IsDurable { get; set; }
        public bool IsBrowser { get; set; }
        public IDestination Destination { get; set; }
    }
}