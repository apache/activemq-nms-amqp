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

namespace Apache.NMS.AMQP.Meta
{
    public class NmsConsumerInfo : INmsResource<NmsConsumerId>
    {
        public static readonly int DEFAULT_CREDIT = 200;
        
        public NmsConsumerInfo(NmsConsumerId consumerId)
        {
            Id = consumerId ?? throw new ArgumentNullException(nameof(consumerId), "Consumer ID cannot be null");
        }

        public NmsConsumerId Id { get; }
        public NmsSessionId SessionId => Id.SessionId;
        public IDestination Destination { get; set; }
        public string Selector { get; set; }
        public bool NoLocal { get; set; }
        public string SubscriptionName { get; set; }
        public bool IsDurable { get; set; }
        public bool LocalMessageExpiry { get; set; }
        public bool IsBrowser { get; set; }
        public int LinkCredit { get; set; } = DEFAULT_CREDIT;
        
        public bool HasSelector() => !string.IsNullOrWhiteSpace(Selector);

        protected bool Equals(NmsConsumerInfo other)
        {
            return Equals(Id, other.Id);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((NmsConsumerInfo) obj);
        }

        public override int GetHashCode()
        {
            return (Id != null ? Id.GetHashCode() : 0);
        }
        
        public override string ToString()
        {
            return $"[{nameof(NmsConsumerInfo)}] {nameof(Id)}: {Id}, {nameof(Destination)}: {Destination}";
        }
    }
}