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
    public class NmsProducerId : INmsResourceId
    {
        private string key;

        public NmsProducerId(NmsSessionId sessionId, long producerId)
        {
            this.SessionId = sessionId ?? throw new ArgumentNullException(nameof(sessionId), "Session ID cannot be null");
            this.Value = producerId;
        }

        public long Value { get; }
        public NmsSessionId SessionId { get; }
        public NmsConnectionId ConnectionId => SessionId.ConnectionId;
        
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((NmsProducerId) obj);
        }
        
        protected bool Equals(NmsProducerId other)
        {
            return Value == other.Value && Equals(SessionId, other.SessionId);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (Value.GetHashCode() * 397) ^ (SessionId != null ? SessionId.GetHashCode() : 0);
            }
        }
        
        public override string ToString()
        {
            return key ?? (key = $"{ConnectionId}:{SessionId.Value.ToString()}:{Value.ToString()}");
        }
    }
}