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
    public class NmsTransactionInfo : INmsResource<NmsTransactionId>
    {
        public NmsTransactionInfo(NmsSessionId sessionId, NmsTransactionId transactionId)
        {
            this.SessionId = sessionId ?? throw new ArgumentNullException(nameof(sessionId), "Session ID cannot be null");
            this.Id = transactionId ?? throw new ArgumentNullException(nameof(transactionId), "Transaction ID cannot be null");
        }

        public NmsTransactionId Id { get; }
        public NmsSessionId SessionId { get; }
        public bool IsInDoubt { get; private set; }
        public byte[] ProviderTxId { get; set; }

        public void SetInDoubt()
        {
            IsInDoubt = true;
        }

        protected bool Equals(NmsTransactionInfo other)
        {
            return Equals(Id, other.Id);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((NmsTransactionInfo) obj);
        }

        public override int GetHashCode()
        {
            return (Id != null ? Id.GetHashCode() : 0);
        }
        
        public override string ToString()
        {
            return $"[{nameof(NmsTransactionInfo)}] {nameof(Id)}: {Id}";
        }
    }
}