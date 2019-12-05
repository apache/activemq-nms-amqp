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

namespace Apache.NMS.AMQP
{
    public class NmsTemporaryTopic : NmsTemporaryDestination, ITemporaryTopic
    {
        public NmsTemporaryTopic(string topicName) : base(topicName)
        {
        }

        public override DestinationType DestinationType { get; } = DestinationType.TemporaryTopic;
        public override bool IsTopic { get; } = true;
        public override bool IsQueue { get; } = false;
        public override bool IsTemporary { get; } = true;
        public string TopicName => Address;
        
        public void Delete()
        {
            Dispose();
        }

        protected bool Equals(NmsTemporaryTopic other)
        {
            return TopicName == other.TopicName && DestinationType == other.DestinationType && IsTopic == other.IsTopic && IsQueue == other.IsQueue && IsTemporary == other.IsTemporary;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((NmsTemporaryTopic) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (int) DestinationType;
                hashCode = (hashCode * 397) ^ (TopicName != null ? TopicName.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ IsTopic.GetHashCode();
                hashCode = (hashCode * 397) ^ IsQueue.GetHashCode();
                hashCode = (hashCode * 397) ^ IsTemporary.GetHashCode();
                return hashCode;
            }
        }

        public override string ToString()
        {
            return $"{nameof(TopicName)}: {TopicName}";
        }
    }
}