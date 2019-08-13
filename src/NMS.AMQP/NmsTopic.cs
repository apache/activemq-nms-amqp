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
    public class NmsTopic : ITopic
    {
        public NmsTopic(string name)
        {
            TopicName = name;
        }

        public void Dispose()
        {
        }

        public DestinationType DestinationType { get; } = DestinationType.Topic;
        public bool IsTopic { get; } = true;
        public bool IsQueue { get; } = false;
        public bool IsTemporary { get; } = false;
        public string TopicName { get; }

        protected bool Equals(NmsTopic other)
        {
            return DestinationType == other.DestinationType && IsTopic == other.IsTopic && IsQueue == other.IsQueue && IsTemporary == other.IsTemporary && TopicName == other.TopicName;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((NmsTopic) obj);
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