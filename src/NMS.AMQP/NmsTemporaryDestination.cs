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

using Apache.NMS.AMQP.Meta;
using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP
{
    public abstract class NmsTemporaryDestination :  IDestination, INmsResource
    {
        protected NmsTemporaryDestination(string address)
        {
            this.Address = address;
        }

        public string Address { get; set; }
        public abstract DestinationType DestinationType { get; }
        public abstract bool IsTopic { get; }
        public abstract bool IsQueue { get; }
        public abstract bool IsTemporary { get; }
        public NmsConnection Connection { get; set; }
        
        public void Dispose()
        {
            if (Connection != null)
            {
                Connection.DeleteTemporaryDestination(this);
                Connection = null;
            }
        }
    }
}