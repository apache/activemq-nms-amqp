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

namespace Apache.NMS.AMQP
{
    public class NmsSharedDurableMessageConsumer : NmsMessageConsumer
    {
        public NmsSharedDurableMessageConsumer(NmsConsumerId consumerId, NmsSession session, IDestination destination, string selector, bool noLocal) : base(consumerId, session, destination, selector, noLocal)
        {
        }

        public NmsSharedDurableMessageConsumer(NmsConsumerId consumerId, NmsSession session, IDestination destination, string name, string selector, bool noLocal) : base(consumerId, session, destination, name, selector, noLocal)
        {
        }

        protected override bool IsDurableSubscription => true;
        
        protected override bool IsSharedSubscription => true;

    }
}