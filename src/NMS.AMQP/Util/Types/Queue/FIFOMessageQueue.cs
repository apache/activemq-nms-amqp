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
using System.Collections;
using System.Collections.Generic;
using Apache.NMS.AMQP;
using Apache.NMS.AMQP.Message;
using System.Threading;

namespace Apache.NMS.AMQP.Util.Types.Queue
{
    internal class FIFOMessageQueue : MessageQueueBase
    {
        
        protected LinkedList<IMessageDelivery> list;
        
        internal FIFOMessageQueue() : base()
        {
            list = new LinkedList<IMessageDelivery>();
        }
        

        public override int Count { get { return list.Count; } }

        
        public override void Clear()
        {
            list.Clear();
        }

        public override void CopyTo(Array array, int index)
        {
            int i = index;
            lock (SyncRoot)
            {
                foreach (MessageDelivery message in list)
                {
                    array.SetValue(message, i);
                    i++;
                }
            }
        }

        public override void Enqueue(IMessageDelivery message)
        {
            lock (SyncRoot)
            {
                list.AddLast(message);
                Monitor.PulseAll(SyncRoot);
            }
        }

        public override void EnqueueFirst(IMessageDelivery message)
        {
            lock (SyncRoot)
            {
                list.AddFirst(message);
                Monitor.PulseAll(SyncRoot);
            }
        }

        public override IList<IMessageDelivery> RemoveAll()
        {
            lock (SyncRoot)
            {
                IList<IMessageDelivery> result = new List<IMessageDelivery>(this.Count);
                foreach(MessageDelivery message in list)
                {
                    result.Add(message);
                }
                list.Clear();
                return result;
            }
        }

        protected override IMessageDelivery PeekFirst()
        {
            return list.First.Value;
        }

        protected override IMessageDelivery RemoveFirst()
        {
            IMessageDelivery first = list.First.Value;
            list.RemoveFirst();
            return first;
        }
        
    }
}
