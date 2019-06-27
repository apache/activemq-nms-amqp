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
using Apache.NMS;

namespace Apache.NMS.AMQP.Util.Types.Queue
{
    internal class PriorityMessageQueue : MessageQueueBase
    {
        
        private LinkedList<IMessageDelivery>[] priorityList = new LinkedList<IMessageDelivery>[((int)MsgPriority.Highest)+1];
        private int count = 0;

        internal PriorityMessageQueue() : base()
        {
            for(int i=0;i<=(int)MsgPriority.Highest; i++)
            {
                LinkedList<IMessageDelivery> list = new LinkedList<IMessageDelivery>();
                priorityList[i] = list;
            }
        }

        public override int Count
        {
            get { return count; }
        }

        public override void Clear()
        {
            lock (SyncRoot)
            {
                foreach(LinkedList<IMessageDelivery> list in priorityList)
                {
                    list.Clear();
                }
                count = 0;
            }
        }

        public override void CopyTo(Array array, int index)
        {
            int i = index;
            lock (SyncRoot)
            {
                
                foreach (LinkedList<IMessageDelivery> list in priorityList)
                {
                    foreach(IMessageDelivery m in list)
                    {
                        array.SetValue(m, i);
                        i++;
                    }
                    
                }

            }
        }

        public override void Enqueue(IMessageDelivery message)
        {
            if (message.EnqueueFirst)
            {
                EnqueueFirst(message);
            }
            else
            {
                lock (SyncRoot)
                {

                    LinkedList<IMessageDelivery> list = priorityList[GetPriorityIndex(message)];
                    list.AddLast(message);
                    count++;
                    Monitor.PulseAll(SyncRoot);
                }
            }
        }

        public override void EnqueueFirst(IMessageDelivery message)
        {
            lock (SyncRoot)
            {
                priorityList[(int)MsgPriority.Highest].AddFirst(message);
                count++;
                Monitor.PulseAll(SyncRoot);
            }
        }

        public override IList<IMessageDelivery> RemoveAll()
        {
            if (IsClosed) return null;
            lock (SyncRoot)
            {
                IList<IMessageDelivery> result = new List<IMessageDelivery>(Count);
                foreach(LinkedList<IMessageDelivery> list in priorityList)
                {
                    foreach(MessageDelivery message in list)
                    {
                        result.Add(message);
                    }
                    count -= list.Count;
                    list.Clear();
                }
                return result;
            }
        }

        protected override IMessageDelivery PeekFirst()
        {
            if (!IsClosed && count > 0)
            {
                for(int i = (int)MsgPriority.Highest; i>=0; i--)
                {
                    LinkedList<IMessageDelivery> list = priorityList[i];
                    if(list.Count != 0)
                    {
                        return list.First.Value;
                    }
                }
            }
            return null;
        }

        protected override IMessageDelivery RemoveFirst()
        {
            if (!IsClosed && count > 0)
            {
                for (int i = (int)MsgPriority.Highest; i >= 0; i--)
                {
                    LinkedList<IMessageDelivery> list = priorityList[i];
                    if (list.Count != 0)
                    {
                        IMessageDelivery first = list.First.Value;
                        list.RemoveFirst();
                        count--;
                        return first;
                    }
                }
            }
            return null;
        }
        
        private MsgPriority GetPriority(IMessageDelivery message)
        {
            return message.Priority;
        }

        private int GetPriorityIndex(IMessageDelivery message)
        {
            return (int)GetPriority(message);
        }

    }

}
