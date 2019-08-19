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
using System.Collections.Generic;
using System.Threading;
using Apache.NMS.AMQP.Message;

namespace Apache.NMS.AMQP.Util
{
    public class PriorityMessageQueue : IDisposable
    {
        private readonly LinkedList<InboundMessageDispatch>[] lists;

        private readonly object syncRoot = new object();

        private bool disposed;
        private int count;

        public PriorityMessageQueue()
        {
            lists = new LinkedList<InboundMessageDispatch>[(int) MsgPriority.Highest + 1];
            for (int i = 0; i < lists.Length; i++)
            {
                lists[i] = new LinkedList<InboundMessageDispatch>();
            }
        }

        public bool IsEmpty => Count == 0;

        public int Count
        {
            get
            {
                lock (syncRoot)
                    return count;
            }
        }

        public InboundMessageDispatch DequeueNoWait()
        {
            return Dequeue(0);
        }

        private InboundMessageDispatch RemoveFirst()
        {
            if (count > 0)
            {
                for (int i = (int) MsgPriority.Highest; i >= 0; i--)
                {
                    LinkedList<InboundMessageDispatch> list = lists[i];
                    if (list.Count > 0)
                    {
                        count--;
                        InboundMessageDispatch envelope = list.First.Value;
                        list.RemoveFirst();
                        return envelope;
                    }
                }
            }

            return null;
        }

        public void Enqueue(InboundMessageDispatch envelope)
        {
            lock (syncRoot)
            {
                GetList(envelope).AddLast(envelope);
                this.count++;
                Monitor.Pulse(syncRoot);
            }
        }
        
        public void EnqueueFirst(InboundMessageDispatch envelope)
        {
            lock (syncRoot)
            {
                lists[(int) MsgPriority.Highest].AddFirst(envelope);
                count++;
                Monitor.Pulse(syncRoot);
            }            
        }

        private LinkedList<InboundMessageDispatch> GetList(InboundMessageDispatch envelope)
        {
            MsgPriority priority = envelope.Message.NMSPriority;
            return lists[(int) priority];
        }

        public InboundMessageDispatch Dequeue(int timeout)
        {
            lock (syncRoot)
            {
                while (timeout != 0 && IsEmpty && !disposed)
                {
                    if (timeout == -1)
                    {
                        Monitor.Wait(syncRoot);
                    }
                    else
                    {
                        long start = DateTime.UtcNow.Ticks / 10_000L;
                        Monitor.Wait(syncRoot, timeout);
                        timeout = Math.Max(timeout + (int) (start - DateTime.UtcNow.Ticks / 10_000L), 0);
                    }
                }

                if (IsEmpty || disposed)
                {
                    return null;
                }

                return RemoveFirst();
            }
        }
        
        public void Clear()
        {
            lock (syncRoot)
            {                
                for (int i = (int) MsgPriority.Highest; i >= 0; i--)
                {
                    lists[i].Clear();
                }
                count = 0;
            }
        }

        public void Dispose()
        {
            lock (syncRoot)
            {
                disposed = true;
                Monitor.PulseAll(syncRoot);
            }
        }
    }
}